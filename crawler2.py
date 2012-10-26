#!/usr/bin/python
#This is a comment
import sys, urllib, time, urlparse, mechanize, json, bottle, os, logging
from getopt import getopt
from itertools import islice
from threading import Thread
from Queue import Queue
from bs4 import BeautifulSoup
from pymongo import Connection
import daemon

# CORE OBJECTS
connection = Connection(env['MONGODB_HOST'], int(env['MONGODB_PORT']))
db_handle = connection.admin
db_handle.authenticate(env['dbuser'], env['dbpass'])
db_handle = db_handle.urls
url_queue = Queue()
db_handle = db_handle.urls

# LOGGING CONFIG
logging.basicConfig(level=logging.DEBUG)

# DATA RETRIEVAL THROTTLING
max_levels_deep = 2
max_child_urls_per_parent = 10

# DATA PROCESSING THROTTLE
num_fetch_threads = 2

# Uses BeautifulSoup to extract image URLs
def saveImageUrls(html, source_url):
  logging.debug('saveImageURLS: received data, calling BS')
  try:
    soup = BeautifulSoup(html)
  except error as soup_error:
    logging.debug('soup failed with error %s' %soup_error)
  img_links = []
  logging.debug('saveImageURLS: Soup called, extracting tags')
  for tag in soup.findAll('img', src=True):
    image_url = urlparse.urljoin(source_url, tag['src'])
    logging.debug('New image tag found: %s' % image_url)
    img_links.append(image_url)
  return img_links

class ProcessURL(Thread):
  
  def __init__ ( self, i, queue_handle):
    self.i = i
    self.queue_handle = queue_handle
    Thread.__init__ ( self )
  
  def run(self):
    while True:
      url = []
      url = self.queue_handle.get()
      if url:
        db_handle.update({'job_id': url[3], 'keep_track_queue':1}, {'$inc': {'queue_count': -1}}, True)
      br = mechanize.Browser()
      #Try to open URL and extract child URLs. If we are not beyond 
      #the depth threshold add child URL's to the queue
      try:
        logging.debug('Opening URL: %s at level %d with parent: %s '% (url[0], url[1], url[2]))
       # time.sleep(2)
        logging.debug('Opening now')
        r = br.open(url[0], timeout = 10)
        logging.debug('URL opened')
        if url[1] < max_levels_deep:
            for link in islice(br.links(), 0, max_child_urls_per_parent):
              # Append the hostname if a relative link
              if link.url[0] == "/":
                link.url = "http://" + urlparse.urlparse(url[0]).hostname + link.url
              logging.debug('New child URL found. Adding to queue: %s' % link.url)
              self.queue_handle.put((link.url, url[1] + 1, url[0], url[3]))
              db_handle.update({'job_id': url[3], 'keep_track_queue':1}, {'$inc': {'queue_count': 1}}, True)

      except :
        logging.error("Couldn't open URL: %s" % url[0])
        # Couldn't open URL, skip
        pass
      try:
        logging.debug('Loading HTML for image tag scanning')
        html = r.read()
        logging.debug('HTML loaded, calling image scanning routing')
        img_links = saveImageUrls(html, url[0])
        logging.debug('Saving document to DB') 
        data = {'url':url[0], 'levels_deep': url[1], 'parent_url': url[2], 'job_id': url[3], 'img_links': img_links}
        db_handle.insert(data)
        db_handle.update({'job_id': url[3], 'keep_track_crawl':1}, {'$inc': {'crawl_count': 1}}, True)
      except:
        logging.error("Couldn't read HTML")
      self.queue_handle.task_done()
      time.sleep(1)


# Main launches worker threads and
# seeds the queue with initial URLS

if __name__ == "__main__":
  daemon.createDaemon()
  args = sys.argv
  job_id = args[1]
  urls = []
  for url in args[2:]:
    urls.append(url)
  logging.debug('Starting Main Crawl Thread with args %s %s' % (str(job_id), str(urls)))
  for i in range(num_fetch_threads):
    logging.debug('Creating worker thread %d' % i)
    try:
      worker = ProcessURL(i, url_queue)
      worker.setDaemon(True)
      worker.start()
    except:
      logging.debug('Error detected')
    logging.debug('Created worker thread %d' % i)
    logging.debug('Attempting to start thread')
  logging.debug('Threads started, attempting to seed queue')
  for url in urls:
    logging.debug('Adding seed URL to Queue: %s' % url)
    url_queue.put((url, 1, None, job_id))
    db_handle.update({'job_id': job_id, 'keep_track_queue':1}, {'$inc': {'queue_count': 1}}, True)
    logging.debug('URL added')
  time.sleep(1) 
  logging.debug('Queue seeded, blocking until empty')
  url_queue.join()
