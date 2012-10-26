#!/usr/bin/python
#This is a comment
import sys, urllib, time, urlparse, mechanize, json, bottle, os, logging, subprocess
import uuid, OpenSSL
from getopt import getopt
from bottle import route, run, request, abort
from pymongo import Connection
from threading import Thread

env = {'MONGODB_HOST': localhost, 'MONGODB_PORT': 24077, \
    'dbuser': root, 'dbpass': '123qwe', \
    'python_path': '/usr/bin/env python', \
    'post_process_path': 'crawler2.py'}


# CORE OBJECTS
connection = Connection(env['MONGODB_HOST'], int(env['MONGODB_PORT']))
db_handle = connection.admin
db_handle.authenticate(env['dbuser'], env['dbpass'])
db_handle = db_handle.urls

# LOGGING CONFIG
logging.basicConfig(level=logging.DEBUG)

class PostThread(Thread):
  def __init__(self, job_id, urls):
    self.job_id = job_id
    self.urls = urls
    Thread.__init__(self)
  def run(self):
    sp_call = python_path + ' ' + post_process_path
    if self.job_id:
      sp_call = sp_call + ' ' + str(self.job_id)
    else:
      logging.error('Error: No Job ID')
      sys.exit()
    if self.urls:
      for url in self.urls:
        sp_call = sp_call + ' ' + str(url)
    else:
      logging.error('Error: no URLs')
      sys.exit()
    print "Post thread starting with options %s" % sp_call
    subprocess.call(sp_call, shell=True)

@route('/result/<job_id>', method='GET')
def getJobResults(job_id):
  logging.info('New GET results request received for job %s' % job_id)
  response = {}
  response['img_urls'] = []
  urls_crawled = db_handle.find({'job_id':job_id, 'keep_track_queue': {'$exists': 0}, 'keep_track_crawl': {'$exists':0}})
  #try:
  for url in urls_crawled:
    for image_url in url['img_links']:
      response['img_urls'].append(image_url)
  #except:
  #response['img_urls'] = []
  return json.dumps(response)

# REST API - GET STATUS OF JOB
# returns a json document containing the number of urls
# crawled and waiting to be crawled for a certain job_id.
# Note that the number of urls waiting to be crawled will
# change quickly up and down as new child URLs are added 
# and then processed

@route('/status/<job_id>', method='GET')
def getJobStatus(job_id):
  logging.info('New GET status request received for job %s' % job_id)
  response = {}
  crawl_count = db_handle.find_one({'job_id':job_id, 'keep_track_crawl': 1, 'crawl_count': {'$exists': 1}})
  response['crawl_count'] = crawl_count['crawl_count']
  queue_count = db_handle.find_one({'job_id':job_id, 'keep_track_queue': 1, 'queue_count': {'$exists': 1}})
  response['queue_count'] = queue_count['queue_count']
  return json.dumps(response) 

# REST API - POST NEW JOB
# requires a json document to be posted containing a list of
# URL's to be crawled. Assigns a unique job ID and initiates
# a new job thread, then returns the job ID

@route('/', method='POST') 
def postNewJob():
  received_data = request.body.readline()
  logging.debug('New POST request received.')
  logging.debug('Running crawler2')
# ERROR CHECKING ON RECEIVED REQUEST
  if not received_data:
    abort(400, 'No data received')
  try:
    processed_data = json.loads(received_data)
    urls = processed_data['urls']
  except:
    abort(400, "Request body requires a 'urls' key and value list of urls")
  try:
    for url in processed_data['urls']:
      print url
      u = urlparse.urlparse(url)
      if u.scheme != 'http':
        abort(400, "Only HTTP protocol supported.")
      if not u.netloc:
        abort(400, "Bad URL value list netloc")
  except:
    abort(400, "Bad URL value list: can't parse")

  urls = processed_data['urls']
  # Assign new unique Job ID
  job_id = str(uuid.UUID(bytes = OpenSSL.rand.bytes(16)))[:7]
  while job_id in db_handle.find({}, {'job_id':1, '_id':0}):
    job_id = str(uuid.UUID(bytes = OpenSSL.rand.bytes(16)))[:7]
  logging.info('New Job ID Created: %s' % job_id)
  pt = PostThread(job_id, urls)
  pt.start()
  # Close HTTP session, return Job ID
  time.sleep(1)
  return 'Your job ID is %s' %job_id

if __name__ == "__main__":
  logging.info('Attempting to start webserver listening on 8080...')
  run(host='33.33.33.10', port=8080)
