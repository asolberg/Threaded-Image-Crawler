UMASK = 0
WORKDIR = "/"
MAXFD = 1024
if (hasattr(os, "devnull")):
   REDIRECT_TO = os.devnull
else:
   REDIRECT_TO = "/dev/null"
def createDaemon():

   try:
      pid = os.fork()
   except OSError, e:
      raise Exception, "%s [%d]" % (e.strerror, e.errno)

   if (pid == 0):
      os.setsid()
      try:
         pid = os.fork()	
      except OSError, e:
         raise Exception, "%s [%d]" % (e.strerror, e.errno)
      if (pid == 0):	
         os.chdir(WORKDIR)
         os.umask(UMASK)
      else:
         os._exit(0)	
   else:
      os._exit(0)	
   import resource		
   maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
   if (maxfd == resource.RLIM_INFINITY):
      maxfd = MAXFD
   for fd in range(0, maxfd):
      try:
         os.close(fd)
      except OSError:	
         pass
   os.open(REDIRECT_TO, os.O_RDWR)	
   os.dup2(0, 1)			
   os.dup2(0, 2)			

   return(0)
