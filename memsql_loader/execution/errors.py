import time

class WorkerException(Exception):
    def __init__(self, *args, **kwargs):
        super(Exception, self).__init__(*args, **kwargs)
        self.time = time.time()

class ConnectionException(WorkerException):
    pass

class RequeueTask(Exception):
    pass
