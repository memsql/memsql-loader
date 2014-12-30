class APSWSQLStepQueueException(Exception):
    pass

class TaskDoesNotExist(APSWSQLStepQueueException):
    pass

class StepAlreadyStarted(APSWSQLStepQueueException):
    pass

class StepNotStarted(APSWSQLStepQueueException):
    pass

class StepAlreadyFinished(APSWSQLStepQueueException):
    pass

class AlreadyFinished(APSWSQLStepQueueException):
    pass

class StepRunning(APSWSQLStepQueueException):
    pass
