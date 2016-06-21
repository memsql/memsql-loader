import os
import multiprocessing
import time
from memsql_loader.util import log
from memsql_loader.execution.worker import Worker
from memsql_loader.loader_db.storage import LoaderStorage

class WorkerPool(object):
    def __init__(self, num_workers=None, idle_timeout=None):
        self.logger = log.get_logger('WorkerPool')
        self.num_workers = num_workers or max(1, int(multiprocessing.cpu_count() * 0.8))
        self.idle_timeout = idle_timeout
        self._workers = []
        self.pid = os.getpid()
        self._worker_lock = multiprocessing.Lock()
        self._last_work_time = time.time()

    def poll(self):
        running = [worker for worker in self._workers if worker.is_alive()]

        if self.idle_timeout is not None:
            if any([worker.is_working() for worker in self._workers]):
                self._last_work_time = time.time()
            elif time.time() > (self._last_work_time + self.idle_timeout):
                return False

        diff = self.num_workers - len(running)
        if diff > 0:
            self.logger.debug('Starting %d workers, for a total of %d', diff, self.num_workers)
            with LoaderStorage.fork_wrapper():
                running += [self._start_worker(i) for i in xrange(diff)]
        self._workers = running

        return True

    def stop(self):
        [worker.signal_exit() for worker in self._workers if worker.is_alive()]
        [worker.join() for worker in self._workers if worker.is_alive()]

    def _start_worker(self, index):
        worker = Worker(index * 0.1, self.pid, self._worker_lock)
        worker.start()
        return worker
