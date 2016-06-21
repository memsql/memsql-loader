import multiprocessing
import threading
import uuid
import random
import time
import os
import signal
import traceback

from memsql_loader.api import shared
from memsql_loader.db import connection_wrapper, pool
from memsql_loader.loader_db.tasks import Tasks
from memsql_loader.loader_db.jobs import Jobs
from memsql_loader.execution.errors import WorkerException, ConnectionException, RequeueTask
from memsql_loader.execution.loader import Loader
from memsql_loader.execution.downloader import Downloader
from memsql_loader.util import db_utils, log
from memsql_loader.util.fifo import FIFO

from memsql_loader.util.apsw_sql_step_queue.errors import APSWSQLStepQueueException, TaskDoesNotExist

HUNG_DOWNLOADER_TIMEOUT = 3600

class ExitingException(Exception):
    pass

class Worker(multiprocessing.Process):
    def __init__(self, worker_sleep, parent_pid, worker_lock):
        self.worker_id = uuid.uuid1().hex[:8]
        self.worker_sleep = worker_sleep
        self.worker_lock = worker_lock
        self.worker_working = multiprocessing.Value('i', 1)
        self.parent_pid = parent_pid
        self._exit_evt = multiprocessing.Event()
        self.logger = log.get_logger('worker[%s]' % self.worker_id)
        super(Worker, self).__init__(name=('worker-%s' % self.worker_id))

    def kill_query_if_exists(self, conn_args, conn_id):
        with pool.get_connection(database='information_schema', **conn_args) as conn:
            id_row = conn.query("SELECT id FROM processlist WHERE info LIKE '%%LOAD DATA%%' AND id=%s", conn_id)
            if len(id_row) > 0:
                # Since this is a LOAD DATA LOCAL query, we need to kill the
                # connection, not the query, since LOAD DATA LOCAL queries
                # don't end until the file is fully read, even if they're
                # killed.
                db_utils.try_kill_connection(conn, conn_id)

    def kill_delete_query_if_exists(self, conn_args, conn_id):
        with pool.get_connection(database='information_schema', **conn_args) as conn:
            id_row = conn.query("SELECT id FROM processlist WHERE info LIKE '%%DELETE%%' AND id=%s", conn_id)
            if len(id_row) > 0:
                db_utils.try_kill_query(conn, conn_id)

    def signal_exit(self):
        self._exit_evt.set()

    def is_working(self):
        return self.worker_working.value == 1

    def run(self):
        self.jobs = Jobs()
        self.tasks = Tasks()
        task = None

        ignore = lambda *args, **kwargs: None
        signal.signal(signal.SIGINT, ignore)
        signal.signal(signal.SIGQUIT, ignore)

        try:
            while not self.exiting():
                time.sleep(random.random() * 0.5)
                task = self.tasks.start()

                if task is None:
                    self.worker_working.value = 0
                else:
                    self.worker_working.value = 1

                    job_id = task.job_id
                    job = self.jobs.get(job_id)

                    old_conn_id = task.data.get('conn_id', None)
                    if old_conn_id is not None:
                        self.kill_query_if_exists(job.spec.connection, old_conn_id)

                    self.logger.info('Task %d: starting' % task.task_id)

                    try:
                        # can't use a pooled connection due to transactions staying open in the
                        # pool on failure
                        with pool.get_connection(database=job.spec.target.database, pooled=False, **job.spec.connection) as db_connection:
                            db_connection.execute("BEGIN")
                            self._process_task(task, db_connection)
                        self.logger.info('Task %d: finished with success', task.task_id)
                    except (RequeueTask, ConnectionException):
                        self.logger.info('Task %d: download failed, requeueing', task.task_id)
                        self.logger.debug("Traceback: %s" % (traceback.format_exc()))
                        task.requeue()
                    except TaskDoesNotExist as e:
                        self.logger.info('Task %d: finished with error, the task was either cancelled or deleted', task.task_id)
                        self.logger.debug("Traceback: %s" % (traceback.format_exc()))
                    except WorkerException as e:
                        task.error(str(e))
                        self.logger.info('Task %d: finished with error', task.task_id)
                    except Exception as e:
                        self.logger.debug("Traceback: %s" % (traceback.format_exc()))
                        raise

            raise ExitingException()

        except ExitingException:
            self.logger.debug('Worker exiting')
            if task is not None and not task.valid():
                try:
                    task.requeue()
                except APSWSQLStepQueueException:
                    pass

    def _process_task(self, task, db_connection):
        job_id = task.job_id
        job = self.jobs.get(job_id)
        if job is None:
            raise WorkerException('Failed to find job with ID %s' % job_id)

        # If this is a gzip file, we add .gz to the named pipe's name so that
        # MemSQL knows to decompress it unless we're piping this into a script,
        # in which case we do the decompression here in-process.
        if job.spec.options.script is not None:
            gzip = False
        else:
            gzip = task.data['key_name'].endswith('.gz')
        fifo = FIFO(gzip=gzip)

        # reduces the chance of synchronization between workers by
        # initially sleeping in the order they were started and then
        # randomly sleeping after that point
        time.sleep(self.worker_sleep)
        self.worker_sleep = 0.5 * random.random()

        if self.exiting() or not task.valid():
            raise ExitingException()

        if job.has_file_id():
            if self._should_delete(job, task):
                self.logger.info('Waiting for DELETE lock before cleaning up rows from an earlier load')
                try:
                    while not self.worker_lock.acquire(block=True, timeout=0.5):
                        if self.exiting() or not task.valid():
                            raise ExitingException()
                        task.ping()
                    self.logger.info('Attempting cleanup of rows from an earlier load')
                    num_deleted = self._delete_existing_rows(db_connection, job, task)
                    self.logger.info('Deleted %s rows during cleanup' % num_deleted)
                finally:
                    try:
                        self.worker_lock.release()
                    except ValueError:
                        # This is raised if we didn't acquire the lock (e.g. if
                        # there was a KeyboardInterrupt before we acquired the
                        # lock above.  In this case, we don't need to
                        # release the lock.
                        pass

        if self.exiting() or not task.valid():
            raise ExitingException()

        downloader = Downloader()
        downloader.load(job, task, fifo)

        loader = Loader()
        loader.load(job, task, fifo, db_connection)

        loader.start()
        downloader.start()

        try:
            while not self.exiting():
                time.sleep(0.5)

                with task.protect():
                    self._update_task(task, downloader)
                    task.save()

                if downloader.is_alive() and time.time() > downloader.metrics.last_change + HUNG_DOWNLOADER_TIMEOUT:
                    # downloader has frozen, and the progress handler froze as well
                    self.logger.error("Detected hung downloader. Trying to exit.")
                    self.signal_exit()

                loader_alive = loader.is_alive()
                downloader_alive = downloader.is_alive()

                if not loader_alive or not downloader_alive:
                    if loader.error or downloader.error:
                        # We want to make sure that in the case of simultaneous
                        # exceptions, we see both before deciding what to do
                        time.sleep(3)
                    # Only exit if at least 1 error or both are not alive
                    elif not loader_alive and not downloader_alive:
                        break
                    else:
                        continue

                    loader_error = loader.error
                    loader_tb = loader.traceback
                    downloader_error = downloader.error
                    downloader_tb = downloader.traceback

                    any_requeue_task = isinstance(loader_error, RequeueTask) or isinstance(downloader_error, RequeueTask)
                    loader_worker_exception = isinstance(loader_error, WorkerException)
                    downloader_worker_exception = isinstance(downloader_error, WorkerException)

                    # If we have any RequeueTasks, then requeue
                    if any_requeue_task:
                        raise RequeueTask()
                    # Raise the earlier exception
                    elif loader_worker_exception and downloader_worker_exception:
                        if loader_error.time < downloader_error.time:
                            raise loader_error, None, loader_tb
                        else:
                            raise downloader_error, None, downloader_tb
                    # If they're both exceptions but one of them isn't a WorkerException
                    elif (downloader_error and loader_error) and (loader_worker_exception or downloader_worker_exception):
                        if not loader_worker_exception:
                            raise loader_error, None, loader_tb
                        else:
                            raise downloader_error, None, downloader_tb
                    # We don't have any WorkerExceptions, raise a random one
                    # Also handles the case where only one exception is raised
                    elif downloader_error or loader_error:
                        raise downloader_error or loader_error, None, downloader_tb or loader_tb
                    else:
                        assert False, 'Program should only reach this conditional block if at least one error exists'
        finally:
            if downloader.is_alive():
                downloader.terminate()

            self.logger.info('Waiting for threads to exit...')
            while downloader.is_alive() or loader.is_alive():
                loader.join(5)
                downloader.join(5)
                if task.valid():
                    task.ping()

            if self.exiting():
                raise ExitingException()

        with task.protect():
            db_connection.execute("COMMIT")
            self._update_task(task, downloader)
            task.finish('success')

    def _should_delete(self, job, task):
        competing_job_ids = ["'%s'" % j.id for j in self.jobs.query_target(job.spec.connection.host, job.spec.connection.port, job.spec.target.database, job.spec.target.table)]
        predicate_sql = "file_id = :file_id and job_id in (%s)" % ','.join(competing_job_ids)
        matching = self.tasks.get_tasks_in_state(
            [ shared.TaskState.SUCCESS ],
            extra_predicate=(predicate_sql, { 'file_id': task.file_id }))
        return len(matching) > 0

    def _delete_existing_rows(self, conn, job, task):
        file_id = task.file_id
        sql = {
            'database_name': job.spec.target.database,
            'table_name': job.spec.target.table,
            'file_id_column': job.spec.options.file_id_column
        }

        thread_ctx = {
            'num_deleted': 0,
            'exception': None
        }

        def _run_delete_query():
            try:
                thread_ctx['num_deleted'] = conn.query('''
                    DELETE FROM `%(database_name)s`.`%(table_name)s`
                    WHERE `%(file_id_column)s` = %%s
                ''' % sql, file_id)
            except connection_wrapper.ConnectionWrapperException as e:
                self.logger.error(
                    'Connection error when cleaning up rows: %s', str(e))
                thread_ctx['exception'] = RequeueTask()
            except pool.MySQLError as e:
                errno, msg = e.args
                msg = 'Error when cleaning up rows (%d): %s' % (errno, msg)
                self.logger.error(msg)
                thread_ctx['exception'] = RequeueTask()
            except Exception as e:
                thread_ctx['exception'] = e

        t = threading.Thread(target=_run_delete_query)
        t.start()

        while not self.exiting() and task.valid():
            try:
                # Ping the task to let the SQL queue know that it's still active.
                task.ping()
            except TaskDoesNotExist:
                # The task might have gotten cancelled between when we checked
                # whether it's valid and when we ping() it. If ping() fails and
                # it has been cancelled in between, then we should proceed with
                # killing the delete query if it exists
                continue

            if not t.is_alive():
                break

            time.sleep(0.5)
        else:
            # delete thread didn't finish on its own
            self.kill_delete_query_if_exists(job.spec.connection, conn.thread_id())
            t.join()

        exc = thread_ctx['exception']
        if exc is not None:
            raise exc

        return thread_ctx['num_deleted']

    def _update_task(self, task, downloader):
        stats = downloader.metrics.get_stats()
        task.bytes_downloaded = stats['bytes_downloaded']
        task.download_rate = stats['download_rate']
        task.data['time_left'] = stats['time_left']

    def exiting(self):
        try:
            os.kill(self.parent_pid, 0)
        except OSError:
            # parent process does not exist, exit immediately
            return True

        return self._exit_evt.is_set()
