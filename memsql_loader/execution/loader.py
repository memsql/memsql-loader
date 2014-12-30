import _mysql
import sys
from threading import Thread, Lock

from memsql_loader.db import connection_wrapper, pool
from memsql_loader.db.load_data import LoadDataStmt
from memsql_loader.util import db_utils, log
from memsql_loader.execution.errors import WorkerException, ConnectionException

class Loader(Thread):
    def load(self, job, task, fifo, db_connection):
        self._job = job
        self._task = task
        self._fifo = fifo
        self._conn = db_connection
        load_data = LoadDataStmt(job, task.file_id, fifo.path)
        self._sql, self._params = load_data.build()
        self._error = None
        self._tb = None

        self._active_conn_id = None
        self._conn_lock = Lock()
        self._fifo.attach_reader(self.abort)

    @property
    def error(self):
        return self._error

    @property
    def traceback(self):
        return self._tb

    def abort(self):
        with self._conn_lock:
            if self._active_conn_id is not None:
                try:
                    with pool.get_connection(database='', **self._job.spec.connection) as conn:
                        db_utils.try_kill_connection(conn, self._active_conn_id)
                except pool.PoolConnectionException:
                    # If we couldn't connect, then its likely that we lost
                    # connection to the database and that the query is dead
                    # because of that anyways.
                    pass

                return True
            return False

    def run(self):
        self.logger = log.get_logger('loader')

        # Because self._conn was passed in from the worker thread, we need
        # to call the mysql_thread_init() C function to make sure that
        # everything is initialized properly.  However, _mysql doesn't expose
        # that function, so we call it implicitly by creating a MySQL
        # connection with a socket that's guaranteed to be invalid.
        try:
            _mysql.connect(unix_socket='.')
        except _mysql.MySQLError:
            pass

        try:
            self.logger.info('Starting loader')

            try:
                with self._conn_lock:
                    self._active_conn_id = self._conn.thread_id()
                    with self._task.protect():
                        self._task.data['conn_id'] = self._active_conn_id
                        self._task.save()
                row_count = self._conn.query(self._sql, *self._params)
            finally:
                with self._conn_lock:
                    self._active_conn_id = None

            with self._task.protect():
                self._task.data['row_count'] = row_count
                self._task.save()

        except connection_wrapper.ConnectionWrapperException as e:
            self.logger.error('LOAD DATA connection error: %s', str(e))
            self._set_error(ConnectionException(str(e)))

        except pool.MySQLError as e:
            errno, msg = e.args
            msg = "LOAD DATA error (%d): %s" % (errno, msg)
            self.logger.error(msg)
            self._set_error(WorkerException(msg))

        except Exception as e:
            self._set_error(e)

        except KeyboardInterrupt:
            self.logger.info('Received KeyboardInterrupt, exiting...')

        finally:
            self._fifo.detach_reader()
            self.logger.info('Finished LOAD_DATA')

    def _set_error(self, err):
        self._error = err
        self._tb = sys.exc_info()[2]
