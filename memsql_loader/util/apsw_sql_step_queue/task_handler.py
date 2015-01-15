import copy
from datetime import datetime
from dateutil import parser
from contextlib import contextmanager

from memsql_loader.util import apsw_helpers, super_json as json
from memsql_loader.util.apsw_sql_step_queue.errors import TaskDoesNotExist, StepAlreadyStarted, StepNotStarted, StepAlreadyFinished, StepRunning, AlreadyFinished
from memsql_loader.util.apsw_sql_step_queue.time_helpers import unix_timestamp

def timedelta_total_seconds(td):
    """ Needed for python 2.6 compat """
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10. ** 6) / 10. ** 6

class TaskHandler(object):
    def __init__(self, execution_id, task_id, queue):
        self.execution_id = execution_id
        self.task_id = task_id
        self._queue = queue
        self.storage = queue.storage

        self.started = 0
        self.finished = None
        self.data = None
        self.result = None

        # NOTE: These fields are specific to the memsql-loader use case;
        # they are not necessary for the queue functionality.
        self.job_id = None
        self.file_id = None
        self.md5 = None
        self.bytes_total = None
        self.bytes_downloaded = None
        self.download_rate = None

        self.steps = None

        self._refresh()

    ###############################
    # Public Interface

    def valid(self):
        """ Check to see if we are still active. """
        if self.finished is not None:
            return False

        with self.storage.cursor() as cursor:
            row = apsw_helpers.get(cursor, '''
                SELECT (last_contact > datetime(:now, 'unixepoch', '-%s second')) AS valid
                FROM %s
                WHERE
                    id = :task_id
                    AND execution_id = :execution_id
            ''' % (self._queue.execution_ttl, self._queue.table_name),
                now=unix_timestamp(datetime.utcnow()),
                task_id=self.task_id,
                execution_id=self.execution_id)

        return bool(row is not None and row.valid)

    def ping(self):
        """ Notify the queue that this task is still active. """
        if self.finished is not None:
            raise AlreadyFinished()

        with self.storage.cursor() as cursor:
            affected_row = apsw_helpers.get(cursor, '''
                SELECT * from %s
                WHERE
                    id = :task_id
                    AND execution_id = :execution_id
                    AND last_contact > datetime(:now, 'unixepoch', '-%s second')
            ''' % (self._queue.table_name, self._queue.execution_ttl),
                now=unix_timestamp(datetime.utcnow()),
                task_id=self.task_id,
                execution_id=self.execution_id)

        if not affected_row:
            raise TaskDoesNotExist()

        with self.storage.transaction() as cursor:
            apsw_helpers.query(cursor, '''
                UPDATE %s
                SET
                    last_contact=datetime(:now, 'unixepoch'),
                    update_count=update_count + 1
                WHERE
                    id = :task_id
                    AND execution_id = :execution_id
                    AND last_contact > datetime(:now, 'unixepoch', '-%s second')
            ''' % (self._queue.table_name, self._queue.execution_ttl),
                now=unix_timestamp(datetime.utcnow()),
                task_id=self.task_id,
                execution_id=self.execution_id)

    def finish(self, result='success'):
        if self._running_steps() != 0:
            raise StepRunning()
        if self.finished is not None:
            raise AlreadyFinished()

        self._save(finished=datetime.utcnow(), result=result)

    def requeue(self):
        if self._running_steps() != 0:
            raise StepRunning()
        if self.finished is not None:
            raise AlreadyFinished()

        with self.storage.cursor() as cursor:
            affected_row = apsw_helpers.get(cursor, '''
                SELECT * from %s
                WHERE
                    id = :task_id
                    AND execution_id = :execution_id
                    AND last_contact > datetime(:now, 'unixepoch', '-%s second')
            ''' % (self._queue.table_name, self._queue.execution_ttl),
                now=unix_timestamp(datetime.utcnow()),
                task_id=self.task_id,
                execution_id=self.execution_id)

        if affected_row is None:
            raise TaskDoesNotExist()

        with self.storage.transaction() as cursor:
            apsw_helpers.query(cursor, '''
                UPDATE %s
                SET
                    last_contact=NULL,
                    update_count=update_count + 1,
                    started=NULL,
                    steps=NULL,
                    execution_id=NULL,
                    finished=NULL,
                    result=NULL
                WHERE
                    id = :task_id
            ''' % self._queue.table_name,
                task_id=self.task_id)

    def start_step(self, step_name):
        """ Start a step. """
        if self.finished is not None:
            raise AlreadyFinished()

        step_data = self._get_step(step_name)
        if step_data is not None:
            if 'stop' in step_data:
                raise StepAlreadyFinished()
            else:
                raise StepAlreadyStarted()

        steps = copy.deepcopy(self.steps)
        steps.append({
            "start": datetime.utcnow(),
            "name": step_name
        })
        self._save(steps=steps)

    def stop_step(self, step_name):
        """ Stop a step. """
        if self.finished is not None:
            raise AlreadyFinished()

        steps = copy.deepcopy(self.steps)

        step_data = self._get_step(step_name, steps=steps)
        if step_data is None:
            raise StepNotStarted()
        elif 'stop' in step_data:
            raise StepAlreadyFinished()

        step_data['stop'] = datetime.utcnow()

        step_data['duration'] = timedelta_total_seconds(step_data['stop'] - step_data['start'])
        self._save(steps=steps)

    @contextmanager
    def step(self, step_name):
        self.start_step(step_name)
        yield
        self.stop_step(step_name)

    def refresh(self):
        self._refresh()

    def save(self):
        self._save()

    ###############################
    # Private Interface

    def _get_step(self, step_name, steps=None):
        for step in (steps if steps is not None else self.steps):
            if step['name'] == step_name:
                return step
        return None

    def _running_steps(self):
        return len([s for s in self.steps if 'stop' not in s])

    def _refresh(self):
        with self.storage.cursor() as cursor:
            row = apsw_helpers.get(cursor, '''
                SELECT * FROM %s
                WHERE
                    id = :task_id
                    AND execution_id = :execution_id
                    AND last_contact > datetime(:now, 'unixepoch', '-%s second')
            ''' % (self._queue.table_name, self._queue.execution_ttl),
                now=unix_timestamp(datetime.utcnow()),
                task_id=self.task_id,
                execution_id=self.execution_id)

        if not row:
            raise TaskDoesNotExist()

        self.task_id = row.id
        self.data = json.loads(row.data)
        self.result = row.result
        self.job_id = row.job_id
        self.file_id = row.file_id
        self.md5 = row.md5
        self.bytes_total = row.bytes_total
        self.bytes_downloaded = row.bytes_downloaded
        self.download_rate = row.download_rate
        self.steps = self._load_steps(json.loads(row.steps))
        self.started = row.started
        self.finished = row.finished

    def _load_steps(self, raw_steps):
        """ load steps -> basically load all the datetime isoformats into datetimes """
        for step in raw_steps:
            if 'start' in step:
                step['start'] = parser.parse(step['start'])
            if 'stop' in step:
                step['stop'] = parser.parse(step['stop'])
        return raw_steps

    def _save(self, finished=None, steps=None, result=None, data=None):
        finished = finished if finished is not None else self.finished
        with self.storage.transaction() as cursor:
            apsw_helpers.query(cursor, '''
                UPDATE %s
                SET
                    last_contact=datetime(:now, 'unixepoch'),
                    update_count=update_count + 1,
                    steps=:steps,
                    finished=datetime(:finished, 'unixepoch'),
                    result=:result,
                    bytes_downloaded=:bytes_downloaded,
                    download_rate=:download_rate,
                    data=:data
                WHERE
                    id = :task_id
                    AND execution_id = :execution_id
                    AND last_contact > datetime(:now, 'unixepoch', '-%s second')
            ''' % (self._queue.table_name, self._queue.execution_ttl),
                now=unix_timestamp(datetime.utcnow()),
                task_id=self.task_id,
                execution_id=self.execution_id,
                steps=json.dumps(steps if steps is not None else self.steps),
                finished=unix_timestamp(finished) if finished else None,
                result=result if result is not None else self.result,
                bytes_downloaded=self.bytes_downloaded,
                download_rate=self.download_rate,
                data=json.dumps(data if data is not None else self.data))

            affected_row = apsw_helpers.get(cursor, '''
                SELECT * from %s
                WHERE
                    id = :task_id
                    AND execution_id = :execution_id
                    AND last_contact > datetime(:now, 'unixepoch', '-%s second')
            ''' % (self._queue.table_name, self._queue.execution_ttl),
                now=unix_timestamp(datetime.utcnow()),
                task_id=self.task_id,
                execution_id=self.execution_id)

        if not affected_row:
            raise TaskDoesNotExist()
        else:
            if steps is not None:
                self.steps = steps
            if finished is not None:
                self.finished = finished
            if result is not None:
                self.result = result
            if data is not None:
                self.data = data
