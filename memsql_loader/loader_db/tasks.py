import copy
from contextlib import contextmanager
from threading import RLock
from datetime import datetime

import memsql_loader.api as api
from memsql_loader.loader_db.storage import LoaderStorage
from memsql_loader.util import apsw_helpers, apsw_sql_step_queue
from memsql_loader.util import super_json as json
from memsql_loader.util.apsw_sql_step_queue.errors import TaskDoesNotExist, StepRunning, AlreadyFinished
from memsql_loader.util.apsw_sql_step_queue.time_helpers import unix_timestamp


class TaskHandler(apsw_sql_step_queue.TaskHandler):
    def __init__(self, *args, **kwargs):
        super(TaskHandler, self).__init__(*args, **kwargs)
        self._lock = RLock()

    @contextmanager
    def protect(self):
        with self._lock:
            self.refresh()
            yield

    def error(self, message):
        with self.protect():
            self.data['error'] = message
            self.finish(result='error')

    # Monkey-patching this to reset download progress
    def requeue(self):
        if self._running_steps() != 0:
            raise StepRunning()
        if self.finished is not None:
            raise AlreadyFinished()

        data = copy.deepcopy(self.data)
        self.bytes_downloaded = None
        self.download_rate = None
        data.pop('time_left', None)

        with self._queue.storage.transaction() as cursor:
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

            apsw_helpers.query(cursor, '''
                UPDATE %s
                SET
                    last_contact=NULL,
                    update_count=update_count + 1,
                    started=NULL,
                    steps=NULL,
                    execution_id=NULL,
                    finished=NULL,
                    data=:data,
                    result=NULL
                WHERE
                    id = :task_id
                    AND execution_id = :execution_id
                    AND last_contact > datetime(:now, 'unixepoch', '-%s second')
            ''' % (self._queue.table_name, self._queue.execution_ttl),
                data=json.dumps(data),
                now=unix_timestamp(datetime.utcnow()),
                task_id=self.task_id,
                execution_id=self.execution_id)

class Tasks(apsw_sql_step_queue.APSWSQLStepQueue):
    def __init__(self):
        storage = LoaderStorage()
        super(Tasks, self).__init__('tasks', storage, execution_ttl=api.shared.TASKS_TTL, task_handler_class=TaskHandler)

    # NOTE: This method overrides bulk_finish on APSWSQLStepQueue so that it
    # finishes tasks even if they are currently running.
    def bulk_finish(self, result='cancelled', extra_predicate=None):
        extra_predicate_sql, extra_predicate_args = (
            self._build_extra_predicate(extra_predicate))

        with self.storage.transaction() as cursor:
            now = unix_timestamp(datetime.utcnow())
            affected_rows = apsw_helpers.query(cursor, '''
                SELECT * from %s
                WHERE
                    finished IS NULL
                    %s
            ''' % (self.table_name, extra_predicate_sql),
                **extra_predicate_args)
            apsw_helpers.query(cursor, '''
                UPDATE %s
                SET
                    execution_id = 0,
                    last_contact = datetime(:now, 'unixepoch'),
                    update_count = update_count + 1,
                    steps = '[]',
                    started = datetime(:now, 'unixepoch'),
                    finished = datetime(:now, 'unixepoch'),
                    result = :result
                WHERE
                    finished IS NULL
                    %s
            ''' % (self.table_name, extra_predicate_sql),
                now=now,
                result=result,
                **extra_predicate_args)

        return len(affected_rows)

    def get_tasks_in_state(self, state, extra_predicate=None):
        extra_predicate_sql, extra_predicate_args = (
            self._build_extra_predicate(extra_predicate))

        query_params = api.shared.TaskState.projection_params()
        if len(state) == 1:
            state_list = "('" + str(state[0]) + "')"
        else:
            state_list = str(tuple(str(v) for v in state ))
        query_params.update(extra_predicate_args)
        with self.storage.cursor() as cursor:
            rows = apsw_helpers.query(cursor, '''
                SELECT *
                FROM %s
                WHERE
                    %s IN %s
                    %s
                ORDER BY id ASC
            ''' % (self.table_name, api.shared.TaskState.PROJECTION, state_list, extra_predicate_sql),
                **query_params)

        return [ api.shared.task_load_row(row) for row in rows ]
