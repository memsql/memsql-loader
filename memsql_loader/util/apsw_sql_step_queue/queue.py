import sys
import time
import random
import uuid
from datetime import datetime

from memsql_loader.util import apsw_helpers, apsw_sql_utility, super_json as json
from memsql_loader.util.apsw_sql_step_queue.task_handler import TaskHandler
from memsql_loader.util.apsw_sql_step_queue.time_helpers import unix_timestamp

def primary_table_definition(table_name):
    return apsw_sql_utility.TableDefinition(table_name, """\
CREATE TABLE IF NOT EXISTS %(table_name)s (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created DATETIME NOT NULL,

    data TEXT,
    result TEXT,

    execution_id CHAR(32) DEFAULT NULL,
    steps TEXT,

    -- These fields are solely for the memsql-loader use case; they are not
    -- necessary for the queue's functionality.
    job_id BINARY(32) DEFAULT NULL,
    file_id TEXT DEFAULT NULL,
    bytes_total INTEGER DEFAULT NULL,
    bytes_downloaded INTEGER DEFAULT NULL,
    download_rate INTEGER DEFAULT NULL,
    md5 TEXT DEFAULT NULL,

    started DATETIME,
    last_contact DATETIME,
    update_count INT UNSIGNED DEFAULT 0 NOT NULL,
    finished DATETIME
    )""" % { 'table_name': table_name }, index_columns=('created', 'started', 'last_contact', 'job_id', 'file_id'))

class APSWSQLStepQueue(apsw_sql_utility.APSWSQLUtility):
    def __init__(self, table_name, storage, execution_ttl=60, task_handler_class=TaskHandler):
        """
        table_name      the table name for the queue in the DB
        execution_ttl   the amount of time (in seconds) that can pass before a task is automatically requeued
        """
        super(APSWSQLStepQueue, self).__init__(storage)

        self.table_name = table_name
        self.execution_ttl = execution_ttl
        self.TaskHandlerClass = task_handler_class
        self._define_table(primary_table_definition(self.table_name))

    ###############################
    # Public Interface

    def qsize(self, extra_predicate=None):
        """ Return an approximate number of queued tasks in the queue. """
        with self.storage.transaction() as cursor:
            count = self._query_queued(cursor, 'COUNT(*) AS count', extra_predicate=extra_predicate)
        return count[0].count

    def enqueue(self, data, job_id=None, file_id=None, md5=None,
                bytes_total=None):
        """ Enqueue task with specified data. """
        jsonified_data = json.dumps(data)
        with self.storage.transaction() as cursor:
            apsw_helpers.query(cursor, '''
                INSERT INTO %s
                    (created,
                     data,
                     job_id,
                     file_id,
                     md5,
                     bytes_total)
                VALUES
                    (datetime(:now, "unixepoch"),
                     :data,
                     :job_id,
                     :file_id,
                     :md5,
                     :bytes_total)
            ''' % self.table_name,
                now=unix_timestamp(datetime.utcnow()),
                data=jsonified_data,
                job_id=job_id,
                file_id=file_id,
                md5=md5,
                bytes_total=bytes_total)
            # Return the number of rows we inserted.
            return 1

    def start(self, block=False, timeout=None, retry_interval=0.5, extra_predicate=None):
        """
        Retrieve a task handler from the queue.

        If block is True, this function will block until it is able to retrieve a task.
        If block is True and timeout is a number it will block for at most <timeout> seconds.
        retry_interval is the maximum time in seconds between successive retries.

        extra_predicate
        If extra_predicate is defined, it should be a tuple of (raw_predicate, predicate_args_dict)
        raw_predicate will be prefixed by AND, and inserted into the WHERE condition in the queries.
        predicate_args_dict can be used to substitute values into raw_predicate;
        for instance, if extra_predicate is
        ("WHERE col1 = :val", {"val": "foo"}), we will generate
        "AND (WHERE col1 = "foo")'.
        """
        start = time.time()
        while 1:
            task_handler = self._dequeue_task(extra_predicate)
            if task_handler is None and block:
                if timeout is not None and (time.time() - start) > timeout:
                    break
                time.sleep(retry_interval * (random.random() + 0.1))
            else:
                break
        return task_handler

    def bulk_finish(self, result='cancelled', extra_predicate=None):
        extra_predicate_sql, extra_predicate_args = (
            self._build_extra_predicate(extra_predicate))

        with self.storage.transaction() as cursor:
            now = unix_timestamp(datetime.utcnow())
            affected_rows = apsw_helpers.query(cursor, '''
                SELECT * from %s
                WHERE
                    finished IS NULL
                    AND (
                        execution_id IS NULL
                        OR last_contact <= datetime(:now, 'unixepoch', '-%s second')
                    )
                    %s
            ''' % (self.table_name, self.execution_ttl, extra_predicate_sql),
                now=now,
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
                    AND (
                        execution_id IS NULL
                        OR last_contact <= datetime(:now, 'unixepoch', '-%s second')
                    )
                    %s
            ''' % (self.table_name, self.execution_ttl, extra_predicate_sql),
                now=now,
                result=result,
                **extra_predicate_args)

        return len(affected_rows)

    ###############################
    # Private Interface

    def _query_queued(self, cursor, projection, limit=None, extra_predicate=None):
        extra_predicate_sql, extra_predicate_args = (
            self._build_extra_predicate(extra_predicate))

        result = apsw_helpers.query(cursor, '''
            SELECT
                %s
            FROM %s
            WHERE
                finished IS NULL
                AND (
                    execution_id IS NULL
                    OR last_contact <= datetime(:now, 'unixepoch', '-%s second')
                )
                %s
            ORDER BY created ASC
            LIMIT :limit
        ''' % (projection, self.table_name, self.execution_ttl, extra_predicate_sql),
            now=unix_timestamp(datetime.utcnow()),
            limit=sys.maxsize if limit is None else limit,
            **extra_predicate_args)
        return result

    def _dequeue_task(self, extra_predicate=None):
        execution_id = uuid.uuid1().hex

        extra_predicate_sql, extra_predicate_args = (
            self._build_extra_predicate(extra_predicate))

        task_id = None
        with self.storage.transaction() as cursor:
            while task_id is None:
                possible_tasks = self._query_queued(cursor, 'id, created, data', limit=5, extra_predicate=extra_predicate)

                if not possible_tasks:
                    # nothing to dequeue
                    return None

                for possible_task in possible_tasks:
                    # attempt to claim the task
                    now = unix_timestamp(datetime.utcnow())
                    apsw_helpers.query(cursor, '''
                        UPDATE %s
                        SET
                            execution_id = :execution_id,
                            last_contact = datetime(:now, 'unixepoch'),
                            update_count = update_count + 1,
                            started = datetime(:now, 'unixepoch'),
                            steps = '[]'
                        WHERE
                            id = :task_id
                            AND finished IS NULL
                            AND (
                                execution_id IS NULL
                                OR last_contact <= datetime(:now, 'unixepoch', '-%s second')
                            )
                            %s
                    ''' % (self.table_name, self.execution_ttl, extra_predicate_sql),
                        now=now,
                        execution_id=execution_id,
                        task_id=possible_task.id,
                        **extra_predicate_args)
                    task_id = possible_task.id
                    break
        return self.TaskHandlerClass(execution_id=execution_id, task_id=task_id, queue=self)

    def _build_extra_predicate(self, extra_predicate):
        """ This method is a good one to extend if you want to create a queue which always applies an extra predicate. """
        if extra_predicate is None:
            return '', {}
        return 'AND (' + extra_predicate[0] + ')', extra_predicate[1]
