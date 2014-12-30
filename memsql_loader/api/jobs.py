import dateutil.parser

from memsql_loader.api.base import Api
from memsql_loader.api import shared
from memsql_loader.api.validation import V, validate_enum, listor

class Jobs(Api):
    SORTABLE_COLUMNS = [ 'id', 'created', 'last_contact', 'state', 'tasks_queued', 'tasks_running', 'tasks_cancelled', 'tasks_errored', 'tasks_finished', 'tasks_total' ]

    name = 'Jobs'

    validate = V.Schema({
        'state': listor(validate_enum(shared.JobState)),
        V.Required('order', default=shared.SortDirection.DESC): validate_enum(shared.SortDirection),
        V.Required('order_by', default='created'): V.Any(*SORTABLE_COLUMNS),
        'page_size': V.Range(1, 10000),
        V.Required('page', default=1): V.Range(min=1)
    })

    def _execute(self, params):
        generated_sql, query_params = self._generate_sql(params)

        rows = self._db_query("""
            SELECT
                id,
                created,
                last_contact,
                spec,
                IFNULL(tasks_total, 0)                                          AS tasks_total,
                IFNULL(tasks_cancelled, 0)                                      AS tasks_cancelled,
                IFNULL(tasks_errored, 0)                                        AS tasks_errored,
                IFNULL(tasks_queued, 0)                                         AS tasks_queued,
                -- Tasks that are cancelled count as finished also
                -- It is always true that if one is null, all are null
                IFNULL(tasks_total - tasks_queued - tasks_finished, 0)          AS tasks_running,
                IFNULL(tasks_finished, 0)                                       AS tasks_finished,
                %(state_projection)s                                            AS state,
                bytes_total,
                bytes_downloaded,
                download_rate,
                first_task_start
            FROM
                jobs
                LEFT JOIN(
                    SELECT
                        tasks.job_id,

                        MIN(tasks.started)                                      AS first_task_start,
                        MAX(tasks.last_contact)                                 AS last_contact,

                        -- counts
                        -- These casts are necessary because MemSQL makes arbitrary choices
                        COUNT(tasks.id)                                         AS tasks_total,
                        CAST(SUM(%(cancelled_cond)s) AS SIGNED)                 AS tasks_cancelled,
                        CAST(SUM(%(error_cond)s) AS SIGNED)                     AS tasks_errored,
                        CAST(SUM(%(finished_cond)s) AS SIGNED)                  AS tasks_finished,
                        CAST(SUM(%(queued_cond)s) AS SIGNED)                    AS tasks_queued,

                        -- download information
                        -- CAST because JSON number types are always floats
                        CAST(SUM(tasks.bytes_total) AS SIGNED)        AS bytes_total,
                        CAST(SUM(tasks.bytes_downloaded) AS SIGNED)   AS bytes_downloaded,
                        CAST(SUM(tasks.download_rate) AS SIGNED)      AS download_rate
                    FROM tasks
                    GROUP BY tasks.job_id
                ) AS job_tasks ON job_tasks.job_id = jobs.id
            %(where_expr)s
            ORDER BY %(order_by)s %(order)s
            %(paging)s
        """ % generated_sql, **query_params)

        # calculate time_left for each job
        for row in rows:
            time_left = -1
            no_nulls = None not in (row.last_contact, row.first_task_start, row.bytes_downloaded)
            if row.state == shared.JobState.RUNNING and no_nulls and row.bytes_downloaded != 0:
                last_contact = dateutil.parser.parse(row.last_contact)
                first_task_start = dateutil.parser.parse(row.first_task_start)
                time_since_start = last_contact - first_task_start
                overall_download_rate = row.bytes_downloaded / max(time_since_start.total_seconds(), 1)
                bytes_remaining = row.bytes_total - row.bytes_downloaded
                if overall_download_rate > 0:
                    time_left = bytes_remaining / overall_download_rate

            row['time_left'] = time_left

        return [ shared.job_load_row(row) for row in rows ]

    def _generate_sql(self, params):
        query_params = shared.TaskState.projection_params()
        return { k: v or '' for k, v in {
            'cancelled_cond': shared.TaskState.CANCELLED_CONDITION,
            'error_cond': shared.TaskState.ERROR_CONDITION,
            'finished_cond': shared.TaskState.FINISHED_CONDITION,
            'queued_cond': shared.TaskState.QUEUED_CONDITION,
            'state_projection': shared.JobState.PROJECTION,
            'order': params['order'],
            'order_by': params['order_by'],
            'where_expr': self._state_predicate(params, query_params),
            'paging': self._paging(params, query_params)
        }.iteritems() }, query_params

    def _state_predicate(self, params, query_params):
        if 'state' in params:
            state_list_string = ','.join("'%s'" % v for v in params['state'])
            return "WHERE (%s) IN (%s)" % (shared.JobState.PROJECTION, state_list_string)

    def _paging(self, params, query_params):
        if 'page_size' in params:
            offset = (params['page'] - 1) * params['page_size']
            return 'LIMIT %s, %s' % (offset, params['page_size'])
