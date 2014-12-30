from memsql_loader.api.base import Api
from memsql_loader.api import shared, exceptions
from memsql_loader.api.validation import V

class Job(Api):
    name = 'Job'

    validate = V.Schema({
        V.Required('job_id'): basestring
    })

    def _execute(self, params):
        generated_sql, query_params = self._generate_sql(params)

        row = self._db_query("""
            SELECT
                id,
                created,
                spec,
                IFNULL(tasks_total, 0)                                      AS tasks_total,
                IFNULL(tasks_queued, 0)                                     AS tasks_queued,
                -- Tasks that are cancelled count as finished also.
                -- It is always true that if one is null, all are null
                IFNULL(tasks_total - tasks_queued - tasks_finished, 0)      AS tasks_running,
                IFNULL(tasks_finished, 0)                                   AS tasks_finished,
                IFNULL(tasks_cancelled, 0)                                  AS tasks_cancelled,
                IFNULL(tasks_succeeded, 0)                                  AS tasks_succeeded,
                IFNULL(tasks_errored, 0)                                    AS tasks_errored,
                %(state_projection)s                                        AS state
            FROM
                jobs
                LEFT JOIN(
                    SELECT
                        tasks.job_id,
                        -- counts
                        COUNT(tasks.id)                                     AS tasks_total,
                        CAST(SUM(%(success_cond)s) AS SIGNED)               AS tasks_succeeded,
                        CAST(SUM(%(error_cond)s) AS SIGNED)                 AS tasks_errored,
                        CAST(SUM(%(cancelled_cond)s) AS SIGNED)             AS tasks_cancelled,
                        CAST(SUM(%(finished_cond)s) AS SIGNED)              AS tasks_finished,
                        CAST(SUM(%(queued_cond)s) AS SIGNED)                AS tasks_queued
                    FROM tasks
                    WHERE %(task_job_id_predicate)s
                    GROUP BY tasks.job_id
                ) AS job_tasks ON job_tasks.job_id = jobs.id
            WHERE %(job_id_predicate)s
            LIMIT 2
        """ % generated_sql, **query_params)

        if len(row) == 0:
            raise exceptions.ApiException('No job found with id `%s`' % params['job_id'])
        elif len(row) > 1:
            raise exceptions.ApiException('More than one job matches id `%s`, try using a more specific prefix' % params['job_id'])

        row = row[0]

        return shared.job_load_row(row)

    def _generate_sql(self, params):
        query_params = shared.TaskState.projection_params()
        return {
            'success_cond': shared.TaskState.SUCCESS_CONDITION,
            'error_cond': shared.TaskState.ERROR_CONDITION,
            'cancelled_cond': shared.TaskState.CANCELLED_CONDITION,
            'finished_cond': shared.TaskState.FINISHED_CONDITION,
            'queued_cond': shared.TaskState.QUEUED_CONDITION,
            'state_projection': shared.JobState.PROJECTION,
            'task_job_id_predicate': self._task_job_id_predicate(params, query_params),
            'job_id_predicate': self._job_id_predicate(params, query_params)
        }, query_params

    def _task_job_id_predicate(self, params, query_params):
        query_params['task_job_id_predicate'] = params['job_id'] + '%'
        return 'tasks.job_id LIKE :task_job_id_predicate'

    def _job_id_predicate(self, params, query_params):
        query_params['job_id_predicate'] = params['job_id'] + '%'
        return 'jobs.id LIKE :job_id_predicate'
