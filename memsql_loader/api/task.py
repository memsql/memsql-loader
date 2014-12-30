from memsql_loader.api.base import Api
from memsql_loader.api import exceptions
from memsql_loader.api.validation import V
from memsql_loader.api import shared

class Task(Api):
    validate = V.Schema({
        V.Required('task_id'): int
    })

    def _execute(self, params):
        generated_sql, query_params = self._generate_sql(params)

        task_row = self._db_get('''
            SELECT *, %(state_projection)s AS state
            FROM tasks
            WHERE
                %(task_id_predicate)s
            LIMIT 1
        ''' % generated_sql, **query_params)

        if not task_row:
            raise exceptions.ApiException('No task found with id `%s`' % params['task_id'])

        return shared.task_load_row(task_row)

    def _generate_sql(self, params):
        query_params = shared.TaskState.projection_params()
        return { k: v or '' for k, v in {
            'task_id_predicate': self._task_id_predicate(params, query_params),
            'state_projection': shared.TaskState.PROJECTION,
        }.iteritems() }, query_params

    def _task_id_predicate(self, params, query_params):
        query_params['task_id_predicate'] = params['task_id']
        return 'tasks.id = :task_id_predicate'
