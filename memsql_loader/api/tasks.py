from memsql_loader.api.base import Api
from memsql_loader.api.validation import V, validate_enum, listor
from memsql_loader.api import shared, exceptions
import sys

class Tasks(Api):
    SORTABLE_COLUMNS = ['id', 'key_name', 'created', 'started', 'finished', 'state', 'error_msg']

    validate = V.Schema({
        V.Required('job_id'): basestring,
        'state': listor(validate_enum(shared.TaskState)),
        V.Required('order', default=shared.SortDirection.ASC): validate_enum(shared.SortDirection),
        V.Required('order_by', default='id'): V.Any(*SORTABLE_COLUMNS),
        'page_size': V.Range(1, 100000),
        V.Required('page', default=1): V.All(int, V.Range(min=1, max=sys.maxint))
    })

    def _execute(self, params):
        generated_sql, query_params = self._generate_sql(params)

        job_count = self._db_get('''
            SELECT COUNT(*) AS count
            FROM jobs
            WHERE %(job_id_predicate)s
        ''' % generated_sql, **query_params).count

        if job_count == 0:
            raise exceptions.ApiException('No job found with id `%s`' % params['job_id'])
        elif job_count > 1:
            raise exceptions.ApiException('More than one job matches id `%s`, try using a more specific prefix' % params['job_id'])

        rows = self._db_query('''
            SELECT
                tasks.*,
                %(state_projection)s AS state
            FROM tasks INNER JOIN jobs ON jobs.id = tasks.job_id
            WHERE
                %(job_id_predicate)s
                %(state_predicate)s
            ORDER BY %(order_by)s %(order)s
            %(paging)s
        ''' % generated_sql, **query_params)

        ret = []
        for row in rows:
            row = shared.task_load_row(row)
            row['key_name'] = row.data['key_name']
            row['error_msg'] = row.data.get('error') or ''
            ret.append(row)
        return ret

    def _generate_sql(self, params):
        query_params = shared.TaskState.projection_params()
        return { k: v or '' for k, v in {
            'job_id_predicate': self._job_id_predicate(params, query_params),
            'state_projection': shared.TaskState.PROJECTION,
            'state_predicate': self._state_predicate(params, query_params),
            'order': params['order'],
            'order_by': params['order_by'],
            'paging': self._paging(params, query_params),
        }.iteritems() }, query_params

    def _job_id_predicate(self, params, query_params):
        query_params['job_id_predicate'] = params['job_id'] + '%'
        return 'jobs.id LIKE :job_id_predicate'

    def _state_predicate(self, params, query_params):
        if 'state' in params:
            state_list_string = ','.join("'%s'" % v for v in params['state'])
            return 'AND (%s) IN (%s)' % (shared.TaskState.PROJECTION, state_list_string)

    def _paging(self, params, query_params):
        if 'page_size' in params:
            offset = (params['page'] - 1) * params['page_size']
            return 'LIMIT %s, %s' % (offset, params['page_size'])
