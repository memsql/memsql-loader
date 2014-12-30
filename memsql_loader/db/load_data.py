from memsql.common import database


class LoadDataStmt(object):
    def __init__(self, job, file_id, source_file):
        self.job = job
        self.file_id = file_id
        self.source_file = source_file

    def build(self):
        generated_sql, query_params = self._generate_sql()

        return ("""
            LOAD DATA %(local)s INFILE %(source_file)s %(dup_key_method)s INTO TABLE `%(database_name)s`.`%(table_name)s`
            %(fields_spec)s
            %(lines_spec)s
            %(ignore)s
            %(columns)s
            %(file_id)s
        """ % generated_sql, query_params)

    def _generate_sql(self):
        query_params = []
        sql = {
            "local": self._generate_local(),
            "source_file": self._generate_source_file(query_params),
            "database_name": self.job.spec.target.database,
            "table_name": self.job.spec.target.table,
            "fields_spec": self._generate_fields_spec(query_params),
            "lines_spec": self._generate_lines_spec(query_params),
            "ignore": self._generate_ignore(query_params),
            "columns": self._generate_columns(query_params),
            "file_id": self._generate_file_id(query_params),
            "dup_key_method": self._generate_dup_key_method(),
        }
        return { k: v if v is not None else '' for k, v in sql.iteritems() }, query_params

    def _generate_local(self):
        if self.job.spec.options.non_local_load:
            return ''
        return 'LOCAL'

    def _generate_source_file(self, query_params):
        query_params.append(self.source_file)
        return "%s"

    def _generate_fields_spec(self, query_params):
        sql = ''
        fields = self.job.spec.options.fields

        if 'terminated' in fields:
            query_params.append(fields.terminated)
            sql += ' TERMINATED BY %s'
        if 'enclosed' in fields:
            query_params.append(fields.enclosed)
            sql += ' ENCLOSED BY %s'
        if 'escaped' in fields:
            query_params.append(fields.escaped)
            sql += ' ESCAPED BY %s'

        return ('FIELDS' + sql) if len(sql) else ''

    def _generate_lines_spec(self, query_params):
        sql = ''
        lines = self.job.spec.options.lines

        if 'starting' in lines:
            query_params.append(lines.starting)
            sql += ' STARTING BY %s'
        if 'terminated' in lines:
            query_params.append(lines.terminated)
            sql += ' TERMINATED BY %s'

        return ('LINES' + sql) if len(sql) else ''

    def _generate_ignore(self, query_params):
        if 'ignore' in self.job.spec.options.lines:
            query_params.append(self.job.spec.options.lines.ignore)
            return 'IGNORE %s LINES'
        else:
            return ''

    def _generate_columns(self, query_params):
        if len(self.job.spec.options.columns) > 0:
            columns = self.job.spec.options.columns
            return "(%s)" % ', '.join("`%s`" % column for column in columns)

    def _generate_file_id(self, query_params):
        if self.job.has_file_id():
            query_params.append(self.file_id)
            return 'SET `%s` = %%s' % self.job.spec.options.file_id_column

    def _generate_dup_key_method(self):
        assert 'duplicate_key_method' in self.job.spec.options
        method = self.job.spec.options.duplicate_key_method.upper()
        return '' if method == 'ERROR' else method


def build_example_query(job, file_id):
    load_data = LoadDataStmt(job, file_id, '<source file>')
    return database.escape_query(*load_data.build())
