from memsql_loader.util import apsw_helpers

class TableDefinition(object):
    def __init__(self, table_name, sql, index_columns=None):
        self.table_name = table_name
        self.sql = sql
        self.index_columns = index_columns or []

class APSWSQLUtility(object):
    def __init__(self, storage):
        self.storage = storage
        self._tables = {}

    ###############################
    # Public Interface

    def setup(self):
        """ Initialize the required tables in the database """
        with self.storage.transaction() as cursor:
            for table_defn in self._tables.values():
                cursor.execute(table_defn.sql)
                for index_column in table_defn.index_columns:
                    index_name = table_defn.table_name + '_' + index_column + '_idx'
                    cursor.execute(
                        'CREATE INDEX %s ON %s (%s)' %
                        (index_name, table_defn.table_name, index_column))
        return self

    def ready(self):
        """ Returns True if the tables have been setup, False otherwise """
        with self.storage.cursor() as cursor:
            rows = apsw_helpers.query(
                cursor, 'SELECT name FROM sqlite_master WHERE type = "table"')
        tables = [row.name for row in rows]
        return all([table_name in tables for table_name in self._tables])

    ###############################
    # Protected Interface

    def _define_table(self, table_definition):
        self._tables[table_definition.table_name] = table_definition
