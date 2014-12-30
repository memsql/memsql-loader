from memsql_loader.util import apsw_helpers, log
from memsql_loader.loader_db.storage import LoaderStorage

class Api(object):
    name = None

    def __init__(self):
        self.logger = log.get_logger(self.name or 'api')
        self.storage = LoaderStorage()

    def query(self, params):
        assert 'validate' in dir(self), '`validate` must be defined'
        return self._execute(self.validate(params))

    def _execute(self, params):
        raise NotImplemented()

    def __db_caller(self, callback):
        with self.storage.transaction() as cursor:
            return callback(cursor)

    def _db_query(self, *args, **kwargs):
        return self.__db_caller(lambda c: apsw_helpers.query(c, *args, **kwargs))

    def _db_custom_query(self, callback):
        return self.__db_caller(callback)

    def _db_get(self, *args, **kwargs):
        return self.__db_caller(lambda c: apsw_helpers.get(c, *args, **kwargs))
