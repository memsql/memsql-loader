import errno
from memsql.common import database

class ConnectionWrapperException(IOError):
    """ This exception consolidates all connection exceptions into one thing """

    def __init__(self, errno, message, host, port=None, database=None,
                 user=None, password=None):
        IOError.__init__(self, errno, message)
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

class ConnectionWrapper(object):
    """This class wraps a database connection and provides better errors.

    Note: This is essentially copied from the code in connection_pool.py in
    the memsql-python library.
    """

    def __init__(self, host, port=3306, database="information_schema",
                 user=None, password=None, max_idle_time=7 * 3600,
                 options=None):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.max_idle_time = max_idle_time
        self.options = options
        self._conn = None

    def connection_info(self):
        return (self.host, self.port)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __wrap_errors(self, fn, *args, **kwargs):
        def wrapped(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except IOError as e:
                if e.errno in [errno.ECONNRESET, errno.ECONNREFUSED, errno.ETIMEDOUT]:
                    # socket connection issues
                    self.__handle_connection_failure(e)
                else:
                    raise
            except database.OperationalError as e:
                # _mysql specific database connect issues, internal state issues
                if self._conn is not None:
                    self.__potential_connection_failure(e)
                else:
                    self.__handle_connection_failure(e)
        return wrapped

    def __potential_connection_failure(self, e):
        """ OperationalError's are emitted by the _mysql library for
        almost every error code emitted by MySQL.  Because of this we
        verify that the error is actually a connection error before
        terminating the connection and firing off a ConnectionWrapperException
        """
        try:
            self._conn.query('SELECT 1')
        except (IOError, database.OperationalError):
            # ok, it's actually an issue.
            self.__handle_connection_failure(e)
        else:
            # seems ok, probably programmer error
            raise database.DatabaseError(*e.args)

    def __handle_connection_failure(self, e):
        # build and raise the new consolidated exception
        message = None
        if isinstance(e, database.OperationalError) or (hasattr(e, 'args') and len(e.args) >= 2):
            err_num = e.args[0]
            message = e.args[1]
        elif hasattr(e, 'errno'):
            err_num = e.errno
        else:
            err_num = errno.ECONNABORTED

        raise ConnectionWrapperException(
            err_num, message, host=self.host, port=self.port, user=self.user,
            password=self.password, database=self.database)

    ##################
    # Wrap DB Api to deal with connection issues and so on in an intelligent way

    def connect(self):
        _connect = self.__wrap_errors(database.connect)
        self._conn = _connect(
            host=self.host, port=self.port, user=self.user,
            password=self.password, database=self.database,
            max_idle_time=self.max_idle_time, options=self.options)

    # catchall
    def __getattr__(self, key):
        method = getattr(self._conn, key, None)
        if method is None:
            raise AttributeError('Attribute `%s` does not exist' % key)
        else:
            return self.__wrap_errors(method)
