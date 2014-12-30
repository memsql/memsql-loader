from memsql.common import database, connection_pool

from memsql_loader.db.connection_wrapper import ConnectionWrapper

PoolConnectionException = connection_pool.PoolConnectionException
MySQLError = database.MySQLError
OperationalError = database.OperationalError

_POOL = connection_pool.ConnectionPool()

def recreate_pool():
    global _POOL
    _POOL = connection_pool.ConnectionPool()

def close_connections():
    _POOL.close()

def get_connection(host, port, database, user, password, pooled=True, **kwargs):
    kwargs['host'] = host
    kwargs['port'] = int(port)
    kwargs['database'] = database
    kwargs['user'] = user
    kwargs['password'] = password

    kwargs['options'] = { 'local_infile': 1 }

    if pooled:
        conn = _POOL.connect(**kwargs)
    else:
        conn = ConnectionWrapper(**kwargs)
        conn.connect()

    return conn
