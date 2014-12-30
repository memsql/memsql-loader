from memsql.common import errorcodes

from memsql_loader.db import pool

def validate_file_id_column(conn, database, table, col_name):
    # Load id column isn't required
    if col_name is None:
        return True

    col = conn.get("SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s",
        database, table, col_name)
    if col is None:
        return False
    col_type = col.COLUMN_TYPE.lower()
    return 'bigint' in col_type and 'unsigned' in col_type

def validate_database_table(conn, database, table):
    db_row = conn.get("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME=%s", database)
    tb_row = conn.get("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s", database, table)
    return db_row is not None, tb_row is not None

def try_kill_query(conn, query_id):
    try:
        conn.execute("KILL QUERY %d" % query_id)
    except pool.MySQLError as (errno, _):
        if errno != errorcodes.ER_NO_SUCH_THREAD:
            raise

def try_kill_connection(conn, conn_id):
    try:
        conn.execute("KILL CONNECTION %d" % conn_id)
    except pool.MySQLError as (errno, _):
        if errno != errorcodes.ER_NO_SUCH_THREAD:
            raise
