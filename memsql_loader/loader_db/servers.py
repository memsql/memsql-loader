import os
from memsql_loader.loader_db.storage import LoaderStorage
from memsql_loader.util import apsw_sql_utility, apsw_helpers

# mark servers as dead after a minute
SERVER_TTL = 60

PRIMARY_TABLE = apsw_sql_utility.TableDefinition('servers', """\
CREATE TABLE IF NOT EXISTS servers (
    pid INT PRIMARY KEY,
    last_contact DATETIME
)
""")

class Servers(apsw_sql_utility.APSWSQLUtility):
    def __init__(self):
        super(Servers, self).__init__(LoaderStorage())

        self._define_table(PRIMARY_TABLE)

    def _garbage_collect(self):
        with self.storage.transaction() as cursor:
            cursor.execute('''
                DELETE FROM servers
                WHERE last_contact < DATETIME('now', '-%s second')
            ''' % (SERVER_TTL,))

    def ping(self):
        """ Ping the servers row associated with the current PID """
        self._garbage_collect()
        with self.storage.transaction() as cursor:
            cursor.execute('''
                INSERT OR IGNORE INTO servers (pid) VALUES (?)
            ''', (os.getpid(),))
            cursor.execute('''
                UPDATE servers
                SET
                    last_contact = DATETIME('now')
                WHERE pid = ?
            ''', (os.getpid(),))

    def server_stop(self):
        """ Delete the servers row associated with the current PID """
        self._garbage_collect()
        with self.storage.transaction() as cursor:
            cursor.execute('DELETE FROM servers WHERE pid = ?', (os.getpid(),))

    def online_servers(self):
        with self.storage.cursor() as cursor:
            servers = apsw_helpers.query(cursor, '''
                SELECT pid from servers
                WHERE last_contact >= DATETIME('now', '-%s second')
                ORDER BY last_contact DESC
                LIMIT 1
            ''' % (SERVER_TTL,))

        return servers
