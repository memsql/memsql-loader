import apsw
import contextlib
import multiprocessing
import threading

class APSWStorageInitFailure(Exception):
    pass

class APSWStorage(object):
    """ SQLite backed database

        Usage ::

            class BarStorage(APSWStorage):
                def setup(self):
                    with self.transaction() as cursor:
                        cursor.execute("create table bar (fuzz int)")

            storage = BarStorage('bar.db')

            # transactions can be nested
            # note that cursor() returns a different connection from the transaction
            with storage.transaction() as cursor:
                cursor.execute("insert into bar values (1)")

                with storage.transaction() as cursor2:
                    cursor2.execute("insert into bar values (2)")

                    assert apsw_helpers.get(storage.cursor(), "select count(*) c from foo").c == 0
                assert apsw_helpers.get(storage.cursor(), "select count(*) c from foo").c == 0
            assert apsw_helpers.get(storage.cursor(), "select count(*) c from foo").c == 2

            # transactions can also be verified for changes
            # note that the connection for the transaction and for checking changes is shared
            with storage.transaction() as cursor:
                cursor.execute("insert into bar values (1) where 0=1")
                assert storage.transaction_changes() == 0, "no changes made by transaction"

    """
    _db = None
    _db_t = None
    _write_lock = None
    _read_lock = None

    def __init__(self, path):
        self._write_lock = multiprocessing.RLock()
        self.path = path
        self.setup_connections()

    def setup_connections(self):
        """ Setup a sqlite3 database at the provided path. """
        # _db_t is for transactions, _db is for all other cursors
        self._db = apsw.Connection(self.path)
        self._db_t = apsw.Connection(self.path)
        self._db.setbusytimeout(60000)
        self._db_t.setbusytimeout(60000)

        self._read_lock = threading.RLock()

        def pragma(cursor, name, value, check_val):
            cursor.execute("pragma %s=%s" % (name, value))
            server_val = cursor.execute("pragma %s" % name).fetchone()[0]
            if not server_val == check_val:
                raise APSWStorageInitFailure("Failed to set %s to %s (%s != %s)" % (name, value, server_val, check_val))

        with self._write_lock:
            for db in [self._db, self._db_t]:
                cursor = db.cursor()
                pragma(cursor, "journal_mode", "WAL", "wal")
                pragma(cursor, "synchronous", "NORMAL", 1)
                pragma(cursor, "foreign_keys", "ON", 1)

    @contextlib.contextmanager
    def transaction(self):
        """ Take the write lock, and return a cursor to the database.

        Transactions can be nested.
        """

        with self._write_lock:
            with self._db_t:
                yield self._db_t.cursor()
            try:
                self._db_t.wal_checkpoint()
            except (apsw.BusyError, apsw.LockedError):
                pass
        with self._read_lock:
            try:
                self._db.wal_checkpoint()
            except (apsw.BusyError, apsw.LockedError):
                pass

    @contextlib.contextmanager
    def cursor(self):
        with self._read_lock:
            yield self._db.cursor()

    def transaction_changes(self):
        return self._db_t.changes()

    def close_connections(self):
        self._db.close(True)
        self._db_t.close(True)
        self._db = None
        self._db_t = None
