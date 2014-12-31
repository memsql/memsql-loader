import contextlib
import gc
import multiprocessing
import os

from memsql_loader.util.apsw_storage import APSWStorage
from memsql_loader.util import paths

MEMSQL_LOADER_DB = 'memsql_loader.db'

def get_loader_db_path():
    return os.path.join(paths.get_data_dir(), MEMSQL_LOADER_DB)

# IMPORTANT NOTE: This class cannot be shared across forked processes unless
# you use fork_wrapper.
class LoaderStorage(APSWStorage):
    _instance = None
    _initialized = False
    _instance_lock = multiprocessing.RLock()

    # We use LoaderStorage as a singleton.
    def __new__(cls, *args, **kwargs):
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = super(LoaderStorage, cls).__new__(
                    cls, *args, **kwargs)
                cls._initialized = False
            return cls._instance

    @classmethod
    def drop_database(cls):
        with cls._instance_lock:
            if os.path.isfile(get_loader_db_path()):
                os.remove(get_loader_db_path())
            if os.path.isfile(get_loader_db_path() + '-shm'):
                os.remove(get_loader_db_path() + '-shm')
            if os.path.isfile(get_loader_db_path() + '-wal'):
                os.remove(get_loader_db_path() + '-wal')
            cls._instance = None

    @classmethod
    @contextlib.contextmanager
    def fork_wrapper(cls):
        # This context manager should be used around any code that forks new
        # processes that will use a LoaderStorage object (e.g. Worker objects).
        # This ensures that we don't share SQLite connections across forked
        # processes.
        with cls._instance_lock:
            if cls._instance is not None:
                cls._instance.close_connections()
                # We garbage collect here to clean up any SQLite objects we
                # may have missed; this is important because any surviving
                # objects post-fork will mess up SQLite connections in the
                # child process.  We use generation=2 to collect as many
                # objects as possible.
                gc.collect(2)
        yield
        with cls._instance_lock:
            if cls._instance is not None:
                cls._instance.setup_connections()

    def __init__(self):
        with LoaderStorage._instance_lock:
            # Since this is a singleton object, we don't want to call the
            # parent object's __init__ if we've already instantiated this
            # object in __new__.  However, we may have closed this object's
            # connections in fork_wrapper above; in that case, we want to set
            # up new database connections.
            if not LoaderStorage._initialized:
                super(LoaderStorage, self).__init__(get_loader_db_path())
                LoaderStorage._initialized = True
                return
            elif not self._db or not self._db_t:
                self.setup_connections()
