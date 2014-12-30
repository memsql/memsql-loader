from memsql_loader.loader_db import jobs, tasks, servers
from memsql_loader.loader_db import storage
from memsql_loader.util import apsw_helpers, log

MODELS = { 'jobs': jobs.Jobs, 'tasks': tasks.Tasks, 'servers': servers.Servers }

def check_bootstrapped():
    loader_storage = storage.LoaderStorage()
    with loader_storage.cursor() as cursor:
        rows = apsw_helpers.query(
            cursor, 'SELECT name FROM sqlite_master WHERE type = "table"')
    tables = [row.name for row in rows]
    return all([model in tables for model in MODELS.keys()])

def bootstrap(force=False):
    logger = log.get_logger('Bootstrap')  # noqa

    def write_log(title, name, msg):
        log_title_width = 28
        title = ("%s [%s]: " % (title, name)).rjust(log_title_width, ' ')
        logger.info(title + msg)

    write_log('Database', storage.MEMSQL_LOADER_DB, 'Checking...')
    if force:
        write_log('Database', storage.MEMSQL_LOADER_DB, 'Dropping...')
        storage.LoaderStorage.drop_database()
    write_log('Database', storage.MEMSQL_LOADER_DB, 'Ready.')

    for Model in MODELS.values():
        instance = Model()
        if not instance.ready():
            write_log('Table', Model.__name__, 'Bootstrapping...')
            instance.setup()
        write_log('Table', Model.__name__, 'Ready.')
