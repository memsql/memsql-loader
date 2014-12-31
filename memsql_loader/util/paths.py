import os
import sys

MEMSQL_LOADER_PATH_ENV = "MEMSQL_LOADER_DATA_DIRECTORY"

def get_data_dir():
    if os.getenv(MEMSQL_LOADER_PATH_ENV, None):
        target = os.environ[MEMSQL_LOADER_PATH_ENV]
    else:
        target = os.path.join(os.path.expanduser("~"), ".memsql-loader")
    parent = os.path.dirname(target)
    if not os.path.exists(parent):
        print("Can't load MemSQL Loader Database. Please ensure that the path '%s' exists." % parent)
        sys.exit(1)
    if not os.path.exists(target):
        try:
            os.mkdir(target)
        except OSError:
            print("Failed to create MemSQL Loader database path: %s" % target)
            sys.exit(1)
    return target
