import atexit
import errno
import os

from memsql_loader.util import paths

def get_pid_file_path():
    return os.path.join(paths.get_data_dir(), "memsql-loader.pid")

def delete_pid_file():
    try:
        os.remove(get_pid_file_path())
    except Exception:
        pass

def write_pid_file():
    atexit.register(delete_pid_file)

    with open(get_pid_file_path(), 'w') as f:
        f.write("%s\n" % os.getpid())

def get_server_pid():
    try:
        with open(get_pid_file_path(), 'r') as f:
            return int(f.read().strip())
    except IOError as e:
        if e.errno == errno.ENOENT:
            return None
        raise

def is_server_running():
    pid = get_server_pid()
    if pid is None:
        return False
    try:
        # This call will succeed and do nothing if the process exists,
        # and it will raise an exception if the process does not exist.
        os.kill(pid, 0)
        return True
    except OSError:
        return False
