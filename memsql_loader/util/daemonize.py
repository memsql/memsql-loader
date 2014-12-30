import os
import sys

def daemonize(logfile):
    """ Daemonize the current process.
    When it returns you will be running in a daemonized child process.
    Mostly copied from: http://www.jejik.com/articles/2007/02/a_simple_unix_linux_daemon_in_python/
    """

    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError as e:
        sys.stderr.write("Fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
        sys.exit(1)

    os.chdir("/")
    os.setsid()
    os.umask(0)

    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError as e:
        sys.stderr.write("Fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
        sys.exit(1)

    sys.stdout.flush()
    sys.stderr.flush()

    # setup a new STDIN
    si = file('/dev/null', 'r')
    os.dup2(si.fileno(), sys.stdin.fileno())

    # setup new STDOUT
    output = '/dev/null' if logfile is None else logfile
    so = file(output, 'a+')
    se = file(output, 'a+', 0)
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())
