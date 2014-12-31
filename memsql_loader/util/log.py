import os
import logging
import argparse

from memsql_loader.util import paths

_debug = False
_disable_stdout = False
_log_path = os.path.join(paths.get_data_dir(), 'memsql_loader.log')

_file_handler = None
_stream_handler = None

def setup(log_path=None, stdout_enabled=True):
    global _stream_handler, _file_handler

    if stdout_enabled:
        _stream_handler = logging.StreamHandler()
        format_str = "(%(levelname)s) %(message)s"
        _stream_handler.setFormatter(logging.Formatter(format_str))
    else:
        _stream_handler = False

    if log_path is not None:
        # mark that we are joining the file log
        try:
            with open(log_path, 'a') as logfile:
                logfile.write('Log file opened by %s\n' % os.getpid())

            _file_handler = logging.FileHandler(filename=log_path)
            format_str = "%(asctime)s %(levelname)s | %(process)d:%(name)s | %(message)s"
            formatter = logging.Formatter(format_str)
            _file_handler.setFormatter(formatter)
        except IOError:
            # we can't write a log file here
            pass

def update_verbosity(debug=False, extra_verbose=False):
    stdout_level = logging.DEBUG if (debug or extra_verbose) else logging.INFO
    if _stream_handler:
        _stream_handler.setLevel(stdout_level)
    if extra_verbose:
        logging.setLoggerClass(Logger)

class _SetDebug(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):
        global _debug
        _debug = True

class _SetStdout(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):
        global _disable_stdout
        _disable_stdout = True

class _SetLogPath(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):
        global _log_path
        _log_path = value
        setattr(namespace, self.dest, _log_path)

def configure(parser):
    parser.add_argument('-D', '--debug',
        default=False,
        action=_SetDebug,
        nargs=0,
        help="Enable verbose logging.")
    parser.add_argument('--disable-stdout',
        default=False,
        action=_SetStdout,
        nargs=0,
        help="Disable console output of logs.")
    parser.add_argument('--log-path',
        default=_log_path,
        action=_SetLogPath,
        help="Detailed logs will be saved at this path.")

def get_logger(name, level=None):
    return Logger(name, level or logging.DEBUG)

class Formatter(logging.Formatter):
    def format(self, record):
        msg = super(Formatter, self).format(record)

        if record.levelno == logging.INFO:
            msg = '> ' + msg
        elif record.levelno in (logging.ERROR, logging.CRITICAL, logging.FATAL, logging.WARN):
            msg = '! ' + msg
        elif record.levelno == logging.DEBUG:
            msg = '# ' + msg

        return msg

class Logger(logging.Logger):
    def __init__(self, name, level=logging.DEBUG):
        logging.Logger.__init__(self, name, level=level)
        self.propagate = False

        if _stream_handler is None:
            setup(log_path=_log_path, stdout_enabled=(not _disable_stdout))
            update_verbosity(debug=_debug)

        if _stream_handler is not False:
            self.addHandler(_stream_handler)
        if _file_handler is not None:
            self.addHandler(_file_handler)
