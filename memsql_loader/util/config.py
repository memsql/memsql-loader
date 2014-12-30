import argparse
import os.path
import sys
from memsql_loader import __version__
from memsql_loader.util import log

from memsql_loader.cli import server, jobs, job, tasks, task, cancel_task, cancel_job, ps, load, stop_server, clear_loader_db

if getattr(sys, 'frozen', False):
    BASE_PATH = os.path.realpath(os.path.dirname(sys.executable))
else:
    BASE_PATH = os.path.realpath(os.path.join(os.path.dirname(__file__), '../..'))

# These are ordered such that the first command is the first command that
# the user should see. Any future commands should also follow this style.
COMMANDS = [ load.RunLoad, ps.Processes, jobs.Jobs, job.Job, tasks.Tasks, task.Task, cancel_job.CancelJob, cancel_task.CancelTask, server.Server, stop_server.StopServer, clear_loader_db.ClearLoaderDb ]
COMMAND_NAMES = [ 'load', 'ps', 'jobs', 'job', 'tasks', 'task', 'cancel-job', 'cancel-task', 'server', 'stop-server', 'clear-loader-db' ]

def make_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument('--version',
        action='version',
        version='memsql-loader ' + __version__)

    log.configure(parser)

    subparsers = parser.add_subparsers(parser_class=argparse.ArgumentParser)
    [command.configure(parser, subparsers) for command in COMMANDS]

    return parser

def load_options(args=None):
    parser = make_parser()
    args = sys.argv[1:] if args is None else args
    if len(set(args) & set(COMMAND_NAMES)) > 0:
        return parser.parse_args(args)
    else:
        return parser.parse_args(args + [ '--help' ])
