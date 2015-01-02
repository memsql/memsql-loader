import argparse
from collections import OrderedDict
import sys
from memsql_loader import __version__
from memsql_loader.util import log

from memsql_loader.cli import server, jobs, job, tasks, task, cancel_task, cancel_job, ps, load, status, stop_server, clear_loader_db
from memsql_loader.cli import log as log_cmd

# These are ordered such that the first command is the first command that
# the user should see. Any future commands should also follow this style.
COMMANDS = OrderedDict([
    ('load', load.RunLoad),
    ('ps', ps.Processes),
    ('jobs', jobs.Jobs),
    ('job', job.Job),
    ('tasks', tasks.Tasks),
    ('task', task.Task),
    ('cancel-job', cancel_job.CancelJob),
    ('cancel-task', cancel_task.CancelTask),
    ('server', server.Server),
    ('status', status.Status),
    ('stop-server', stop_server.StopServer),
    ('clear-loader-db', clear_loader_db.ClearLoaderDb),
    ('log', log_cmd.Log)
])

def make_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument('--version',
        action='version',
        version='memsql-loader ' + __version__)

    log.configure(parser)

    subparsers = parser.add_subparsers(parser_class=argparse.ArgumentParser)
    [command.configure(parser, subparsers) for command in COMMANDS.values()]

    return parser

def load_options(args=None):
    parser = make_parser()
    args = sys.argv[1:] if args is None else args
    if len(set(args) & set(COMMANDS.keys())) > 0:
        return parser.parse_args(args)
    else:
        return parser.parse_args(args + [ '--help' ])
