""" Deletes the loader's database. """

import sys

from memsql_loader.util import cli_utils, servers
from memsql_loader.util.command import Command
from memsql_loader.loader_db.storage import LoaderStorage


class ClearLoaderDb(Command):
    @staticmethod
    def configure(parser, subparsers):
        subparser = subparsers.add_parser('clear-loader-db', help="Deletes the database containing MemSQL Loader's queued, running, and finished jobs.")
        subparser.set_defaults(command=ClearLoaderDb)

        subparser.add_argument('-f', '--force', help='Clear the loader database even if the MemSQL Loader server is running', action='store_true')

    def run(self):
        if not self.options.force:
            if servers.is_server_running():
                print 'Please stop any currently-running servers with stop-server before deleting the MemSQL Loader database.'
                sys.exit(1)

        prompt = 'Are you sure you want to delete the MemSQL Loader database?\nThe database contains queued, running, and finished jobs.'
        if not cli_utils.confirm(prompt, default=False):
            print 'Exiting.'
            sys.exit(1)
        LoaderStorage.drop_database()
        print 'MemSQL Loader database deleted.'
