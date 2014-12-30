""" Deletes the loader's database. """

import sys

from memsql_loader.util import cli_utils
from memsql_loader.util.command import Command
from memsql_loader.loader_db.storage import LoaderStorage


class ClearLoaderDb(Command):
    @staticmethod
    def configure(parser, subparsers):
        subparser = subparsers.add_parser('clear-loader-db', help="Deletes the database containing MemSQL Loader's queued, running, and finished jobs.")
        subparser.set_defaults(command=ClearLoaderDb)

    def run(self):
        prompt = 'Are you sure you want to delete the MemSQL Loader database?\nThe database contains queued, running, and finished jobs.'
        if not cli_utils.confirm(prompt, default=False):
            print 'Exiting.'
            sys.exit(1)
        LoaderStorage.drop_database()
        print 'MemSQL Loader database deleted.'
