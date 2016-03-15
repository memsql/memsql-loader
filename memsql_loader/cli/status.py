""" Determines if a MemSQL Loader server is running. """

import sys

from memsql_loader.util.command import Command
from memsql_loader.util import servers

class Status(Command):
    @staticmethod
    def configure(parser, subparsers):
        subparser = subparsers.add_parser('status', help='Determines if a MemSQL Loader server is running.')
        subparser.set_defaults(command=Status)

    def run(self):
        if servers.is_server_running():
            print 'A MemSQL Loader server is currently running.'
            sys.exit(0)
        else:
            print 'No currently running servers.'
            sys.exit(1)
