""" Determines if a MemSQL Loader server is running. """

import sys

from memsql_loader.util.command import Command
from memsql_loader.loader_db.servers import Servers

class Status(Command):
    @staticmethod
    def configure(parser, subparsers):
        subparser = subparsers.add_parser('status', help='Determines if a MemSQL Loader server is running.')
        subparser.set_defaults(command=Status)

    def run(self):
        self.servers = Servers()
        online_servers = self.servers.online_servers()
        if online_servers:
            print 'A MemSQL Loader server is currently running.'
            sys.exit(0)
        else:
            print 'No currently running servers.'
            sys.exit(1)
