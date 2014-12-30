""" Stop any currently-running worker servers """

import os
import signal
import sys

from memsql_loader.util.command import Command
from memsql_loader.loader_db.servers import Servers

class StopServer(Command):
    @staticmethod
    def configure(parser, subparsers):
        subparser = subparsers.add_parser('stop-server', help='Stop the currently-running server')
        subparser.set_defaults(command=StopServer)

    def run(self):
        self.servers = Servers()
        online_servers = self.servers.online_servers()
        if not online_servers:
            print 'No currently running servers'
            sys.exit(0)
        for row in online_servers:
            pid = row.pid
            try:
                os.kill(pid, signal.SIGQUIT)
                print 'Stopped server with PID %s' % pid
            except os.error as e:
                print 'Error killing server with PID %s: %s' % (pid, e)
