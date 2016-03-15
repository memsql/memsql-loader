""" Stop any currently-running worker servers """

import os
import signal
import sys

from memsql_loader.util.command import Command
from memsql_loader.util import servers

class StopServer(Command):
    @staticmethod
    def configure(parser, subparsers):
        subparser = subparsers.add_parser('stop-server', help='Stop the currently-running server')
        subparser.set_defaults(command=StopServer)

    def run(self):
        if not servers.is_server_running():
            print 'No currently running servers'
            sys.exit(0)
        pid = servers.get_server_pid()
        try:
            os.kill(pid, signal.SIGQUIT)
            print 'Stopped server with PID %s' % pid
        except os.error as e:
            print 'Error killing server with PID %s: %s' % (pid, e)
