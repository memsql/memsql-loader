import sys
import os

from memsql_loader.util.command import Command
from memsql_loader.util import log

class Log(Command):
    @staticmethod
    def configure(parser, subparsers):
        subparser = subparsers.add_parser('log', help='Tail the MemSQL Loader log file or print out it\'s path.')
        subparser.set_defaults(command=Log)

        subparser.add_argument('-p', '--path', default=False, action='store_true',
            help='Print out the MemSQL Loader logfile path and then exit.')
        subparser.add_argument('-n', '--characters', default=100, type=int,
            help='Output the last n characters rather than the last 100.')

    def run(self):
        if self.options.path:
            print(log._log_path)
        else:
            try:
                size = os.path.getsize(log._log_path)
                if self.options.characters > size:
                    self.options.characters = size

                logfile = open(log._log_path, 'r')
                logfile.seek(-1 * self.options.characters, 2)
                while True:
                    pos = logfile.tell()
                    line = logfile.readline()
                    if not line:
                        logfile.seek(pos)
                    else:
                        sys.stdout.write(line)
            except IOError:
                print("Failed to tail file %s" % log._log_path)
                sys.exit(1)
            except KeyboardInterrupt:
                sys.exit(0)
