""" Start the worker server """

from memsql_loader.util.command import Command
from memsql_loader.util import log, cli_utils
from memsql_loader.execution.worker_pool import WorkerPool
from memsql_loader.db import pool
from memsql_loader.loader_db import storage
from memsql_loader.util.daemonize import daemonize
from memsql_loader.util.setuser import setuser
from memsql_loader.util import bootstrap, servers
import argparse
import multiprocessing
import time
import sys
import signal

WORKER_WARN_THRESHOLD = 100

# This class is used in the load command to start a server with default
# arguments in a separate process.
class ServerProcess(multiprocessing.Process):
    def __init__(self, daemonize=False, num_workers=None, idle_timeout=None):
        self.num_workers = num_workers
        self.idle_timeout = idle_timeout
        super(ServerProcess, self).__init__()
        self.daemonize = daemonize

    def run(self):
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(parser_class=argparse.ArgumentParser)
        Server.configure(parser, subparsers)
        log.configure(parser)
        fake_args = ['server']
        if self.num_workers is not None:
            fake_args.append('--num-workers')
            fake_args.append(str(self.num_workers))
        if self.idle_timeout is not None:
            fake_args.append('--idle-timeout')
            fake_args.append(str(self.idle_timeout))
        options = parser.parse_args(fake_args)
        options.daemonize = self.daemonize
        Server(options)

class Server(Command):
    @staticmethod
    def configure(parser, subparsers):
        subparser = subparsers.add_parser('server', help='Start a server that runs queued jobs')
        subparser.set_defaults(command=Server)
        subparser.add_argument('-d', '--daemonize', action='store_true', help='Daemonize the MemSQL Loader server process.', default=False)
        subparser.add_argument('--set-user', default=None, help='Specify a user for MemSQL Loader to use.')
        subparser.add_argument('-n', '--num-workers', default=None, type=int,
            help='Number of workers to run; equates to the number of loads that can be run in parallel.')
        subparser.add_argument('-i', '--idle-timeout', default=None, type=int,
            help='Seconds before server automatically shuts down; defaults to never.')
        subparser.add_argument('-f', '--force-workers', action='store_true',
            help='Ignore warnings on number of workers. This is potentially dangerous!')

    def ensure_bootstrapped(self):
        if not bootstrap.check_bootstrapped():
            bootstrap.bootstrap()

    def run(self):
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGQUIT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

        self.exiting = False
        self.logger = log.get_logger('Server')

        if self.options.num_workers is not None and self.options.num_workers < 1:
            self.logger.error('number of workers must be a positive integer')
            sys.exit(1)

        if self.options.idle_timeout is not None and self.options.idle_timeout < 1:
            self.logger.error('idle timeout must be a positive integer')
            sys.exit(1)

        # switch over to the correct user as soon as possible
        if self.options.set_user is not None:
            if not setuser(self.options.set_user):
                self.logger.error('failed to switch to user %s' % self.options.set_user)
                sys.exit(1)

        if servers.is_server_running():
            self.logger.error('A MemSQL Loader server is already running.')
            sys.exit(1)

        if self.options.daemonize:
            # ensure connection pool forks from daemon
            pool.close_connections()
            with storage.LoaderStorage.fork_wrapper():
                daemonize(self.options.log_path)
            pool.recreate_pool()

        # record the fact that we've started successfully
        servers.write_pid_file()

        if self.options.num_workers > WORKER_WARN_THRESHOLD and not self.options.force_workers:
            if not cli_utils.confirm('Are you sure you want to start %d workers? This is potentially dangerous.' % self.options.num_workers, default=False):
                print 'Exiting.'
                sys.exit(1)

        self.logger.debug('Starting worker pool')
        self.pool = WorkerPool(num_workers=self.options.num_workers, idle_timeout=self.options.idle_timeout)

        print 'MemSQL Loader Server running'

        loader_db_name = storage.MEMSQL_LOADER_DB
        has_valid_loader_db_conn = False
        while not self.exiting:
            try:
                if bootstrap.check_bootstrapped():
                    has_valid_loader_db_conn = True
                    if self.pool.poll():
                        time.sleep(1)
                    else:
                        self.logger.info('Server has been idle for more than the idle timeout (%d seconds). Stopping.', self.options.idle_timeout)
                        self.exit()
                else:
                    if has_valid_loader_db_conn:
                        self.logger.warn('The %s database is unreachable or not ready; stopping worker pool', loader_db_name)
                        self.pool.stop()
                    has_valid_loader_db_conn = False
                    time.sleep(5)
            except KeyboardInterrupt:
                break

        self.stop()

    def exit(self):
        # This function is used to stop the server's main loop from a different
        # thread.  This is useful for testing.
        self.exiting = True

    def stop(self, unused_signal=None, unused_frame=None):
        self.pool.stop()
        pool.close_connections()
        servers.delete_pid_file()
        sys.exit(0)
