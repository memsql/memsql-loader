import sys

from memsql_loader.util.command import Command
from memsql_loader.util.pretty_printer import PrettyPrinter, TableFormat
from memsql_loader.util import log

from memsql_loader.api.tasks import Tasks as TasksApi
from memsql_loader.api import shared, exceptions
from memsql_loader.loader_db.jobs import Jobs

class Tasks(Command):
    @staticmethod
    def configure(parser, subparsers):
        subparser = subparsers.add_parser('tasks', help='Show information about all child tasks of a job')
        subparser.set_defaults(command=Tasks)

        subparser.add_argument('job_id', type=str, nargs='?', default=None,
                               help='List tasks corresponding to this particular job.')

        subparser.add_argument('-s', '--state',
            choices=shared.TaskState.elements.keys(),
            type=lambda s: s.upper(),
            action='append',
            help='Only show tasks matching a particular set of states')

        subparser.add_argument('-o', '--order',
            choices=shared.SortDirection.elements.keys(),
            type=lambda s: s.upper(),
            default=shared.SortDirection.ASC,
            help='The order to display the results in')

        subparser.add_argument('-b', '--order-by', dest='order_by',
            choices=TasksApi.SORTABLE_COLUMNS,
            help='The column to sort the results by')

        subparser.add_argument('-l', '--page-size', type=int,
            help='The number of results to return')

        subparser.add_argument('-p', '--page', type=int,
            help='The page of results to return')

        subparser.add_argument('--json', action='store_true', default=False,
            help='Display results in JSON format')

        subparser.add_argument('--last-job', action='store_true', default=False,
            help='Show the tasks for the most recent job.')

    def run(self):
        self.logger = log.get_logger('Tasks')
        self.tasks_api = TasksApi()

        if not self.options.job_id and not self.options.last_job:
            print 'You must specify a job ID or use the --last-job option.'
            sys.exit(1)

        if self.options.last_job:
            jobs = Jobs()
            job_list = jobs.all()
            if not job_list:
                print 'No jobs found.'
                sys.exit(1)
            self.options.job_id = job_list[-1].id

        try:
            result = self.tasks_api.query({ k: v for k, v in {
                'job_id': self.options.job_id,
                'state': self.options.state,
                'order': self.options.order,
                'order_by': self.options.order_by,
                'page_size': self.options.page_size,
                'page': self.options.page,
            }.iteritems() if v })
        except exceptions.ApiException as e:
            print e.message
            sys.exit(1)

        if result:
            tablefmt = TableFormat.JSON if self.options.json else TableFormat.TABLE
            print PrettyPrinter(result, columns=TasksApi.SORTABLE_COLUMNS, format=tablefmt).format()
        else:
            print 'No tasks found that match this query'
            sys.exit(1)
