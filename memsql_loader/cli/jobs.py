import sys

from memsql_loader.util.command import Command
from memsql_loader.util.pretty_printer import PrettyPrinter, TableFormat
from memsql_loader.util import log

from memsql_loader.api.jobs import Jobs as JobsApi
from memsql_loader.api import shared, exceptions

class Jobs(Command):
    @staticmethod
    def configure(parser, subparsers):
        subparser = subparsers.add_parser('jobs', help='Show information about all jobs')
        subparser.set_defaults(command=Jobs)

        subparser.add_argument('-s', '--state',
            choices=shared.JobState.elements.keys(),
            type=lambda s: s.upper(),
            action='append',
            help='Only show jobs matching a particular set of states')

        subparser.add_argument('-o', '--order',
            choices=shared.SortDirection.elements.keys(),
            default=shared.SortDirection.ASC,
            type=lambda s: s.upper(),
            help='The order to display the results in')

        subparser.add_argument('-b', '--order-by', dest='order_by',
            choices=JobsApi.SORTABLE_COLUMNS,
            help='The column to sort the results by')

        subparser.add_argument('-p', '--page', type=int,
            help='The page of results to return')

        subparser.add_argument('-l', '--page-size', type=int,
            help='The number of results to return')

        subparser.add_argument('--json', action='store_true', default=False,
            help='Display results in JSON format')

    def run(self):
        self.logger = log.get_logger('Jobs')
        self.jobs_api = JobsApi()

        try:
            result = self.jobs_api.query({ k: v for k, v in {
                'state': self.options.state,
                'order': self.options.order,
                'order_by': self.options.order_by,
                'page': self.options.page,
                'page_size': self.options.page_size
            }.iteritems() if v })
        except exceptions.ApiException as e:
            print e.message
            sys.exit(1)

        if result:
            tablefmt = TableFormat.JSON if self.options.json else TableFormat.TABLE
            columns = JobsApi.SORTABLE_COLUMNS + ["database", "table"]

            for job in result:
                job["database"] = job.spec["target"]["database"]
                job["table"] = job.spec["target"]["table"]

            print PrettyPrinter(result, columns=columns, format=tablefmt, align={
                "database": "l",
                "table": "l",
            }).format()
        else:
            print 'No jobs found that match this query'
            sys.exit(1)
