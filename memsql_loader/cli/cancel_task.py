from memsql_loader.util.command import Command
from memsql_loader.util import log

from memsql_loader.loader_db.tasks import Tasks

class CancelTask(Command):
    @staticmethod
    def configure(parser, subparsers):
        subparser = subparsers.add_parser('cancel-task', help='Cancel a specific task')
        subparser.set_defaults(command=CancelTask)

        subparser.add_argument('task_id', type=int,
            help='The ID of the task to cancel')

    def run(self):
        self.logger = log.get_logger('CancelTask')

        self.tasks = Tasks()
        rows_affected = self.tasks.bulk_finish(extra_predicate=('id = :task_id', { 'task_id': self.options.task_id }))

        plural = not rows_affected == 1
        print 'Cancelled', rows_affected, 'task%s.' % ('s' if plural else '')
