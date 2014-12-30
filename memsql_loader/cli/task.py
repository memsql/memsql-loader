import sys

from clark.super_enum import SuperEnum

from memsql_loader.util.command import Command
from memsql_loader.util import log, super_json as json

from memsql_loader.api import exceptions
from memsql_loader.api.task import Task as TaskApi

class Task(Command):
    @staticmethod
    def configure(parser, subparsers):
        subparser = subparsers.add_parser('task', help='Show information about a single task')
        subparser.set_defaults(command=Task)

        subparser.add_argument('task_id', type=int,
            help='The ID of the task to lookup')

    def run(self):
        self.logger = log.get_logger('Task')
        self.task_api = TaskApi()

        try:
            result = self.task_api.query({ 'task_id': self.options.task_id })
        except exceptions.ApiException as e:
            print e.message
            sys.exit(1)

        result = { k: str(v) if isinstance(v, SuperEnum.Element) else v for k, v in result.items() }
        print json.dumps(result, sort_keys=True, indent=4 * ' ')
