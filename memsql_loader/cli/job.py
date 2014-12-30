import sys, datetime

from clark.super_enum import SuperEnum

from memsql_loader.util.command import Command
from memsql_loader.util import log, super_json as json

from memsql_loader.api import exceptions
from memsql_loader.api.job import Job as JobApi
from memsql_loader.api.tasks import Tasks as TasksApi

class Job(Command):
    @staticmethod
    def configure(parser, subparsers):
        subparser = subparsers.add_parser('job', help='Show information about a single job')
        subparser.set_defaults(command=Job)

        subparser.add_argument('job_id',
            help='The ID of the job to lookup')

        subparser.add_argument('--spec', action='store_true', default=False,
            help='Output only the spec of this job')

    def run(self):
        self.logger = log.get_logger('Job')
        self.job_api = JobApi()
        self.tasks_api = TasksApi()

        try:
            result = self.job_api.query({ 'job_id': self.options.job_id })
        except exceptions.ApiException as e:
            print e.message
            sys.exit(1)

        if self.options.spec:
            print json.dumps(result.spec, sort_keys=True, indent=4 * ' ')
        else:
            try:
                finished_tasks = self.tasks_api.query({
                    'job_id': self.options.job_id,
                    'state': 'SUCCESS'
                })
            except exceptions.ApiException as e:
                print e.message
                sys.exit(1)

            files_loaded = len(finished_tasks)
            rows_loaded = reduce(lambda x, y: x + y.get('data', {}).get('row_count', 0), finished_tasks, 0)
            avg_rows_per_file = None
            avg_rows_per_second = None

            if files_loaded > 0:
                avg_rows_per_file = rows_loaded / files_loaded

                min_start_time = datetime.datetime.max
                max_stop_time = datetime.datetime.min
                for row in finished_tasks:
                    for step in row.steps:
                        if step['name'] == 'download':
                            min_start_time = min(min_start_time, step['start'])
                            max_stop_time = max(max_stop_time, step['stop'])
                            break
                    else:
                        continue
                avg_rows_per_second = rows_loaded / (max_stop_time - min_start_time).total_seconds()

            result['stats'] = { k: v for k, v in {
                'files_loaded': files_loaded,
                'rows_loaded': rows_loaded,
                'avg_rows_per_file': avg_rows_per_file,
                'avg_rows_per_second': avg_rows_per_second
            }.iteritems() if v is not None }

            if result.tasks_total > 0:
                result['stats'].update({
                    'success_rate': result.tasks_succeeded * 1.0 / result.tasks_total,
                    'error_rate': result.tasks_errored * 1.0 / result.tasks_total
                })

            result["database"] = result.spec["target"]["database"]
            result["table"] = result.spec["target"]["table"]

            result = dict(result)
            del result['spec']

            result = { k: str(v) if isinstance(v, SuperEnum.Element) else v for k, v in result.iteritems() }
            print json.dumps(result, sort_keys=True, indent=4 * ' ')
