import sys

from memsql_loader.util.command import Command
from memsql_loader.util import apsw_helpers, log

from memsql_loader.loader_db.storage import LoaderStorage
from memsql_loader.loader_db.tasks import Tasks

CANCEL_JOB_MESSAGE = '''
    Cancelled job%s matching ID `%s`, totalling %d task%s.
'''.strip()

class CancelJob(Command):
    @staticmethod
    def configure(parser, subparsers):
        subparser = subparsers.add_parser('cancel-job', help='Cancel a specific job')
        subparser.set_defaults(command=CancelJob)

        subparser.add_argument('job_id',
            help='The ID of the job to cancel')

        subparser.add_argument('--multiple', action='store_true', default=False,
            help='Allow the cancellation of multiple jobs matching the specified job ID')

    def run(self):
        self.logger = log.get_logger('CancelJob')

        self.tasks = Tasks()

        rows_affected = 0
        if self.options.multiple:
            rows_affected = self.tasks.bulk_finish(extra_predicate=("job_id LIKE :job_id", { 'job_id': self.options.job_id + '%%' }))
        else:
            loader_storage = LoaderStorage()
            with loader_storage.transaction() as cursor:
                jobs = apsw_helpers.query(cursor, '''
                    SELECT id FROM jobs WHERE id LIKE :job_id
                ''', job_id=self.options.job_id + '%')

            if len(jobs) > 1:
                print len(jobs), 'jobs match this job ID:'
                print '\n'.join([ row.id for row in jobs ])
                print 'Please use a more specific prefix or specify the `--multiple` flag if you'
                print 'would like to cancel more than one job.'
                sys.exit(1)
            elif len(jobs) == 0:
                print '0 jobs match this job ID.'
                sys.exit(1)
            else:
                rows_affected = self.tasks.bulk_finish(extra_predicate=("job_id = :job_id", { 'job_id': jobs[0].id }))

        job_suffix = '(s)' if self.options.multiple else ''
        task_suffix = 's' if not rows_affected == 1 else ''
        print CANCEL_JOB_MESSAGE % (job_suffix, self.options.job_id, rows_affected, task_suffix)
