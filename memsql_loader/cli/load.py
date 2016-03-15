""" Queue a job """

import getpass
import httplib
import os
import pycurl
import sys
import time

from collections import defaultdict
from memsql_loader.api import shared
from memsql_loader.cli.server import ServerProcess
from memsql_loader.db import load_data, pool
from memsql_loader.loader_db.jobs import Jobs, Job
from memsql_loader.loader_db.tasks import Tasks
from memsql_loader.loader_db.storage import LoaderStorage
from memsql_loader.util import bootstrap, log, db_utils, cli_utils, schema, webhdfs, servers
from memsql_loader.util import super_json as json
from memsql_loader.util.command import Command
from simplejson import JSONDecodeError
from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection
import voluptuous as V

ER_CONFIG_VALIDATION = """Invalid specification:
%(formatted_spec)s

Error(s):
%(error_msg)s

For more information go to http://developers.memsql.com/docs/latest/loader/index.html.
"""

class _PasswordNotSpecified(object):
    pass

def _decode_escape_characters(opt):
    # This function attempts to parse special characters (e.g. \n) in a
    # command-line option.
    if opt:
        try:
            opt = opt.decode('string_escape')
        except ValueError:
            # We may get a ValueError here if the string has an odd number
            # of backslashes.  In that case, we just return the original
            # string.
            pass
    return opt

class RunLoad(Command):
    @staticmethod
    def configure(parser, subparsers):
        subparser = subparsers.add_parser('load', help='Kick off a load job', add_help=False,
            description='This command supports specifying load parameters with command line options or a spec file.')
        subparser.set_defaults(command=RunLoad)
        subparser.set_defaults(subparser=subparser)

        subparser.add_argument('--help', action='store_true', help='show this help message and exit.')

        subparser.add_argument('paths', type=str, nargs='*',
            help='Path to a valid spec file which defines the job to enqueue.')

        database_args = subparser.add_argument_group('database', description="Configure the database connection.")

        database_args.add_argument('-h', '--host', help='Hostname of the target database.', default=None)
        database_args.add_argument('-P', '--port', help='Port of the target database.', type=int, default=None)
        database_args.add_argument('-u', '--user', help='User of the target database.', default=None)
        database_args.add_argument('-p', '--password', help='Password of the target database.', default=None, const=_PasswordNotSpecified, nargs='?')

        dest_options = subparser.add_argument_group('destination', description="Configure the destination table.")
        dest_options.add_argument('-D', '--database', type=str, default=None, help='Target database name.')
        dest_options.add_argument('-t', '--table', type=str, default=None, help='Target table name.')

        file_access_options = subparser.add_argument_group('file access', description="Configure access to source files.")
        file_access_options.add_argument('--aws-access-key', type=str, default=None,
            help='AWS Access Key (defaults to AWS_ACCESS_KEY_ID environment variable).')
        file_access_options.add_argument('--aws-secret-key', type=str, default=None,
            help='AWS Secret Key (defaults to AWS_SECRET_ACCESS_KEY environment variable).')

        file_access_options.add_argument('--hdfs-host', type=str, default=None,
            help='The hostname of the HDFS cluster namenode (for loading files from HDFS).')
        file_access_options.add_argument('--webhdfs-port', type=int, default=None,
            help='The WebHDFS port for the HDFS cluster namenode.')
        file_access_options.add_argument('--hdfs-user', type=str, default=None,
            help='The username to use when making HDFS requests.')

        load_data_options = subparser.add_argument_group('load data options', description="Configure the target LOAD DATA command")

        load_data_options.add_argument('--fields-terminated', '--delimiter', type=str, default=None,
            help="Field terminator (default '\\t').")

        load_data_options.add_argument('--fields-enclosed', type=str, default=None,
            help="Field enclose character (default '').")
        load_data_options.add_argument('--fields-escaped', type=str, default=None,
            help="Field escape character (default '\\').")

        load_data_options.add_argument('--lines-terminated', type=str, default=None,
            help="Line terminator (default '\\n').")
        load_data_options.add_argument('--lines-starting', type=str, default=None,
            help="Line prefix (default '').")

        load_data_options.add_argument('--ignore-lines', type=int, default=None,
            help="Number of lines to ignore.")

        dup_group = load_data_options.add_mutually_exclusive_group()
        dup_group.add_argument('--dup-ignore', '--ignore', default=False, action='store_true', help='Ignore rows that fail to parse or conflict with a unique key.')
        dup_group.add_argument('--dup-replace', '--replace', default=False, action='store_true', help='Replace rows in the database that have a unique key conflict with a row being inserted.')

        load_data_options.add_argument('--columns', type=str, default=None, help="List of columns to load into")
        load_data_options.add_argument('--file-id-column', type=str, default=None,
            help="An optional column that memsql-loader uses to save a per-file id on the row. This can"
            "be used by the loader to transactionally reload files.")

        load_data_options.add_argument('--non-local-load', default=None, action='store_true', help='Use the LOAD DATA command instead of LOAD DATA LOCAL.')

        subparser.add_argument('--script', type=str, default=None,
            help="Path to script or binary to process files before passing to LOAD DATA")

        subparser.add_argument('--spec', type=str, default=None,
            help="Path to JSON spec file for the load. Command line options override values in the spec.")
        subparser.add_argument('--print-spec', default=False, action='store_true',
            help="Print a JSON spec for this load job rather than running it.")

        subparser.add_argument('-f', '--force', default=False, action='store_true',
            help='Specify this flag to forcefully load all files in this job.\n'
                 'NOTE: This will cancel any currently queued or running tasks that are loading files in this job.')
        subparser.add_argument('-d', '--dry-run', default=False, action='store_true',
            help='Lists each file and the corresponding LOAD DATA command, but does not load anything.')

        subparser.add_argument('--debug', default=False, action='store_true',
            help='Enable verbose logging.')

        subparser.add_argument('--sync', default=False, action='store_true',
            help='Wait until the current load finishes before exiting.')

        subparser.add_argument('--no-daemon', default=False, action='store_true',
            help="Do not start the daemon process automatically if it's not running.")

        subparser.add_argument('--skip-checks', default=False, action='store_true',
            help="Skip various initialization checks, e.g. checking that we can connect to any HDFS hosts.")

    def ensure_bootstrapped(self):
        if not bootstrap.check_bootstrapped():
            bootstrap.bootstrap()

    @staticmethod
    def pre_process_options(options, logger):
        # Pre-process some options before sending it to the schema builder
        #   1) columns need to be parsed into a list
        #   2) duplicate_key_method needs to be fixed
        if not options.paths:
            # This helps us identify the case where no paths are
            # specified (e.g. if a --spec argument is provided); it keeps us
            # from assuming that paths is an empty list.
            options.paths = None
        if options.columns:
            # UNDONE: does this need to be more sophisticated?
            options.columns = [x.strip() for x in options.columns.split(",")]

        if options.dup_ignore:
            options.duplicate_key_method = 'ignore'
        elif options.dup_replace:
            options.duplicate_key_method = 'replace'
        else:
            options.duplicate_key_method = None

        # Allow strings like '\n' or '\u0001' to be passed in for the following
        # fields; we will escape them automatically.
        options.fields_terminated = _decode_escape_characters(options.fields_terminated)
        options.fields_enclosed = _decode_escape_characters(options.fields_enclosed)
        options.fields_escaped = _decode_escape_characters(options.fields_escaped)
        options.lines_terminated = _decode_escape_characters(options.lines_terminated)
        options.lines_starting = _decode_escape_characters(options.lines_starting)

    def process_options(self):
        """ This function validates the command line options and converts the options
        into a spec file. The spec file is then passed through the job schema validator
        for further validation."""
        if self.options.help:
            self.options.subparser.print_help()
            sys.exit(0)

        log.update_verbosity(debug=self.options.debug)

        if self.options.spec:
            try:
                with open(self.options.spec, 'r') as f:
                    base_spec = json.loads(f.read())
            except JSONDecodeError:
                print >>sys.stderr, "Failed to load spec file '%s': invalid JSON" % self.options.spec
                sys.exit(1)
            except IOError as e:
                print >>sys.stderr, "Unable to open spec file '%s': %s" % (self.options.spec, str(e))
                sys.exit(1)
        else:
            base_spec = {}

        self.pre_process_options(self.options, self.logger)

        if self.options.password == _PasswordNotSpecified:
            password = getpass.getpass('Enter password: ')
            self.options.password = password

        try:
            merged_spec = schema.build_spec(base_spec, self.options)
        except schema.InvalidKeyException as e:
            self.logger.error(str(e))
            sys.exit(1)

        # only pull the AWS arguments from the command line if there is
        # at least one S3 path. This is more of a UX thing, as having them
        # there won't break anything.
        try:
            paths = merged_spec['source']['paths']
        except KeyError:
            paths = []

        for path in paths:
            if path.startswith('s3://'):
                schema.DEFAULT_AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
                schema.DEFAULT_AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
                break

        try:
            self.job = Job(merged_spec)
            self.logger.debug("Produced spec:\n%s", json.pformat(self.job.spec))
        except V.Invalid as err:
            if isinstance(err, V.MultipleInvalid):
                errors = err.errors
            else:
                errors = [err]

            error_msgs = []
            seen_paths = []

            if self.options.spec:
                er_config_validation = """\
Invalid specification:
%(formatted_spec)s

Error(s):
%(error_msg)s"""
                er_msg_fmt = " - Error [%(error_path)s] (or %(error_cmd_line)s on the command line): %(message)s."
            else:
                er_config_validation = """\
Invalid command line options for load:

%(error_msg)s"""
                er_msg_fmt = " - Invalid value for %(error_cmd_line)s: %(message)s."

            for e in errors:
                extra_key = (e.message == 'extra keys not allowed')

                error_path = '.'.join([str(leg) for leg in e.path])

                cmd_line_opt = schema.get_command_line_mapping([x for x in e.path])
                if cmd_line_opt == 'paths':
                    cmd_line_opt = "the path argument (positional)"
                else:
                    cmd_line_opt = '--' + cmd_line_opt

                if any((error_path in seen_path) for seen_path in seen_paths):
                    # we do this because voluptuous triggers missing
                    # key errors for any required key that has a sub-error
                    continue

                seen_paths.append(error_path)

                error_msgs.append(
                    er_msg_fmt % {
                        'error_path': error_path, 'error_cmd_line': cmd_line_opt,
                        'message': 'key %s is not allowed' % error_path if extra_key else e.error_message})

            self.logger.error(er_config_validation % {
                'formatted_spec': json.pformat(merged_spec),
                'error_msg': "\n".join(error_msgs)
            })
            sys.exit(1)

        if self.options.print_spec:
            print json.pformat(self.job.spec)
            sys.exit(0)

    def validate_conditions(self):
        """ This happens after schema validation, and it checks the viability of the
        job based on "external" conditions like the existence of files, database connectivity,
        etc. We want to do this after we print_spec, because these checks don't have to do
        with the validity of the schema format."""

        self.s3_conn = None
        for path in self.job.paths:
            self.validate_path_conditions(path)

        # validate database/table exists
        with pool.get_connection(database='INFORMATION_SCHEMA', **self.job.spec.connection) as conn:
            has_database, has_table = db_utils.validate_database_table(conn, self.job.spec.target.database, self.job.spec.target.table)
        if not has_database:
            self.logger.error("The specified database `%s` does not exist.", self.job.spec.target.database)
            sys.exit(1)

        if not has_table:
            self.logger.error("The specified table `%s` does not exist.", self.job.spec.target.table)
            sys.exit(1)

        file_id_col = self.job.spec.options.file_id_column
        with pool.get_connection(database='INFORMATION_SCHEMA', **self.job.spec.connection) as conn:
            if not db_utils.validate_file_id_column(conn, self.job.spec.target.database, self.job.spec.target.table, file_id_col):
                self.logger.error("The `file_id_column` specified (%s) must exist in the table and be of type BIGINT UNSIGNED", file_id_col)
                sys.exit(1)

    def validate_path_conditions(self, path):
        if path.scheme == 's3':
            is_anonymous = self.job.spec.source.aws_access_key is None or self.job.spec.source.aws_secret_key is None
            if is_anonymous:
                self.logger.debug('Either access key or secret key was not specified, connecting to S3 as anonymous')
                self.s3_conn = S3Connection(anon=True)
            else:
                self.logger.debug('Connecting to S3')
                self.s3_conn = S3Connection(self.job.spec.source.aws_access_key, self.job.spec.source.aws_secret_key)

            try:
                if not cli_utils.RE_VALIDATE_BUCKET_NAME.match(path.bucket):
                    raise cli_utils.AWSBucketNameInvalid("Bucket name is not valid")

                self.s3_conn.get_bucket(path.bucket)
            except S3ResponseError as e:
                if e.status == 403:
                    self.logger.error('Invalid credentials for this bucket, aborting')
                elif e.status == 404:
                    self.logger.error('Bucket not found, aborting')
                else:
                    self.logger.error("Accessing S3 bucket resulted in %s %s, aborting" % (e.status, e.reason))
                sys.exit(1)
            except cli_utils.AWSBucketNameInvalid as e:
                self.logger.error(e.message)
                sys.exit(1)
        elif path.scheme == 'hdfs':
            if self.job.spec.source.webhdfs_port is None:
                self.logger.error('source.webhdfs_port must be defined for HDFS jobs')
                sys.exit(1)

            if not self.options.skip_checks:
                try:
                    # We try getting a content summary of the home directory
                    # for HDFS to make sure that we can connect to the WebHDFS
                    # server.
                    curl = pycurl.Curl()

                    url = webhdfs.get_webhdfs_url(
                        self.job.spec.source.hdfs_host,
                        self.job.spec.source.webhdfs_port,
                        self.job.spec.source.hdfs_user, 'GETCONTENTSUMMARY', '')

                    curl.setopt(pycurl.URL, url)

                    def _check_hdfs_response(data):
                        if 'It looks like you are making an HTTP request to a Hadoop IPC port' in data:
                            self.logger.error(
                                'You have provided an IPC port instead of the '
                                'WebHDFS port for the webhdfs-port argument.')

                    curl.setopt(pycurl.WRITEFUNCTION, _check_hdfs_response)
                    curl.perform()
                    status_code = curl.getinfo(pycurl.HTTP_CODE)
                    if status_code != httplib.OK:
                        self.logger.error('HTTP status code %s when testing WebHDFS connection' % status_code)
                        self.logger.error('Make sure your HDFS server is running and WebHDFS is enabled and ensure that you can access the data at %s' % url)
                        sys.exit(1)
                except pycurl.error as e:
                    errno = e.args[0]
                    self.logger.error('libcurl error %s when testing WebHDFS connection' % errno)
                    self.logger.error('Make sure your HDFS server is running and WebHDFS is enabled and ensure that you can access the data at %s' % url)
                    sys.exit(1)

    def queue_job(self):
        all_keys = list(self.job.get_files(s3_conn=self.s3_conn))

        paths = self.job.spec.source.paths

        if self.options.dry_run:
            print "DRY RUN SUMMARY:"
            print "----------------"
            if len(all_keys) == 0:
                print "Paths %s matched no files" % ([str(p) for p in paths])
            else:
                print "List of files to load:"
                for key in all_keys:
                    print key.name
                print "Example LOAD DATA statement to execute:"
                file_id = self.job.get_file_id(all_keys[0])
                print load_data.build_example_query(self.job, file_id)
            sys.exit(0)
        elif len(all_keys) == 0:
            self.logger.warning("Paths %s matched no files. Please check your path specification (be careful with relative paths)." % ([str(p) for p in paths]))

        self.jobs = None
        spec = self.job.spec
        try:
            self.logger.info('Creating job')
            self.jobs = Jobs()
            self.jobs.save(self.job)

            self.tasks = Tasks()

            etags = []
            for key in all_keys:
                if key.scheme in ['s3', 'hdfs']:
                    etags.append(key.etag)

            if etags and not self.options.force:
                database, table = spec.target.database, spec.target.table
                host, port = spec.connection.host, spec.connection.port
                competing_job_ids = [j.id for j in self.jobs.query_target(host, port, database, table)]
                md5_map = self.get_current_tasks_md5_map(etags, competing_job_ids)
            else:
                # For files loading on the filesystem, we are not going to MD5 files
                # for performance reasons. We are also basing this on the assumption
                # that filesystem loads are generally a one-time operation.
                md5_map = None
                if self.options.force:
                    self.logger.info('Loading all files in this job, regardless of identical files that are currently loading or were previously loaded (because of the --force flag)')
                if self.job.spec.options.file_id_column is not None:
                    self.logger.info('Since you\'re using file_id_column, duplicate records will be checked and avoided')

            count = self.submit_files(all_keys, md5_map, self.job, self.options.force)

            if count == 0:
                self.logger.info('Deleting the job, it has no child tasks')
                try:
                    self.jobs.delete(self.job)
                except:
                    self.logger.error("Rollback failed for job: %s", self.job.id)
            else:
                self.logger.info("Successfully queued job with id: %s", self.job.id)

                if not servers.is_server_running():
                    self.start_server()

                if self.options.sync:
                    self.wait_for_job()

        except (Exception, AssertionError):
            self.logger.error('Failed to submit files, attempting to roll back job creation...')
            exc_info = sys.exc_info()
            if self.jobs is not None:
                try:
                    self.jobs.delete(self.job)
                except:
                    self.logger.error("Rollback failed for job: %s", self.job.id)
            # Have to use this old-style raise because raise just throws
            # the last exception that occured, which could be the one in
            # the above try/except block and not the original exception.
            raise exc_info[0], exc_info[1], exc_info[2]

    def get_current_tasks_md5_map(self, etags, bad_job_ids):
        if not etags:
            return {}
        job_ids_string = ','.join("'%s'" % job_id for job_id in bad_job_ids)

        def _get_matching_tasks(md5_list):
            md5_list_string = ','.join("'%s'" % md5 for md5 in md5_list)
            predicate_sql = "md5 IN (%s) AND job_id IN (%s)" % (md5_list_string, job_ids_string)

            return self.tasks.get_tasks_in_state(
                [ shared.TaskState.QUEUED, shared.TaskState.RUNNING, shared.TaskState.SUCCESS ],
                extra_predicate=(predicate_sql, {}))

        matching_tasks = self.inlist_split(filter(None, etags), _get_matching_tasks, [])

        md5_map = defaultdict(lambda: [])
        for task in matching_tasks:
            md5_map[task.md5].append(task.data['key_name'])
        return md5_map

    def inlist_split(self, inlist, inlist_query, initializer):
        INLIST_SIZE = 2000
        ret = initializer
        for i in xrange(0, len(inlist), INLIST_SIZE):
            ret += inlist_query(inlist[i:i + INLIST_SIZE])
        return ret

    def submit_files(self, keys, md5_map, job, force):
        if force:
            def _finish_jobs_with_file_id(file_id_list):
                # Opens us up to SQL injection, but we have to do this
                # because we need uniform types or else the inlist will
                # not be parameterized
                # The reason for the sort is to avoid a deadlock inside MemSQL.
                # Without the sort, we risk deadlocking against other QueueJobs
                # running which collide on the same file_id(s).  More info: T11636
                serialized = ','.join('"%s"' % str(file_id) for file_id in sorted(file_id_list))
                return self.tasks.bulk_finish(extra_predicate=("file_id IN (%s)" % serialized, {}))
            file_ids = [ job.get_file_id(key) for key in keys ]
            tasks_cancelled = self.inlist_split(file_ids, _finish_jobs_with_file_id, 0)
            if tasks_cancelled > 0:
                if tasks_cancelled == 1:
                    msg = "--force was specified, cancelled %d queued or running task that was loading a file identical to files in this job"
                else:
                    msg = "--force was specified, cancelled %d queued or running tasks that were loading files identical to files in this job"
                self.logger.info(msg, tasks_cancelled)

        self.logger.info('Submitting files')

        count = 0
        ignored_count = 0
        for index, key in enumerate(keys):
            if index % 1000 == 0:
                sys.stdout.write('. ')
                sys.stdout.flush()

            if not (md5_map and key.name in md5_map[key.etag]):
                file_id = job.get_file_id(key)
                data = {
                    'scheme': key.scheme,
                    'key_name': key.name
                }
                if key.bucket is not None:
                    data['bucket'] = key.bucket.name
                self.tasks.enqueue(
                    data, job_id=job.id, file_id=str(file_id), md5=key.etag,
                    bytes_total=key.size)
                count += 1
            else:
                ignored_count += 1

        sys.stdout.write('\n')
        self.logger.info("Submitted %d files", count)
        if ignored_count > 0:
            self.logger.info("Ignored %d files that are identical to currently loading or previously loaded files.", ignored_count)
            self.logger.info('Run again with --force to load these files anyways.')

        return count

    def start_server(self):
        if self.options.no_daemon:
            self.logger.warn(
                "There don't seem to be any running MemSQL Loader servers. "
                "Please start one.")
            if self.options.sync:
                self.logger.error(
                    "You have specified --sync, but there are no running "
                    "MemSQL Loader servers.")
                sys.exit(1)
        else:
            self.logger.info("Starting a MemSQL Loader server")
            if not self.options.sync:
                self.logger.info(
                    "This load job will run in the background.  You can "
                    "monitor its progress with memsql-loader job %s" % (self.job.id))
            with LoaderStorage.fork_wrapper():
                ServerProcess(daemonize=True).start()

    def wait_for_job(self):
        self.logger.info("Waiting for job %s to finish..." % self.job.id)
        num_unfinished_tasks = -1
        unfinished_states = [ shared.TaskState.RUNNING, shared.TaskState.QUEUED ]
        predicate = ('job_id = :job_id', { 'job_id': self.job.id })
        while num_unfinished_tasks != 0:
            try:
                time.sleep(0.5)
                num_unfinished_tasks = len(self.tasks.get_tasks_in_state(
                    unfinished_states, extra_predicate=predicate))
            except KeyboardInterrupt:
                self.logger.info(
                    'Caught Ctrl-C. This load will continue running in the '
                    'background.  You can monitor its progress with '
                    'memsql-loader job %s' % (self.job.id))
                sys.exit(0)
        successful_tasks = self.tasks.get_tasks_in_state(
            [ shared.TaskState.SUCCESS ], extra_predicate=predicate)
        error_tasks = self.tasks.get_tasks_in_state(
            [ shared.TaskState.ERROR ], extra_predicate=predicate)
        cancelled_tasks = self.tasks.get_tasks_in_state(
            [ shared.TaskState.CANCELLED ], extra_predicate=predicate)
        self.logger.info("Job %s finished with %s successful tasks, %s cancelled tasks, and %s errored tasks" % (self.job.id, len(successful_tasks), len(cancelled_tasks), len(error_tasks)))
        if error_tasks:
            self.logger.info("Error messages include: ")
            for task in error_tasks[:10]:
                if task.data.get('error'):
                    self.logger.error(task.data['error'])
            self.logger.info("To see all error messages, run: memsql-loader tasks %s" % self.job.id)
            sys.exit(1)
        sys.exit(0)

    def run(self):
        self.logger = log.get_logger('Load')
        self.process_options()

        self.validate_conditions()
        self.queue_job()
