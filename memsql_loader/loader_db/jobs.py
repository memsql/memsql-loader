from memsql_loader.loader_db.storage import LoaderStorage
from memsql_loader.util.attr_dict import AttrDict
from memsql_loader.util import super_json as json
from memsql_loader.util import apsw_sql_utility, apsw_helpers, log, schema
from memsql_loader.util.apsw_sql_step_queue.time_helpers import unix_timestamp
from memsql_loader.vendor import glob2

from boto.exception import S3ResponseError
from pywebhdfs.webhdfs import PyWebHdfsClient
import uuid
import datetime
import os
import hashlib

PRIMARY_TABLE = apsw_sql_utility.TableDefinition('jobs', """\
CREATE TABLE IF NOT EXISTS jobs (
    id BINARY(32) PRIMARY KEY,
    created DATETIME NOT NULL,
    spec TEXT NOT NULL
)""", index_columns=('created',))

def hash_64_bit(value):
    result = hashlib.sha256(value.encode('utf-8'))
    return int(result.hexdigest()[:16], 16)

class Jobs(apsw_sql_utility.APSWSQLUtility):
    def __init__(self):
        super(Jobs, self).__init__(LoaderStorage())

        self._define_table(PRIMARY_TABLE)

    def save(self, job):
        assert isinstance(job, Job), 'job must be of type Job'
        with self.storage.transaction() as cursor:
            cursor.execute('''
                REPLACE INTO jobs (id, created, spec)
                VALUES (?, DATETIME(?, 'unixepoch'), ?)
            ''', (job.id, unix_timestamp(datetime.datetime.utcnow()), job.json_spec()))

    def delete(self, job):
        assert isinstance(job, Job), 'job must be of type Job'
        with self.storage.transaction() as cursor:
            cursor.execute('DELETE FROM jobs WHERE id = ?', (job.id,))

    def get(self, job_id):
        with self.storage.transaction() as cursor:
            job = apsw_helpers.get(
                cursor, 'SELECT id, spec FROM jobs WHERE id = ?', job_id)

        if job is not None:
            job['spec'] = json.loads(job.spec)
            return Job(job.spec, job.id)

    def all(self):
        with self.storage.cursor() as cursor:
            result = apsw_helpers.query(
                cursor, 'SELECT id, spec FROM jobs ORDER BY created ASC')

        return [Job(json.loads(job.spec), job.id) for job in result]

    def query_target(self, host, port, database, table):
        with self.storage.cursor() as cursor:
            result = apsw_helpers.query(cursor, 'SELECT id, spec FROM jobs')

        ret = []
        for job in result:
            spec = json.loads(job.spec)
            if spec['connection']['host'] != host:
                continue
            if spec['connection']['port'] != port:
                continue
            if spec['target']['database'] != database:
                continue
            if spec['target']['table'] != table:
                continue
            ret.append(Job(spec, job.id))
        return ret

class Job(object):
    def __init__(self, spec, job_id=None):
        """ Spec should be passed in as a python Object, if job_id isn't passed in it will be generated """
        self.id = job_id if job_id is not None else uuid.uuid1().hex
        self.spec = schema.validate_spec(spec)
        self.paths = [ schema.LoadPath(path) for path in self.spec.source.paths ]

    def json_spec(self):
        return json.dumps(self.spec)

    def get_file_id(self, key):
        """ Returns the file id for the specified key """
        bucket_name = ''
        if key.bucket is not None:
            bucket_name = key.bucket.name
        return hash_64_bit(bucket_name + key.name)

    def has_file_id(self):
        assert 'file_id_column' in self.spec.options
        return self.spec.options.file_id_column is not None

    def get_files(self, s3_conn=None):
        # We are standardizing on UNIX semantics for file matching (vs. S3 prefix semantics). This means
        # we expect that on both S3 and UNIX:
        #   bucket/1
        #   bucket/2
        #   bucket/a/1
        #   bucket/a/2
        #
        # bucket/* matches just 1,2 and bucket/** matches all 4 files
        logger = log.get_logger('Jobs')
        for load_path in self.paths:
            if load_path.scheme == 's3':
                bucket = s3_conn.get_bucket(load_path.bucket)
                s3_globber = glob2.S3Globber(bucket)

                for keyname in s3_globber.glob(load_path.pattern):
                    if not s3_globber.isdir(keyname):
                        try:
                            key = s3_globber.get_key(keyname)
                            if key is not None:
                                yield AttrDict({
                                    'scheme': 's3',
                                    'name': key.name,
                                    'etag': key.etag,
                                    'size': key.size,
                                    'bucket': bucket
                                })
                            else:
                                logger.warning("Key `%s` not found, skipping", keyname)
                        except S3ResponseError as e:
                            logger.warning("Received %s %s accessing `%s`, skipping", e.status, e.reason, keyname)
            elif load_path.scheme == 'file':
                fs_globber = glob2.Globber()
                for fname in fs_globber.glob(load_path.pattern):
                    if not fs_globber.isdir(fname):
                        yield AttrDict({
                            'scheme': 'file',
                            'name': fname,
                            'etag': None,
                            'size': os.path.getsize(fs_globber._normalize_string(fname)),
                            'bucket': None
                        })
            elif load_path.scheme == 'hdfs':
                hdfs_host = self.spec.source.hdfs_host
                webhdfs_port = self.spec.source.webhdfs_port
                hdfs_user = self.spec.source.hdfs_user

                client = PyWebHdfsClient(
                    hdfs_host, webhdfs_port, user_name=hdfs_user)
                hdfs_globber = glob2.HDFSGlobber(client)
                for fname in hdfs_globber.glob(load_path.pattern):
                    if not hdfs_globber.isdir(fname):
                        fileinfo = hdfs_globber.get_fileinfo(fname)
                        yield AttrDict({
                            'scheme': 'hdfs',
                            'name': fileinfo['path'],
                            'etag': fileinfo['etag'],
                            'size': fileinfo['length'],
                            'bucket': None
                        })
            else:
                assert False, "Unknown scheme %s" % load_path.scheme
