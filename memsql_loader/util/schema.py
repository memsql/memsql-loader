import os
import urlparse
import voluptuous as V

from memsql_loader.util.attr_dict import AttrDict
from memsql_loader.util import log
from memsql_loader.vendor import glob2

class LoadPath(object):
    def __init__(self, path):
        self.path = path

        parsed = urlparse.urlparse(path)
        self.bucket = None
        if parsed.scheme == 's3':
            self.scheme = 's3'
            self.bucket = parsed.netloc
            # this strips the starting /
            self.pattern = parsed.path[1:]
        elif parsed.scheme == 'hdfs':
            self.scheme = 'hdfs'
            self.pattern = parsed.netloc + parsed.path
            self.pattern = self.pattern.lstrip('/')
        elif parsed.scheme == 'file' or not parsed.scheme:
            self.scheme = 'file'
            # cannot use os.path.join because of the starting /
            self.pattern = parsed.netloc + parsed.path
        else:
            raise V.Invalid("Unknown file scheme %s" % parsed.scheme)

        if self.scheme == 'file' and '|' in self.pattern:
            raise V.Invalid("OR (|) operators are not supported in file patterns", path=[ 'source', 'paths' ])

        if self.bucket is not None and glob2.has_magic(self.bucket):
            raise V.Invalid("Buckets (%s) cannot have pattern characters ('*', '[', ']')" % (self.bucket), path=[ 'source', 'paths' ])

        if self.bucket and not self.pattern:
            raise V.Invalid("Path '%s' specifies a bucket ('%s') but cannot match any keys" % (path, self.bucket), path=[ 'source', 'paths' ])

    def __str__(self):
        bucket_s = self.bucket.encode('utf-8') if self.bucket else ''
        pattern_s = self.pattern.encode('utf-8')
        return self.scheme + "://" + os.path.join(bucket_s, pattern_s)

DEFAULT_AWS_ACCESS_KEY = None
DEFAULT_AWS_SECRET_KEY = None

def get_spec_validator():
    _options_fields_schema = V.Schema({
        V.Required("terminated", default='\t'): basestring,
        V.Required("enclosed", default=""): basestring,
        V.Required("escaped", default="\\"): basestring
    })

    _options_lines_schema = V.Schema({
        V.Required("ignore", default=0): int,
        V.Required("starting", default=""): basestring,
        V.Required("terminated", default='\n'): basestring
    })

    _options_schema = V.Schema({
        V.Required("fields", default=_options_fields_schema({})): _options_fields_schema,
        V.Required("lines", default=_options_lines_schema({})): _options_lines_schema,
        V.Required("columns", default=[]): [basestring],
        V.Required("file_id_column", default=None): V.Any(basestring, None),
        V.Required("non_local_load", default=False): bool,
        V.Required("duplicate_key_method", default="error"): V.Any("error", "replace", "ignore"),
    })

    _db_schema = V.Schema({
        V.Required('host', default='127.0.0.1'): basestring,
        V.Required('port', default=3306): int,
        V.Required('user', default='root'): basestring,
        V.Required('password', default=''): basestring,
    })

    # Each path in paths looks something like:
    #   [s3://|file://|hdfs://][bucket/]file/pattern
    SPEC_VALIDATOR = V.Schema({
        V.Required("source"): V.Schema({
            V.Required("aws_access_key", default=DEFAULT_AWS_ACCESS_KEY): V.Any(basestring, None),
            V.Required("aws_secret_key", default=DEFAULT_AWS_SECRET_KEY): V.Any(basestring, None),
            V.Required("hdfs_host", default=None): V.Any(basestring, None),
            V.Required("webhdfs_port", default=50070): V.Any(int, None),
            V.Required("hdfs_user", default=None): V.Any(basestring, None),
            V.Required("paths"): [basestring],
        }),
        V.Required("connection", default=_db_schema({})): _db_schema,
        V.Required("target"): V.Schema({
            V.Required("database"): basestring,
            V.Required("table"): basestring
        }, required=True),
        V.Required("options", default=_options_schema({})): _options_schema
    })

    return SPEC_VALIDATOR

def get_command_line_options(key_list):
    """ This is not the prettiest thing in the world. The idea is to
    match a schema path (like options.fields.terminated) into one of
    the command line options. The command line options are written to
    make sense as command line parameters, so they don't straightforwardly
    match the JSON spec. To account for this, we allow any suffix (for both
    the forward and reverse path). Below, we also assert that every field
    in the schema has exactly one matching command line option.

    Some examples:
        source::aws_access_key          matches --aws-access-key
        options::fields::terminated     matches --fields-terminated
        options::lines::ignore          matches --ignore-lines
    """

    ret = set()
    for l in [key_list[n:] for n in range(len(key_list))]:
        ret.add('_'.join(l))
    for l in [key_list[-n:] for n in range(len(key_list))]:
        ret.add('_'.join(reversed(l)))
    return list(ret)

COMMAND_LINE_MAPPING = {}
def set_command_line_mapping(all_keys, option_name):
    global COMMAND_LINE_MAPPING
    base = COMMAND_LINE_MAPPING
    for k_o in all_keys[:-1]:
        k = str(k_o)
        if k not in base:
            base[k] = {}
        base = base[k]
    base[str(all_keys[-1])] = option_name

def get_command_line_mapping(all_keys):
    global COMMAND_LINE_MAPPING
    base = COMMAND_LINE_MAPPING
    for k_o in all_keys[:-1]:
        k = str(k_o)
        base = base[k]
    return base[str(all_keys[-1])]

def build_spec_recursive(logger, options, base_spec, validator, parent_keys):
    ret = {}
    for key, val in validator.schema.items():
        key_s = str(key)
        full_key_path = parent_keys + [key]
        schema_path = ".".join(map(str, full_key_path))

        if isinstance(val, V.Schema):
            # Recurse on a subspec
            base_val = base_spec[key_s] if key_s in base_spec else {}
            newval = build_spec_recursive(logger, options, base_val, val, full_key_path)
        else:
            # Match it to a command line option (and assert that exactly one exists)
            cl_options = get_command_line_options(map(str, full_key_path))
            found = False
            for opt in cl_options:
                if hasattr(options, opt):
                    assert not found, "Multiple keys for path %s have options (%s)" % (schema_path, cl_options)
                    newval = getattr(options, opt)
                    set_command_line_mapping(full_key_path, opt)
                    found = True
            assert found, "No command line option for %s (%s)" % (schema_path, cl_options)

        if newval is not None:
            # this means that the user passed in an option that overrides the spec
            ret[key_s] = newval
        elif key_s in base_spec:
            ret[key_s] = base_spec[key_s]

    return ret

def build_spec(base_spec, options):
    # for each part in the base_spec, we expect one of two
    # things to be exposed in the options -> either the key name
    # itself or full-schema-path-to-keyname.
    logger = log.get_logger('Schema')
    return build_spec_recursive(logger, options, base_spec, get_spec_validator(), [])

def validate_spec(spec):
    spec = AttrDict.from_dict(get_spec_validator()(spec))

    # post validation steps go here
    assert 'file_id_column' in spec.options
    if spec.options.file_id_column is not None:
        file_id_column = spec['options']['file_id_column']
        if 'columns' not in spec['options']:
            raise V.Invalid('options.columns must be specified if file_id_column is provided', path=[ 'options', 'columns' ])
        else:
            if file_id_column in spec['options']['columns']:
                raise V.Invalid('options.columns can not contain the file_id_column, it will be filled in by MemSQL-Loader',
                    path=[ 'options', 'columns' ])
    return spec
