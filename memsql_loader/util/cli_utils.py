import re

class InvalidDatabaseException(Exception):
    pass

class AWSBucketNameInvalid(Exception):
    pass

class KeyFilterEmpty(Exception):
    pass

class MissingTable(Exception):
    pass

# https://www.debuggex.com/r/0CSrpe6QwbXre5vG
# http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
RE_VALIDATE_BUCKET_NAME = re.compile(r"^(?:[a-z0-9]+(?:[.\-]?[a-z0-9]+)*)+$")

def prompt(message, default=None, validate=None, string_escape=False):
    message = (message + " [%s] " % default) if default is not None else (message + ' ')
    while True:
        try:
            value = raw_input(message)
            if string_escape:
                value = value.decode('string-escape')
            if value == '' and default is not None:
                return default
            if validate is not None:
                value = validate(value)
            return value
        except EOFError:
            raise
        except Exception as e:
            print("%s\n" % e)

def confirm(message, default=True):
    default = 'Y/n' if default else 'y/N'

    def _check(value):
        value = value.lower()
        if value not in ('y', 'n'):
            raise Exception('Please enter either y or n')
        return value

    answer = prompt(message, default, _check)
    return answer in ('y', 'Y/n')
