import re

from memsql_loader.api.exceptions import ApiException

from memsql_loader.api.jobs import Jobs
from memsql_loader.api.job import Job
from memsql_loader.api.tasks import Tasks
from memsql_loader.api.task import Task
APIS = [ Jobs, Job, Tasks, Task ]

__underscorer1 = re.compile(r'(.)([A-Z][a-z]+)')
__underscorer2 = re.compile('([a-z0-9])([A-Z])')

def __camelcase_convertor(s):
    subbed = __underscorer1.sub(r'\1_\2', s)
    return __underscorer2.sub(r'\1_\2', subbed).lower()

API_NAME_MAP = { __camelcase_convertor(ApiClass.__name__): ApiClass for ApiClass in APIS }

class ApiCaller(object):
    """ This class allows you to query any of the API's by name
    """

    def __init__(self):
        for name, ApiClass in API_NAME_MAP.items():
            setattr(self, name, ApiClass())

    def call(self, api, params):
        return self.get(api).query(params)

    def get(self, api):
        if not hasattr(self, api):
            raise ApiException('Api not found: %s' % api)
        return getattr(self, api)
