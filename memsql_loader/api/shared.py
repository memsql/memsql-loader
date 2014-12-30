import re
from datetime import datetime
from dateutil import parser

from clark.super_enum import SuperEnum
from memsql_loader.util import super_json as json
from memsql_loader.util.apsw_sql_step_queue.time_helpers import unix_timestamp

TASKS_TTL = 120


class TaskState(SuperEnum):
    QUEUED = SuperEnum.E
    RUNNING = SuperEnum.E
    SUCCESS = SuperEnum.E
    ERROR = SuperEnum.E
    CANCELLED = SuperEnum.E

    SUCCESS_CONDITION = 'tasks.result = \'success\''
    ERROR_CONDITION = 'tasks.result = \'error\''
    CANCELLED_CONDITION = 'tasks.result = \'cancelled\''
    FINISHED_CONDITION = 'tasks.finished IS NOT NULL'
    QUEUED_CONDITION = 'tasks.finished IS NULL AND (tasks.execution_id IS NULL OR tasks.last_contact <= datetime(:now, "unixepoch", "-%s second"))' % TASKS_TTL

    # The cancelled condition is not necessary here since a cancelled
    # task also counts as finished, and UPPER(tasks.result) will return
    # what we want in that case.
    PROJECTION = re.sub(r'\s+', ' ', """
        (CASE
            WHEN (%s) THEN UPPER(tasks.result)
            WHEN (%s) THEN 'QUEUED'
            ELSE 'RUNNING'
        END)
    """ % (FINISHED_CONDITION, QUEUED_CONDITION)).strip()

    @staticmethod
    def projection_params():
        return {
            'now': unix_timestamp(datetime.utcnow())
        }

class JobState(SuperEnum):
    QUEUED = SuperEnum.E
    RUNNING = SuperEnum.E
    FINISHED = SuperEnum.E
    CANCELLED = SuperEnum.E

    PROJECTION = re.sub(r'\s+', ' ', '''
        (CASE
            WHEN (
                (job_tasks.tasks_total - job_tasks.tasks_finished) = 0
                AND job_tasks.tasks_cancelled > 0) THEN 'CANCELLED'
            WHEN (
                job_tasks.tasks_total IS NULL
                OR job_tasks.tasks_finished = job_tasks.tasks_total) THEN 'FINISHED'
            WHEN (job_tasks.tasks_queued = job_tasks.tasks_total) THEN 'QUEUED'
            ELSE 'RUNNING'
        END)
    ''').strip()

class SortDirection(SuperEnum):
    DESC = SuperEnum.E
    ASC = SuperEnum.E

def task_load_row(row):
    row['data'] = json.safe_loads(row.data or '', {})

    row['steps'] = json.safe_loads(row.steps or '', [])
    for step in row.steps:
        if 'start' in step:
            step['start'] = parser.parse(step['start'])
        if 'stop' in step:
            step['stop'] = parser.parse(step['stop'])

    if 'state' in row and row.state in TaskState:
        row['state'] = TaskState[row.state]

    return row

def job_load_row(row):
    row['spec'] = json.safe_loads(row.spec or '', {})

    if 'state' in row and row.state in JobState:
        row['state'] = JobState[row.state]

    return row
