import curses
import sys
from datetime import datetime
from collections import OrderedDict, defaultdict

from memsql_loader.api import shared, exceptions
from memsql_loader.api.jobs import Jobs as JobsApi

from memsql_loader.util.attr_dict import AttrDict
from memsql_loader.util.command import Command
from memsql_loader.util.pretty_printer import PrettyPrinter, TableFormat
from memsql_loader.util import log

from memsql_loader.loader_db.tasks import Tasks

class Processes(Command):
    # OrderedDict because we want the columns in this order
    TASKS_KEY_FN = OrderedDict([
        ('task_id', lambda row, for_display=False: row.id),
        ('job_id', lambda row, for_display=False: row.job_id),
        ('file', lambda row, for_display=False: row.data['key_name']),
        ('progress', lambda row, for_display=False: row.bytes_downloaded or -1),
        ('rate', lambda row, for_display=False: row.download_rate or -1),
        ('time_left', lambda row, for_display=False: row.data.get('time_left', sys.maxint)),
        ('last_contact', lambda row, for_display=False: row.last_contact)
    ])

    JOBS_KEY_FN = OrderedDict([
        ('job_id', lambda row, for_display=False: row.id),
        ('tasks', lambda row, for_display=False: row.tasks_finished),
        ('progress', lambda row, for_display=False: row.bytes_downloaded or -1),
        ('rate', lambda row, for_display=False: row.download_rate or -1),
        ('time_left', lambda row, for_display=False: row.data.get('time_left', sys.maxint)),
        ('last_contact', lambda row, for_display=False: row.last_contact or (None if for_display else datetime.min))
    ])

    @classmethod
    def configure(cls, parser, subparsers):
        subparser = subparsers.add_parser('ps', help='Show information about currently running tasks or jobs')
        subparser.set_defaults(command=Processes)

        subparser.add_argument('-o', '--order',
            choices=shared.SortDirection.elements.keys(),
            default='DESC',
            type=lambda s: s.upper(),
            help='The order to display the results in')

        subparser.add_argument('-b', '--order-by', dest='order_by',
            default=None,
            help='The column to sort the results by')

        subparser.add_argument('--jobs', action='store_true',
            help='Display information about currently running jobs instead of tasks')

        subparser.add_argument('--watch', action='store_true',
            help='Auto update every second')

    def run(self):
        self.logger = log.get_logger('Processes')
        self.error = False
        self.KEY_FN = self.JOBS_KEY_FN if self.options.jobs else self.TASKS_KEY_FN

        if self.options.watch:
            # Takes care of setup and tear-down
            curses.wrapper(self.main_cli)
        else:
            print self.get_ps_str()
            if self.error:
                sys.exit(1)

    def main_cli(self, stdscr):
        # Block each getch() for 10 tenths of a second
        curses.halfdelay(10)
        # Visibility 0 is invisible
        curses.curs_set(0)

        try:
            while True:
                ps_str = self.get_ps_str()
                lines = ps_str.split('\n')

                max_y, max_x = stdscr.getmaxyx()
                stdscr.erase()
                for i, line in enumerate(lines):
                    # We don't want to draw on the last line because the
                    # Press q to exit message goes there
                    if i >= max_y - 1:
                        break
                    stdscr.addstr(i, 0, line[:max_x])
                # Assumes that terminal size is greater than 15 character
                # Will crash otherwise...but who uses terminals 15 characters wide?
                stdscr.addstr(max_y - 1, 0, 'Press q to exit', curses.A_REVERSE)
                stdscr.refresh()

                if stdscr.getch() == ord('q'):
                    break
        except KeyboardInterrupt:
            pass

    def get_ps_str(self):
        if self.options.jobs:
            try:
                active_rows = JobsApi().query({
                    'state': [ shared.JobState.QUEUED, shared.JobState.RUNNING ],
                })
            except exceptions.ApiException as e:
                self.error = True
                return e.message

            # We want this row to have the same format as the one from Tasks
            for row in active_rows:
                row['data'] = { k: v for k, v in {
                    'time_left': row.time_left
                }.iteritems() if v is not None }
        else:
            active_rows = Tasks().get_tasks_in_state([ shared.TaskState.RUNNING ])

        if len(active_rows) == 0:
            self.error = True
            return 'No currently running %s' % ('jobs' if self.options.jobs else 'tasks')

        # Sorting the columns
        if self.options.order_by is not None:
            self._sort(active_rows)

        # Calculate the maximum number of digits for the tasks column
        if self.options.jobs:
            total_tasks = 0
            for row in active_rows:
                total_tasks += row.tasks_total
            self.max_tasks_digits = len(str(total_tasks))

        formatted_tasks = []
        for row in active_rows:
            formatted_row = {}
            for key, fn in self.KEY_FN.iteritems():
                formatted_row[key] = fn(row, for_display=True)

            # The tasks column for jobs requires special formatting
            if self.options.jobs:
                formatted_row.update(self._format_tasks_col(row))

            formatted_row.update(self._make_progress(row, 50))
            formatted_tasks.append(formatted_row)

        if len(active_rows) > 1:
            formatted_tasks.append(self._create_formatted_totals_row(active_rows))

        return PrettyPrinter(
            formatted_tasks,
            columns=self.KEY_FN.keys(),
            format=TableFormat.TABLE
        ).format()

    def _sort(self, active_rows):
        if self.options.order_by not in self.KEY_FN.keys():
            print 'Invalid column to sort by'
            sys.exit(1)
        sort_dir = shared.SortDirection[self.options.order]
        active_rows.sort(
            key=self.KEY_FN[self.options.order_by],
            reverse=(sort_dir == shared.SortDirection.DESC)
        )

    def _format_tasks_col(self, row):
        TASKS_FORMAT_STR = "{0:>{2}d}/{1:<{2}d} finished"
        return {
            'tasks': TASKS_FORMAT_STR.format(row.tasks_finished, row.tasks_total, self.max_tasks_digits)
        }

    def _create_formatted_totals_row(self, active_rows):
        totals_row = AttrDict({
            'tasks_finished': 0,
            'tasks_total': 0,
            'bytes_downloaded': 0,
            'bytes_total': 0,
            'download_rate': 0,
            'data': defaultdict(lambda: 0, { 'time_left': -1 })
        })
        for row in active_rows:
            if self.options.jobs:
                totals_row['tasks_finished'] += row.tasks_finished
                totals_row['tasks_total'] += row.tasks_total
            totals_row['bytes_downloaded'] += row.bytes_downloaded or 0
            totals_row['bytes_total'] += row.bytes_total or 0
            totals_row['download_rate'] += row.download_rate or 0
            totals_row.data['time_left'] = max(row.data.get('time_left', -1), totals_row.data['time_left'])

        formatted_totals_row = defaultdict(lambda: '')

        first_col = self.KEY_FN.keys()[0]
        formatted_totals_row[first_col] = 'Total'
        if self.options.jobs:
            formatted_totals_row.update(self._format_tasks_col(totals_row))
        formatted_totals_row.update(self._make_progress(totals_row, 50))

        return formatted_totals_row

    def _make_progress(self, row, width):
        # formatted percent has a max length of 4 (100%)
        # _format_filesize can return at most a string of length 10 (1,024.0 KB)
        # _format_time can return at most a string of length 8 (23 hours)
        NO_PROGRESS_FORMAT_STR = "{:<4} {:>10}/{:<10}"
        PROGRESS_FORMAT_STR = "{:<4} {} {:>10}/{:<10}"
        RATE_FORMAT_STR = "{:>10}/s"
        TIME_LEFT_FORMAT_STR = "{:>13}"

        try:
            current = row.bytes_downloaded or 0
            total = row.bytes_total or 0
            rate = row.download_rate or 0
            time_left = row.data.get('time_left', -1)

            percent = 0 if total == 0 else current * 1.0 / total
            formatted = [
                "%d%%" % int(percent * 100),
                self._format_filesize(current),
                self._format_filesize(total)
            ]
            formatted_rate = self._format_filesize(rate)
            formatted_time_left = self._format_time(time_left)
        except KeyError:
            percent = 0
            formatted = [ '0%', '--', '--' ]
            formatted_rate = '--'
            formatted_time_left = '--'

        string_without_progress = NO_PROGRESS_FORMAT_STR.format(*formatted)

        progress_width = width - len(string_without_progress)
        # Bar is surrounded by brackets [] and an extra space
        bar_width = progress_width - 3

        if bar_width <= 0:
            return string_without_progress

        filled_bar_width = int(bar_width * percent)
        bar = '=' * filled_bar_width
        if not filled_bar_width == bar_width and not percent == 0:
            bar += '>'

        formatted_bar = "[{0:{1}}]".format(bar, bar_width)
        formatted.insert(1, formatted_bar)

        return {
            'progress': PROGRESS_FORMAT_STR.format(*formatted),
            'rate': RATE_FORMAT_STR.format(formatted_rate),
            'time_left': TIME_LEFT_FORMAT_STR.format(formatted_time_left)
        }

    def _format_filesize(self, num):
        for x in [ 'B', 'KB', 'MB', 'GB', 'TB' ]:
            if num < 1024.0:
                return "{:,.1f} {}".format(num, x)
            num /= 1024.0

        return "{:,.1f} {}".format(num, 'PB')

    def _format_time(self, seconds):
        suffixes = [ 'sec', 'min', 'hour', 'day' ]
        conversion_rate = [ 60, 60, 24 ]

        if seconds == -1:
            return 'estimating...'

        time = seconds

        for suffix, rate in zip(suffixes, conversion_rate):
            if time < rate:
                plural = round(time) != 1.0
                return "%2.f %s%s left" % (time, suffix, 's' if plural else '')
            time /= rate

        plural = round(time) != 1.0
        return "%2.f %s%s left" % (time, suffixes[-1], 's' if plural else '')
