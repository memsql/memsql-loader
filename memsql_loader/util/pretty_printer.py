from prettytable import PrettyTable
from clark.super_enum import SuperEnum

from memsql_loader.api.shared import SortDirection
from memsql_loader.util import super_json as json

class TableFormat(SuperEnum):
    HTML = SuperEnum.E
    JSON = SuperEnum.E
    TABLE = SuperEnum.E

# Wrapper around PrettyTable and json.dump
class PrettyPrinter(object):
    # Assumes that the format of the data is a list of dictionaries
    # similar to AttrDict's
    def __init__(self, data, columns=None, align={}, sort_by=None, sort_dir=SortDirection.DESC, format=TableFormat.TABLE):
        if columns is not None:
            # Filter fields of rows in data by columns
            self.data = [ { key: row[key] for key in columns } for row in data ]
            self.columns = columns
        elif columns is None and len(data) > 0:
            self.data = data
            self.columns = data[0].keys()
        else:
            self.data = []
            self.columns = []

        self.align = align
        self.sort_by = sort_by
        self.reverse_sort = sort_dir == SortDirection.DESC
        self.tablefmt = format

    def format(self):
        if self.tablefmt == TableFormat.JSON:
            # TODO(cary) Patch clark.super_enum to support JSON serialization
            printable_data = [
                { k: str(v) if isinstance(v, SuperEnum.Element) else v for k, v in row.iteritems() }
                for row in self.data
            ]
            return json.dumps(printable_data, sort_keys=True, indent=4 * ' ').encode('utf-8')
        else:
            ptable = PrettyTable(self.columns)
            for k, v in self.align.iteritems():
                ptable.align[k] = v

            for row in self.data:
                ptable.add_row([ row[col] for col in self.columns ])

            if self.tablefmt == TableFormat.TABLE:
                return ptable.get_string(sortby=self.sort_by, reversesort=self.reverse_sort).encode('utf-8')
            elif self.tablefmt == TableFormat.HTML:
                return ptable.get_html_string(sortby=self.sort_by, reversesort=self.reverse_sort).encode('utf-8')
