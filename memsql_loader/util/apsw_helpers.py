import apsw

class _NoDefault(object):
    pass

class _RowBase(object):
    pass

class Row(_RowBase):
    """ Encapsulates a value tuple and column dictionary from APSW.

    Provides very fast access to values/column names via attribute and item
    lookup.

    It is recommended to use the query and get methods in this module to create
    Row objects.
    """

    def __init__(self, fields, values):
        """ Row() constructs a new Row object from a field and value tuple.

        :param fields: The field names for this Row.
        :param values: The field values for this Row.
        """
        super(Row, self).__setattr__("_fields", fields)
        super(Row, self).__setattr__("_values", values)

    def get(self, name, default=_NoDefault):
        """ Retrieve the value of the specified column.

        :param name: The name of the column
        :param default: An optional default value to return if the column doesn't exist.
        """
        try:
            return self._values[self._fields.index(name)]
        except (ValueError, IndexError):
            if default == _NoDefault:
                raise KeyError(name)
            else:
                return default

    def set(self, name, value):
        """ Set the value of the specified column.

        :param name: The name of the column
        """
        try:
            if isinstance(self._values, tuple):
                self._values = list(self._values)
            self._values[self._fields.index(name)] = value
        except (ValueError, IndexError):
            self._fields += (name,)
            self._values += (value,)

    def __getattr__(self, name):
        """ Rows support looking up a column value by attribute access.

        Usage::

            row = get(cursor, "select * from foo limit 1")
            assert row.bar == "baz"
        """
        try:
            return self.get(name)
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        """ Rows support setting a column value by attribute access.

        Usage::

            row = get(cursor, "select * from foo limit 1")
            row.bar = "boing"
        """
        if hasattr(self, name) and name not in self._fields:
            super(Row, self).__setattr__(name, value)
        else:
            self.set(name, value)

    def __getitem__(self, name):
        """ Rows support looking up a column value by [] access.

        Usage::

            row = get(cursor, "select * from foo limit 1")
            row["bar"] = "boing"
        """
        return self.get(name)

    def __setitem__(self, name, value):
        """ Rows support setting a column value by [] access.

        Usage::

            row = get(cursor, "select * from foo limit 1")
            row["bar"] = "boing"
        """
        self.set(name, value)

    def __contains__(self, name):
        """ Check to see if the row contains the specified column.

        :param name: The name of the column to check.
        """
        return name in self._fields

    has_key = __contains__

    def __iter__(self):
        return self._fields.__iter__()

    def __len__(self):
        return self._fields.__len__()

    def keys(self):
        """ Returns a generator that yields keys. """
        for field in iter(self._fields):
            yield field

    def values(self):
        """ Returns a generator that yields values. """
        for value in iter(self._values):
            yield value

    def items(self):
        """ Returns a generator that yields (key, value) pairs. """
        for key_value in zip(self._fields, self._values):
            yield key_value

    def __eq__(self, other):
        if isinstance(other, Row):
            return dict.__eq__(dict(self.items()), dict(other.items()))
        else:
            return dict.__eq__(dict(self.items()), other)

    def __ne__(self, other):
        return not self == other

    def as_dict(self):
        """ Turn the row into a plain `dict`. """
        return dict(self.items())

    def for_json(self):
        """ Row's serialize into JSON dictionaries. """
        return self.as_dict()

    def nope(self, *args, **kwargs):
        """ Not supported by Row objects """
        raise NotImplementedError('This object is partially immutable. To get a dictionary, call "foo = foo.as_dict()" first.')

    update = nope
    pop = nope
    setdefault = nope
    fromkeys = nope
    clear = nope
    __delitem__ = nope
    __reversed__ = nope

class SelectResult(list):
    """ A SelectResult encapsulates a APSW field list and a set of value tuples from a query.

    It is very efficient since it doesn't copy the underlying tuples at all.  It
    reads the provided row iterator (usually a cursor) and builds a single list
    of refs to the original tuples.  A single field list is shared by all rows.
    """

    def __init__(self, fields, rows, is_rows=False, RowClass=Row):
        """ Construct a SelectResult given a field list and a value tuple iterator.

        :param fields: A tuple of the field names
        :param rows: An iterator of value tuples or Rows
        :param is_rows: Are the "Rows" in the rows iterator already Row objects?
        """
        self.fields = fields
        self.RowClass = RowClass
        if not is_rows:
            super(SelectResult, self).__init__(self.RowClass(fields, row) for row in rows)
        else:
            super(SelectResult, self).__init__(rows)

    def width(self):
        """ Return the number of columns in this select result. """
        return len(self.fields)

    def __getitem__(self, i):
        """ Retrieve a row by index or a slice of rows. """
        if isinstance(i, slice):
            return SelectResult(self.fields, super(SelectResult, self).__getitem__(i), is_rows=True, RowClass=self.RowClass)
        return super(SelectResult, self).__getitem__(i)

def query(cursor, query, *params, **kwparams):
    """ Run a query on the cursor, and return a SelectResult.

    In the case where there are no rows returned, the SelectResult will not have any fields.

    Optional kwparams:
        :param RowClass: Return a SelectResult using the specified RowClass (must subclass Row).

    :param cursor: An APSW connection cursor
    :param query: The query to execute. Can contain ?s for positional escapes or
        :params for dictionary escapes.
    :param params: Positional params to escape into the query.
    :param kwparams: Dictionary params to escape into the query and additional query() options.

    Usage::

        rows = query(cursor, "select * from foo where bar=:baz", { "baz": "test" })
        assert rows[0] == { "baz": "test" }
    """
    RowClass = kwparams.pop("RowClass", Row)
    debug = kwparams.pop("debug", False)

    if len(params) and len(kwparams):
        raise apsw.Error("Only specify positional or dictionary params, not both")

    params = params if len(params) else kwparams

    if debug:
        print(query, params)

    cursor.execute(query, params)

    try:
        description = cursor.getdescription()
    except apsw.ExecutionCompleteError:
        return SelectResult((), [], RowClass=RowClass)
    else:
        return SelectResult(tuple(f[0] for f in description), cursor, RowClass=RowClass)

def get(cursor, query, *params, **kwparams):
    """ Run a query on the cursor, and return the first row as a Row or None.

    Optional kwparams:
        :param RowClass: Return a SelectResult using the specified RowClass (must subclass Row).

    :param cursor: An APSW connection cursor
    :param query: The query to execute. Can contain ?s for positional escapes or
        :params for dictionary escapes.
    :param params: Positional params to escape into the query.
    :param kwparams: Dictionary params to escape into the query and additional get() options.

    Usage::

        row = get(cursor, "select a from foo where a=? limit 1", "asdf")
        assert row.a == "asdf"

        row = get(cursor, "select a from foo where a=:bar limit 1", bar="baz")
        assert row.a == "baz"
    """
    RowClass = kwparams.pop("RowClass", Row)

    if len(params) and len(kwparams):
        raise apsw.Error("Only specify positional or dictionary params, not both")

    params = params if len(params) else kwparams
    cursor.execute(query, params)

    try:
        description = cursor.getdescription()
    except apsw.ExecutionCompleteError:
        return None

    rows = cursor.fetchall()

    if len(rows) == 1:
        return RowClass(tuple(f[0] for f in description), rows[0])
    elif len(rows) == 0:
        return None
    else:
        raise apsw.Error("More than one more returned from query")
