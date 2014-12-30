class AttrDict(dict):
    def __getattr__(self, key):
        try:
            return self.__getitem__(key)
        except KeyError:
            # This lets you use dict-type attributes that aren't keys
            return getattr(super(AttrDict, self), key)

    def __setattr__(self, key, value):
        return self.__setitem__(key, value)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, dict.__repr__(self))

    @staticmethod
    def from_dict(source):
        def _transform(d):
            """ Turns a nested dict into nested AttrDict's """
            for k, v in d.iteritems():
                if isinstance(v, dict):
                    d[k] = _transform(v)
            return AttrDict(d)

        return _transform(source)
