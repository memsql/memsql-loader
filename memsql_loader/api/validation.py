import voluptuous as V
from clark.super_enum import SuperEnum

def listor(sub_validator):
    """ Like d3.functor for lists """
    def _validate(value):
        return [sub_validator(val) for val in (value if isinstance(value, list) else [value])]
    return _validate

def validate_enum(EnumType):
    def _validate(value):
        if isinstance(value, SuperEnum.E) and value in EnumType:
            return value
        else:
            if isinstance(value, basestring) and value in EnumType:
                return EnumType[value]
            else:
                raise V.Invalid('Must be one of %s' % EnumType.elements.keys())
    return _validate
