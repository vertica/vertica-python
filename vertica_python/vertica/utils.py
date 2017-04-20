from __future__ import print_function, division, absolute_import

from six import text_type, binary_type

UTF_8 = 'utf-8'


def s2b(s, unicode_error='strict'):
    if isinstance(s, text_type):
        return s.encode(encoding=UTF_8, errors=unicode_error)
    elif isinstance(s, binary_type):
        return s
    else:
        raise TypeError('should be some kind of string')


def s2u(s, unicode_error='strict'):
    if isinstance(s, binary_type):
        return s.decode(encoding=UTF_8, errors=unicode_error)
    elif isinstance(s, text_type):
        return s
    else:
        raise TypeError('should be some kind of string')
