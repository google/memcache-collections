# Copyright (c) 2010 Lunant <http://lunant.net/>
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
"""The Python dictionary-based mock memcached client library. It does not
connect to any memcached server, but keeps a dictionary and stores every cache
into there internally. It is a just emulated API of memcached client only for
tests. It implements expiration also. NOT THREAD-SAFE.

    try:
        import memcache
    except ImportError:
        import warnings
        import mockcache as memcache
        warnings.warn("imported mockcache instead of memcache; cannot find "
                      "memcache module")

    mc = memcache.Client(["127.0.0.1:11211"])

This module and other memcached client libraries have the same behavior.

    >>> from mockcache import Client
    >>> mc = Client()
    >>> mc
    <mockcache.Client {}>
    >>> mc.get("a")
    >>> mc.get("a") is None
    True
    >>> mc.set("a", "1234")
    1
    >>> mc.get("a")
    '1234'
    >>> mc
    <mockcache.Client {'a': ('1234', None, 1)}>
    >>> mc.add("a", "1111")
    0
    >>> mc.get("a")
    '1234'
    >>> mc
    <mockcache.Client {'a': ('1234', None, 1)}>
    >>> mc.replace("a", "2222")
    1
    >>> mc.get("a")
    '2222'
    >>> mc
    <mockcache.Client {'a': ('2222', None, 2)}>
    >>> mc.append("a", "3")
    1
    >>> mc.get("a")
    '22223'
    >>> mc
    <mockcache.Client {'a': ('22223', None, 3)}>
    >>> mc.prepend("a", "1")
    1
    >>> mc.get("a")
    '122223'
    >>> mc
    <mockcache.Client {'a': ('122223', None, 4)}>
    >>> mc.incr("a")
    122224
    >>> mc.get("a")
    122224
    >>> mc
    <mockcache.Client {'a': (122224, None, 5)}>
    >>> mc.incr("a", 10)
    122234
    >>> mc.get("a")
    122234
    >>> mc
    <mockcache.Client {'a': (122234, None, 6)}>
    >>> mc.decr("a")
    122233
    >>> mc.get("a")
    122233
    >>> mc
    <mockcache.Client {'a': (122233, None, 7)}>
    >>> mc.decr("a", 5)
    122228
    >>> mc.get("a")
    122228
    >>> mc
    <mockcache.Client {'a': (122228, None, 8)}>
    >>> mc.replace("b", "value")
    0
    >>> mc.get("b")
    >>> mc.get("b") is None
    True
    >>> mc
    <mockcache.Client {'a': (122228, None, 8)}>
    >>> mc.add("b", "value", 5)
    1
    >>> mc.get("b")
    'value'
    >>> mc  # doctest: +ELLIPSIS
    <mockcache.Client {'a': (122228, None, 8), 'b': ('value', ...)}>
    >>> import time
    >>> time.sleep(6)
    >>> mc.get("b")
    >>> mc.get("b") is None
    True
    >>> mc
    <mockcache.Client {'a': (122228, None, 8)}>
    >>> mc.set("c", "foo")
    1
    >>> mc.gets("c")
    'foo'
    >>> mc.cas("c", "bar")
    1
    >>> mc.gets("c")
    'bar'
    >>> mc.set("c", "foo")
    1
    >>> mc.cas("c", "baz")
    0
    >>> mc.get_multi(["a", "b", "c"])
    {'a': 122228, 'c': 'foo'}
    >>> mc.set_multi({"a": 999, "b": 998, "c": 997}, key_prefix="pf_")
    []
    >>> mc.get("pf_a")
    999
    >>> multi_result = mc.get_multi(["b", "c"], key_prefix="pf_")
    >>> multi_result["b"]
    998
    >>> multi_result["c"]
    997
    >>> mc.delete("a")
    1
    >>> mc.get("a") is None
    True
    >>> mc.set("a b c", 123) #doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    MockcachedKeyCharacterError: Control characters not allowed
    >>> mc.set(None, 123) #doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    MockcachedKeyNoneError: Key is None
    >>> mc.set(123, 123) #doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    MockcachedKeyTypeError: Key must be str()'s
    >>> mc.set(u"a", 123) #doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    MockcachedStringEncodingError: Key must be str()'s, not unicode.
    >>> mc.set("a" * 251, 123) #doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    MockcachedKeyLengthError: Key length is > ...

"""

import datetime


__author__ = "Hong Minhee <http://dahlia.kr/>"
__maintainer__ = __author__
__email__ = "dahlia@lunant.com"
__copyright__ = "Copyright (c) 2010 Lunant <http://lunant.com/>"
__license__ = "MIT License"
__version__ = "1.0.1"


SERVER_MAX_KEY_LENGTH = 250
SERVER_MAX_VALUE_LENGTH = 1024*1024


class Client(object):
    """Dictionary-based mock memcached client. Almost like other Python
    memcached client libraries' interface.

    """

    __slots__ = "dictionary", "cas_ids"

    # exceptions for Client
    class MockcachedKeyError(Exception):
        pass
    class MockcachedKeyLengthError(MockcachedKeyError):
        pass
    class MockcachedKeyCharacterError(MockcachedKeyError):
        pass
    class MockcachedKeyNoneError(MockcachedKeyError):
        pass
    class MockcachedKeyTypeError(MockcachedKeyError):
        pass
    class MockcachedStringEncodingError(Exception):
        pass

    def __init__(self, *args, **kwargs):
        """Does nothing. It takes no or any arguments, but they are just for
        compatibility so ignored.

        """
        # map of key to (value, expiration_time, unique_id) tuple
        self.dictionary = {}
        # attribute name is intentionally compatible with python-memcached
        self.cas_ids = {}

    def set_servers(self, servers):
        """Does nothing, like `__init__`. Just for compatibility."""
        pass

    def disconnect_all(self):
        """Does nothing, like `__init__`. Just for compatibility."""
        pass

    def delete(self, key, time=0):
        """Deletes the `key` from the dictionary."""
        if key in self.dictionary:
            if int(time) < 1:
                del self.dictionary[key]
                return 1
            # support for delete lock time here is surely broken
            self.set(key, self.dictionary[key], time)
        return 0

    def incr(self, key, delta=1):
        """Increments an integer by the `key`."""
        try:
            value, exp, id = self.dictionary[key]
        except KeyError:
            return
        else:
            value = int(value) + delta
            self.dictionary[key] = value, exp, id + 1
            return value

    def decr(self, key, delta=1):
        """Decrements an integer by the `key`."""
        return self.incr(key, -delta)

    def append(self, key, val):
        """Append the `val` to the end of the existing `key`'s value.
        It works only when there is the key already.

        """
        try:
            value, exp, id = self.dictionary[key]
            self.dictionary[key] = str(value) + val, exp, id + 1
        except KeyError:
            return 0
        else:
            return 1

    def prepend(self, key, val):
        """Prepends the `val` to the beginning of the existing `key`'s value.
        It works only when there is the key already.

        """
        try:
            value, exp, id = self.dictionary[key]
            self.dictionary[key] = val + str(value), exp, id + 1
        except KeyError:
            return 0
        else:
            return 1

    def add(self, key, val, time=0):
        """Adds a new `key` with the `val`. Almost like `set` method,
        but it stores the value only when the `key` doesn't exist already.

        """
        if key in self.dictionary:
            return 0
        return self.set(key, val, time)

    def replace(self, key, val, time=0):
        """Replaces the existing `key` with `val`. Almost like `set` method,
        but it store the value only when the `key` already exists.

        """
        if key not in self.dictionary:
            return 0
        return self.set(key, val, time)

    def set(self, key, val, time=0):
        """Sets the `key` with `val`."""
        check_key(key)
        if not time:
            time = None
        elif time < 60 * 60 * 24 * 30:
            time = datetime.datetime.now() + datetime.timedelta(0, time)
        else:
            time = datetime.datetime.fromtimestamp(time)
        _, _, id = self.dictionary.get(key, (None, None, 0))
        self.dictionary[key] = val, time, id + 1
        return 1

    def set_multi(self, mapping, time=0, key_prefix=''):
        """Sets all the key-value pairs in `mapping`. If `key_prefix` is
        given, it is prepended to all keys in `mapping`."""
        for key, value in mapping.items():
            self.set('%s%s' % (key_prefix, key), value, time)
        return []

    def cas(self, key, value, time=0):
        if not (key in self.dictionary and key in self.cas_ids):
            return 0
        _, _, id = self.dictionary[key]
        if id != self.cas_ids[key]:
            return 0
        return self.set(key, value, time)

    def reset_cas(self):
        self.cas_ids.clear()

    def get(self, key):
        """Retrieves a value of the `key` from the internal dictionary."""
        check_key(key)
        try:
            val, exptime, _ = self.dictionary[key]
        except KeyError:
            return
        else:
            if exptime and exptime < datetime.datetime.now():
                del self.dictionary[key]
                return
            return val

    def gets(self, key):
        """Retrieves a value of the `key` from the internal dictionary,
        recording state for future mcas calls."""
        check_key(key)
        try:
            val, exptime, id = self.dictionary[key]
            self.cas_ids[key] = id
        except KeyError:
            return
        else:
            if exptime and exptime < datetime.datetime.now():
                del self.dictionary[key]
                return
            return val

    def get_multi(self, keys, key_prefix=''):
        """Retrieves values of the `keys` at once from the internal
        dictionary. If `key_prefix` is given, it is prepended to all
        keys before retrieving them.
        """
        dictionary = self.dictionary

        prefixed_keys = [(key, '%s%s' % (key_prefix, key)) for key in keys]
        pairs = ((key, self.dictionary[prefixed])
                  for (key, prefixed) in prefixed_keys
                  if prefixed in dictionary)
        now = datetime.datetime.now
        return dict((key, value) for key, (value, exp, _) in pairs
                                 if not exp or exp > now())

    def flush_all(self):
        """Clear the internal dictionary"""
        self.dictionary.clear()

    def __repr__(self):
        modname = "" if __name__ == "__main__" else __name__ + "."
        return "<%sClient %r>" % (modname, self.dictionary)


def check_key(key, key_extra_len=0):
    """Checks sanity of key. Fails if:
        Key length is > SERVER_MAX_KEY_LENGTH (Raises MockcachedKeyLengthError).
        Contains control characters  (Raises MockcachedKeyCharacterError).
        Is not a string (Raises MockcachedKeyTypeError)
        Is an unicode string (Raises MockcachedStringEncodingError)
        Is None (Raises MockcachedKeyNoneError)
    """
    import types
    if type(key) == types.TupleType:
        key = key[1]
    if not key:
        raise Client.MockcachedKeyNoneError, ("Key is None")
    if isinstance(key, unicode):
        msg = "Keys must be str()'s, not unicode. Convert your unicode " \
              "strings using mystring.encode(charset)!"
        raise Client.MockcachedStringEncodingError, msg
    if not isinstance(key, str):
        raise Client.MockcachedKeyTypeError, ("Key must be str()'s")

    if isinstance(key, basestring):
        if len(key) + key_extra_len > SERVER_MAX_KEY_LENGTH:
             raise Client.MockcachedKeyLengthError, ("Key length is > %s" % \
                                                     SERVER_MAX_KEY_LENGTH)
        for char in key:
            if ord(char) < 33 or ord(char) == 127:
                raise Client.MockcachedKeyCharacterError, \
                      "Control characters not allowed"
