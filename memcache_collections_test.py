# Copyright 2013 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for memcache_collections module."""

import collections
import unittest
from memcache_collections import deque, mcas, mcas_get, skiplist, AddError
from random import randrange
from uuid import uuid4

__author__ = 'John Belmonte <john@neggie.net>'


class Client:
  MOCKCACHE = 1
  PYTHON_MEMCACHED = 2
  PYLIBMC = 3
  APP_ENGINE = 4
  APP_ENGINE_MOCK = 5

# Note that tests expect a pristine, isolated memcache evironment.  Therefore
# only the mock client options are expected to pass deterministically.
_client = Client.MOCKCACHE


def GetMemcacheClient():
  if _client in (Client.APP_ENGINE, Client.APP_ENGINE_MOCK):
    from google.appengine.api import memcache
    return memcache.Client()
  elif _client == Client.PYTHON_MEMCACHED:
    import memcache
    # Anyone else bothered by this client having all cas calls return success
    # when CAS support is disabled (i.e. cache_cas=False, the default)!?
    return memcache.Client(['127.0.0.1:11211'], cache_cas=True)
  elif _client == Client.MOCKCACHE:
    import mockcache
    return mockcache.Client(['127.0.0.1:11211'], cache_cas=True)
  elif _client == Client.PYLIBMC:
    # pylibmc is buggy, yields:
    # "*** glibc detected *** python: double free or corruption"
    import pylibmc
    return pylibmc.Client(['127.0.0.1:11211'], behaviors={'cas': True})
  else:
    raise AssertionError('unknown client type')


class memcacheCollectionsTestCase(unittest.TestCase):

  def setUp(self):
    if _client == Client.APP_ENGINE_MOCK:
      from google.appengine.ext import testbed
      self.testbed = testbed.Testbed()
      self.testbed.activate()
      self.testbed.init_memcache_stub()

  def tearDown(self):
    if _client == Client.APP_ENGINE_MOCK:
      self.testbed.deactivate()

  def baseQueueTest(self, d):
    self.failUnlessRaises(IndexError, d.popleft)
    self.failUnlessRaises(IndexError, d.pop)
    d.appendleft(5)
    d.appendleft(10)
    self.assertEqual(5, d.pop())
    d.append(7)
    self.assertEqual(10, d.popleft())
    self.assertEqual(7, d.popleft())
    self.failUnlessRaises(IndexError, d.popleft)

  def testInMemoryDeque(self):
    self.baseQueueTest(collections.deque())

  def testDeque(self):
    # TODO: Write serious unit test which covers all concurrency
    # cases of the lock-free algorithm.  Current test passes even when CAS is
    # ignored...
    mc = GetMemcacheClient()
    self.baseQueueTest(deque.create(mc))

    # test create and bind
    d1 = deque.create(mc)
    d2 = deque.bind(mc, d1.name)
    d1.appendleft(5)
    self.assertEqual(5, d2.pop())
    self.failUnlessRaises(IndexError, d1.popleft)

    # test named create and bind
    name = 'foo'
    d1 = deque.create(mc, name)
    self.failUnlessRaises(AddError, deque.create, mc, name)
    d2 = deque.bind(mc, name)
    self.assertEqual(name, d1.name)
    d1.appendleft(5)
    self.assertEqual(5, d2.pop())
    self.failUnlessRaises(IndexError, d1.popleft)

  def testMcasGet(self):
    mc = GetMemcacheClient()
    key = uuid4().hex
    value = 'foo'
    self.assertTrue(mc.set(key, value))
    item = mcas_get(mc, key)
    self.assertEqual(key, item.key)
    self.assertEqual(value, item.value)
    self.assertTrue(mc.set(key, 'bar'))
    item2 = mcas_get(mc, key)
    self.assertNotEqual(item.cas_id, item2.cas_id)

  def testMcas(self):
    mc = GetMemcacheClient()
    key1, key2 = uuid4().hex, uuid4().hex
    mc.set_multi({key1: 'foo', key2: 'bar'})
    item1, item2 = mcas_get(mc, key1), mcas_get(mc, key2)
    self.assertTrue(mcas(mc, [(item1, 'foo2'), (item2, 'bar2')]))
    self.assertEqual(mc.get_multi([key1, key2]), {key1: 'foo2', key2: 'bar2'})

  def testMcasFail(self):
    mc = GetMemcacheClient()
    key1, key2 = uuid4().hex, uuid4().hex
    mc.set_multi({key1: 'foo', key2: 'bar'})
    item1, item2 = mcas_get(mc, key1), mcas_get(mc, key2)
    mc.set(key2, 'baz')
    self.assertFalse(mcas(mc, [(item1, 'foo2'), (item2, 'bar2')]))
    self.assertEqual(mc.get_multi([key1, key2]), {key1: 'foo', key2: 'baz'})

  def testSkiplist(self):
    mc = GetMemcacheClient()
    s = skiplist.create(mc)
    self.assertFalse(s.contains(5))
    s.insert(5)
    self.assertTrue(s.contains(5))
    s.insert(10)
    self.assertTrue(s.contains(10))
    for i in xrange(100):
      x = randrange(0, 2**16)
      s.insert(x)
      self.assertTrue(s.contains(x))

    # test create and bind
    s1 = skiplist.create(mc)
    s2 = skiplist.bind(mc, s1.name)
    s1.insert(5)
    self.assertTrue(s2.contains(5))

    # test named create and bind
    name = 'foo'
    s1 = skiplist.create(mc, name)
    self.failUnlessRaises(AddError, skiplist.create, mc, name)
    s2 = skiplist.bind(mc, name)
    self.assertEqual(name, s1.name)
    s1.insert(5)
    self.assertTrue(s2.contains(5))


if __name__ == '__main__':
  unittest.main()
