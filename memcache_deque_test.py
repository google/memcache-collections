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

"""Unit tests for memcache_deque module."""

__author__ = 'John Belmonte <jbelmonte@google.com>'

import collections
import unittest
from memcache_deque import MemcacheDeque

IS_APP_ENGINE = False
IS_APP_ENGINE_STUB = False


def GetMemcacheClient():
  if IS_APP_ENGINE:
    from google.appengine.api import memcache
    return memcache.Client()
  elif True:
    import memcache
    # Anyone else bothered by this client having all cas calls return success
    # when CAS support is disabled (i.e. cache_cas=False, the default)!?
    return memcache.Client(['127.0.0.1:11211'], cache_cas=True)
  else:
    # pylibmc is buggy, yields:
    # "*** glibc detected *** python: double free or corruption"
    import pylibmc
    return pylibmc.Client(['127.0.0.1:11211'], behaviors={'cas': True})


class MemcacheDequeTestCase(unittest.TestCase):

  def setUp(self):
    if IS_APP_ENGINE_STUB:
      from google.appengine.ext import testbed
      self.testbed = testbed.Testbed()
      self.testbed.activate()
      self.testbed.init_memcache_stub()

  def tearDown(self):
    if IS_APP_ENGINE_STUB:
      self.testbed.deactivate()

  def _TestDeque(self, d):
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
    self._TestDeque(collections.deque())

  def testMemcacheDeque(self):
    # TODO(jbelmonte): use a mock or stub memcache client
    # TODO(jbelmonte): Write serious unit test which covers all concurrency
    # cases of the lock-free algorithm.  Current test passes even when CAS is
    # ignored...
    mc = GetMemcacheClient()
    self._TestDeque(MemcacheDeque.create(mc))

    # test create and bind
    d1 = MemcacheDeque.create(mc)
    d2 = MemcacheDeque.bind(mc, d1.name)
    d1.appendleft(5)
    self.assertEqual(5, d2.pop())
    self.failUnlessRaises(IndexError, d1.popleft)

    # test named create and bind
    name = 'foo'
    d1 = MemcacheDeque.create(mc, name)
    d2 = MemcacheDeque.bind(mc, name)
    self.assertEqual(name, d1.name)
    d1.appendleft(5)
    self.assertEqual(5, d2.pop())
    self.failUnlessRaises(IndexError, d1.popleft)


if __name__ == '__main__':
  unittest.main()
