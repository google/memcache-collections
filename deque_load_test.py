#!/usr/bin/env python2

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

"""Distributed load and concurrency test for memcache_collections.deque.

Run "deque_load_test.py manager" in one shell, then any number of
"deque_load_test.py writer" and "deque_load_test.py reader" in other shells.

Currently assumes everything is run on the same machine, and that memcache
is local on the standard port.
"""

from __future__ import division

from multiprocessing import Process, Queue
from multiprocessing.managers import BaseManager
from uuid import uuid4
import sys
import time

from memcache_collections import deque

__author__ = 'John Belmonte <john@neggie.net>'

# TODO: take manager address, other params from the command line
CONTROL_PORT =  50000
CONTROL_KEY = b'abc'
WRITER_QUEUE = 'writers'
READER_QUEUE = 'readers'
MEMCACHE_HOSTS = ['127.0.0.1:11211']
TARGET_RATE = 200  # None for unlimited
BATCH_SIZE = 1000

def GetMemcacheClient(hosts):
  if True:
    import memcache
    return memcache.Client(hosts, cache_cas=True)
  else:
    # pylibmc is buggy, yields:
    # "*** glibc detected *** python: double free or corruption"
    import pylibmc
    return pylibmc.Client(hosts, behaviors={'cas': True})


class QueueManager(BaseManager): pass


class ManagerProcess(Process):

  def __init__(self, q):
    self.q = q
    super(ManagerProcess, self).__init__()

  def run(self):
    memcache_queue_name = uuid4().hex
    # TODO: launch a private memcached server for the load test
    mc = GetMemcacheClient(MEMCACHE_HOSTS)
    deque.create(mc, memcache_queue_name)
    # TODO: graceful end of test; end-to-end checksum
    for i in range(100):
      self.q.put({
          'servers': MEMCACHE_HOSTS,
          'memcache_queue_name': memcache_queue_name,
          'batch_size': BATCH_SIZE,
          'value_size': 100,
      })


def manager():
  command_queue = Queue()
  consumer_queue = Queue()

  w = ManagerProcess(command_queue)
  w.start()

  QueueManager.register(WRITER_QUEUE, callable=lambda:command_queue)
  QueueManager.register(READER_QUEUE, callable=lambda:consumer_queue)
  m = QueueManager(address=('', CONTROL_PORT), authkey=CONTROL_KEY)
  s = m.get_server()
  s.serve_forever()


def writer():
  QueueManager.register(WRITER_QUEUE)
  QueueManager.register(READER_QUEUE)
  m = QueueManager(address=('', CONTROL_PORT), authkey=CONTROL_KEY)
  m.connect()
  command_queue = getattr(m, WRITER_QUEUE)()
  consumer_queue = getattr(m, READER_QUEUE)()

  while True:
    command = command_queue.get()
    batch_size = command['batch_size']
    mc = GetMemcacheClient(command['servers'])
    d = deque.bind(mc, command['memcache_queue_name'])
    value = 'x' * command['value_size']
    print('writer received command')
    batch_time = time.time()
    for i in range(batch_size):
      op_time = time.time()
      d.appendleft(value)
      if TARGET_RATE:
        time.sleep(max(0, 1 / TARGET_RATE - (time.time() - op_time)))
    print('done: %.1f writes per second' % (
         batch_size/(time.time() - batch_time)))
    consumer_queue.put({
        'servers': command['servers'],
        'memcache_queue_name': command['memcache_queue_name'],
        'batch_size': batch_size,
    })


def reader():
  QueueManager.register(READER_QUEUE)
  m = QueueManager(address=('', CONTROL_PORT), authkey=CONTROL_KEY)
  m.connect()
  consumer_queue = getattr(m, READER_QUEUE)()

  while True:
    command = consumer_queue.get()
    mc = GetMemcacheClient(command['servers'])
    d = deque.bind(mc, command['memcache_queue_name'])
    print('reader received command')
    batch_time = time.time()
    for i in range(command['batch_size']):
      op_time = time.time()
      d.pop()
      if TARGET_RATE:
        time.sleep(max(0, 1 / TARGET_RATE - (time.time() - op_time)))
    print('done: %.1f reads per second' % (
         command['batch_size']/(time.time() - batch_time)))


def main():
  command = sys.argv[1]
  if command == 'manager':
    manager()
  elif command == 'writer':
    writer()
  elif command == 'reader':
    reader()
  else:
    assert False, 'unknown command: ' + command


if __name__ == '__main__':
  main()
