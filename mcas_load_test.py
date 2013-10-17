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

"""Distributed load and concurrency test for memcache_collections.mcas.

We start with N memcache entries containing the value 0.  Then perform
concurrent MCAS operations (retrying as needed) on M locations at a time,
incrementing each location.  Finally sum all locations.  The total count should
equal the number of operations times M.  Contention can be controlled by
adjusting N, M, and the number MCAS operations running in parallel.

Run "deque_load_test.py manager" in one shell, then any number of
"deque_load_test.py worker" in other shells.

Currently assumes everything is run on the same machine, and that memcache
is local on the standard port.
"""

from __future__ import division

from multiprocessing import Process, JoinableQueue
from multiprocessing.managers import BaseManager
from random import Random
from uuid import uuid4
import sys
import time

from memcache_collections import mcas, mcas_get

__author__ = 'John Belmonte <jbelmonte@google.com>'

# TODO(jbelmonte): take manager address, other params from the command line
CONTROL_PORT = 50000
CONTROL_KEY = b'abc'
COMMAND_QUEUE = 'workers'
MEMCACHE_HOSTS = ['127.0.0.1:11211']
NUM_LOCATIONS_TOTAL = 100
NUM_LOCATIONS_PER_MCAS = 5
TARGET_RATE = 50  # None for unlimited
NUM_BATCHES = 20
BATCH_SIZE = 100

def GetMemcacheClient(hosts):
  import memcache
  return memcache.Client(hosts, cache_cas=True)


class QueueManager(BaseManager): pass


class ManagerProcess(Process):

  def __init__(self, q):
    self.q = q
    super(ManagerProcess, self).__init__()

  def run(self):
    # TODO(jbelmonte): launch a private memcached server for the load test
    mc = GetMemcacheClient(MEMCACHE_HOSTS)
    location_keys = [uuid4().hex for _ in range(NUM_LOCATIONS_TOTAL)]
    mc.set_multi(dict((key, 0) for key in location_keys))
    try:
      for _ in range(NUM_BATCHES):
        self.q.put({
            'servers': MEMCACHE_HOSTS,
            'location_keys': location_keys,
            'num_locations_per_mcas': NUM_LOCATIONS_PER_MCAS,
            'batch_size': BATCH_SIZE,
        })
      print 'Waiting for commands to complete...'
      self.q.join()
      print 'done.'
      locations = mc.get_multi(location_keys)
      assert len(locations) == len(location_keys)
      actual_count = sum(locations.values())
      expected_count = NUM_BATCHES * BATCH_SIZE * NUM_LOCATIONS_PER_MCAS
      assert actual_count == expected_count, 'count expected=%d, actual=%d' % (
          expected_count, actual_count)
    finally:
      mc.delete_multi(location_keys)
    # TODO(jbelmonte): how to exit the process?


def manager():
  command_queue = JoinableQueue()

  w = ManagerProcess(command_queue)
  w.start()

  QueueManager.register(COMMAND_QUEUE, callable=lambda:command_queue)
  m = QueueManager(address=('', CONTROL_PORT), authkey=CONTROL_KEY)
  s = m.get_server()
  s.serve_forever()


def worker():
  QueueManager.register(COMMAND_QUEUE)
  m = QueueManager(address=('', CONTROL_PORT), authkey=CONTROL_KEY)
  m.connect()
  command_queue = getattr(m, COMMAND_QUEUE)()
  r = Random()
  r.seed(time.time())

  while True:
    command = command_queue.get()
    mc = GetMemcacheClient(command['servers'])
    location_keys = command['location_keys']
    num_locations_per_mcas = command['num_locations_per_mcas']
    batch_size = command['batch_size']
    print('worker received command')
    batch_time = time.time()
    num_retries = 0
    for i in range(batch_size):
      op_time = time.time()
      mcas_keys = r.sample(location_keys, num_locations_per_mcas)
      while True:
        if mcas(mc, ((item, item.value + 1)
            for item in (mcas_get(mc, key) for key in mcas_keys))):
          break
        else:
          num_retries += 1
      if TARGET_RATE:
        time.sleep(max(0, 1 / TARGET_RATE - (time.time() - op_time)))
    print('done: %.1f operations per second, %s retries' % (
         batch_size/(time.time() - batch_time), num_retries))
    command_queue.task_done()


def main():
  command = sys.argv[1]
  if command == 'manager':
    manager()
  elif command == 'worker':
    worker()
  else:
    assert False, 'unknown command: ' + command


if __name__ == '__main__':
  main()
