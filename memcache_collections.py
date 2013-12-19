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

"""Concurrent, distributed data structures on memcache.

Main items in this module:

   deque:  a lock-free, double-ended queue
   mcas, mcas_get:  lock-free, multi-entry compare and set
"""

import copy
import threading
from uuid import uuid4

__author__ = 'John Belmonte <john@neggie.net>'
__all__ = ['deque', 'mcas', 'mcas_get', 'Entry',
    'Error', 'NodeNotFoundError', 'SetError']


class Error(Exception):
  """Base class for exceptions in this module."""
  pass


class NodeNotFoundError(Error):
  """Node not found in storage-- indicates a corrupted collection."""
  pass


class SetError(Error):
  """Memcache set operation failed-- indicates server unavailable (or full,
  if evicitons are disabled)."""
  pass

class AddError(Error):
  """Memcache add operation failed-- indicates server unavailable (or full,
  if evicitons are disabled), or a race creating an entry."""
  pass


class _Node(object):

  def __init__(self, uuid=None):
    self.uuid = uuid or uuid4().hex
    self.value = None
    self.next_uuid = None
    self.prev_uuid = None

  def __getstate__(self):
    # No need to store UUID as it's the key.  Unpickling party must restore it.
    state = self.__dict__.copy()
    del state['uuid']
    return state

  def __eq__(self, other):
    return self.__dict__ == other.__dict__

  def __ne__(self, other):
    return self.__dict__ != other.__dict__


class _DequeClient:
  """Encapsulates all memcache access."""

  def __init__(self, memcache_client):
    self.mc = memcache_client

  def SaveNode(self, node):
    # TODO: Use protocol buffers or JSON for serialization so that
    # schema is language independent.
    if not self.mc.set(node.uuid, node):
      raise SetError

  def AddNode(self, node):
    if not self.mc.add(node.uuid, node):
      raise AddError

  def DeleteNode(self, node):
    self.mc.delete(node.uuid)

  def LoadNodeFromUuid(self, uuid):
    node = self.mc.gets(uuid)
    if node is None:
      raise NodeNotFoundError
    node.uuid = uuid
    return node

  def Cas(self, node, attributes, update_on_success=False):
    """Write node to memcache using CAS.

    Args:
      node: A _Node instance.
      attributes: A dictionary of node attributes to update.
      update_on_success: True if attribute changes should be applied to input
          node on successful CAS.

    Returns:
      True on successful CAS.
    """
    new_node = copy.copy(node)
    # restore UUID since it's excluded from serialization, which affects copy
    new_node.uuid = node.uuid
    new_node.__dict__.update(attributes)
    result = self.mc.cas(node.uuid, new_node)
    # TODO: pluggable "CAS ID deleter" to avoid unbounded memory use
    if result and update_on_success:
      node.__dict__.update(attributes)
    return result


class _Status(object):
  STABLE = 'STABLE'
  R_PUSH = 'R_PUSH'
  L_PUSH = 'L_PUSH'


class deque(object):
  """Lock-free deque on memcache.

  Each node of the queue is held in a separate memcache entry.  Nodes are
  deleted upon pop (as opposed to being re-used).  All operations
  on the queue are serialized (i.e. head/tail access is not disjoint).

  This interface is modeled after collections.deque.  Only append and pop
  operations are supported (no length, iteration, clear, etc.).

  Synopsis:

    >>> from memcache_collections import deque
    >>> mc = memcache.Client(['127.0.0.1:11211'], cache_cas=True)
    >>> d = deque.create(mc, 'my_deque')
    >>> d.appendleft(5)
    >>> d.appendleft('hello')

  then from some other process or machine:

    >>> d = deque.bind(mc, 'my_deque')
    >>> d.pop()
    5
    >>> d.pop()
    'hello'
    >>> d.pop()
    Traceback (most recent call last):
        ...
    IndexError

  This class is thread safe assuming that the given memcache client is.


  CAVEATS

  Low durability - the collection will be corrupted on loss of any
  underlying memcache entry (e.g. from evictions or memcache server
  restart).  The corruption state is detectable and reported by the
  client library.

  Due to the eviction issue, mixing these collections with other classes
  of data in the same memcache instance is problematic (i.e. less
  important items may corrupt the data structure).  You probably
  want to dedicate a memcache instance to hold fragile data such as these
  collections and manage it so as to not reach the full state.  The
  memcached server has an option to disable evictions (-M) which can help
  in ensuring the system is healthy.

  Regarding CAS ID management, the Python memcache API unfortunately
  requires client implementations to hold on to CAS ID's indefinitely.
  This is problematic for our use case since the number of memcache
  entries using CAS is unbounded.  As a consequence, deque users are
  responsible for managing CAS ID lifetime, for example by calling
  reset_cas() on the given memcache client at appropriate times, and
  perhaps dedicating a memcache client instance soley for use by our
  collections.  As far as this API is concerned, it's safe to call
  reset_cas() outside of any public method.


  PERFORMANCE NOTES

  Very roughly I've seen the deque reach contention limits at ~400
  combined read+write operations/s on a slow laptop and ~1,200 ops/s on
  a decent workstation given small values and the pure Python memcached
  client talking to memcached on the same machine via TCP.

  In applications where exact item ordering is not important, higher
  operation rates can be achieved by application-level sharding to
  multiple deque instances.


  IMPLEMENTATION NOTES

  Based on "CAS-Based Lock-Free Algorithm for Shared Deques",
  Maged M. Michael, 2003,
  http://www.cs.bgu.ac.il/~mpam092/wiki.files/michael-dequeues.pdf.

  This was the simplest (correct) lock-free algorithm I could find, which
  not surprisingly doesn't allow disjoint access of each queue end.
  Algorithms supporting disjoint access redily exist, such as presented
  in "Lock-Free and Practical Doubly Linked List-Based Deques Using
  Single-Word Compare-and-Swap", Sundell and Tsigas, 2005.  This would
  effectively halve the contention rate when the deque is used as a FIFO,
  but at a considerable complexity cost.

  I'm aware of simple lock-free queue and list implementations which
  attempt to employ memcache's atomic increment operation to generate
  node addresses. However they all seem to suffer from unhandled race
  conditions and it's not obvious to me there is a fix.  Increment can be
  used as a locking mechanism, however.  A locking approach may be more
  fair to clients relative to CAS over a network, but introduces its own
  complexities in needing to address client deaths, deadlocks, etc.

  The implementation assumes that memcache CAS does not suffer from the
  "A->B->A" problem (valid at least for memcached).
  """

  def __init__(self, deque_client, uuid):
    """For internal use only.  Use create() or bind()."""
    self.client = deque_client
    self.anchor_uuid = uuid
    # last_missing_node is used for detecting queue corruption.  We only need
    # to remember the most-recent failed load as the push and pop algorithms
    # will deterministically return to the same missing node if it's still
    # referenced by a valid portion of the queue.  We put this under thread-
    # local since different threads may be concurrently pushing, popping, or
    # working on opposite ends of the queue.
    self.thread_local = threading.local()
    self.thread_local.last_missing_node = None

  @classmethod
  def create(cls, memcache_client, name=None):
    """Create a new collection in memcache.

    If optional name is given, it will be used verbatim as the key for the
    root entry of the collection.

    Raises AddError if the named collection already exists.
    """
    client = _DequeClient(memcache_client)
    # TODO: create _Anchor class derived from _Node w/util methods
    # TODO: track deque length
    anchor = _Node(name)
    anchor.value = _Status.STABLE
    client.AddNode(anchor)
    return cls(client, anchor.uuid)

  @classmethod
  def bind(cls, memcache_client, name):
    """Bind to an existing collection in memcache."""
    return cls(_DequeClient(memcache_client), name)

  @property
  def name(self):
    """Returns the unique name of this queue instance."""
    return self.anchor_uuid

  def _SafeLoadNodeFromUuid(self, uuid):
    """Returns node if it exists, else None.

    Raises NodeNotFoundError if a node is missing twice consecutively, which
    implies corruption of the deque.
    """
    try:
      return self.client.LoadNodeFromUuid(uuid)
    except NodeNotFoundError, e:
      if uuid == self.thread_local.last_missing_node:
        raise e
      else:
        self.thread_local.last_missing_node = uuid

  def _Stabilize(self, anchor, last=None):
    """Make deque coherent if necessary and transition to stable state.

    An incoherent deque will require that the penultimate node be adjusted
    to point to the last node.

    We silently cede upon any detected race, as that provably implies the
    deque reached the stable state by another process.
    """
    is_right = (anchor.value == _Status.R_PUSH)
    link, reverse_link = ('next_uuid', 'prev_uuid')[::1 if is_right else -1]
    if last is None:
      last = self._SafeLoadNodeFromUuid(getattr(anchor, link))
      if last is None:
        # race: node was popped since time we loaded anchor
        return
    # N.B.: the Michael2003 algorithm now checks that the anchor hasn't changed,
    # ensuring the dereferenced "last" node is a valid starting point for the
    # following link adjustment.  (It wouldn't be if the deque transitioned
    # into the empty or single item states in the meantime.)  For our
    # implementation the check is uneccessary since we have an isolated,
    # self-consistent copy of the anchor record.
    penultimate = self._SafeLoadNodeFromUuid(getattr(last, reverse_link))
    if penultimate is None:
      # race: both last and penultimate nodes popped since time we loaded former
      return
    if getattr(penultimate, link) != last.uuid:
      # Ensure our dereferenced "last" node still had that position at the time
      # of the comparison just made.  Otherwise the node may have already been
      # popped and the following CAS would corrupt the queue.
      if self.client.LoadNodeFromUuid(anchor.uuid) != anchor:
        return
      if not self.client.Cas(penultimate, {link: last.uuid}):
        return
    self.client.Cas(anchor, {'value': _Status.STABLE})

  def _PushCommon(self, value, target_status):
    node = _Node()
    node.value = value
    # TODO: track success rate
    while True:
      anchor = self.client.LoadNodeFromUuid(self.anchor_uuid)
      if anchor.next_uuid is None:  # empty queue
        self.client.SaveNode(node)
        if self.client.Cas(anchor,
                           {'prev_uuid': node.uuid, 'next_uuid': node.uuid}):
          break
      elif anchor.value == _Status.STABLE:
        node.prev_uuid = anchor.next_uuid  # used only in R_PUSH case
        node.next_uuid = anchor.prev_uuid  # used only in L_PUSH case
        self.client.SaveNode(node)
        link = 'next_uuid' if target_status == _Status.R_PUSH else 'prev_uuid'
        if self.client.Cas(anchor,
                           {link: node.uuid, 'value': target_status},
                           update_on_success=True):
          self._Stabilize(anchor, node)
          break
      else:
        self._Stabilize(anchor)

  def append(self, value):
    """Add value to the right side of the queue."""
    self._PushCommon(value, _Status.R_PUSH)

  def appendleft(self, value):
    """Add value to the left side of the queue."""
    self._PushCommon(value, _Status.L_PUSH)

  def _PopCommon(self, is_right):
    link, reverse_link = ('next_uuid', 'prev_uuid')[::1 if is_right else -1]
    while True:
      anchor = self.client.LoadNodeFromUuid(self.anchor_uuid)
      node_uuid = getattr(anchor, link)
      if node_uuid is None:  # empty queue
        raise IndexError
      is_single_item = anchor.prev_uuid == anchor.next_uuid
      if not (is_single_item or anchor.value == _Status.STABLE):
        self._Stabilize(anchor)
        continue
      node = self._SafeLoadNodeFromUuid(node_uuid)
      if node is None:
        # race: node popped since time we loaded anchor
        continue
      if self.client.Cas(anchor,
          {'prev_uuid': None, 'next_uuid': None} if is_single_item
          else {link: getattr(node, reverse_link)}):
        break
    # TODO: async delete if supported
    self.client.DeleteNode(node)
    return node.value

  def pop(self):
    """Remove and return an element from the right side of the queue.  If
    no elements are present, raises an IndexError."""
    return self._PopCommon(is_right=True)

  def popleft(self):
    """Remove and return an element from the left side of the queue.  If
    no elements are present, raises an IndexError."""
    return self._PopCommon(is_right=False)


def _last_iter(iterable):
  """Transforms given iterable to yield (item, is_last) tuples."""
  it = iter(iterable)
  last = it.next()
  for val in it:
    yield last, False
    last = val
  yield last, True


class _McasStatus(object):
  UNDECIDED = 'UNDECIDED'
  SUCCESSFUL = 'SUCCESSFUL'
  FAILED = 'FAILED'


class Entry(object):
  """Represents a memcache entry at a certain point in time."""

  def __init__(self, key, value, cas_id):
    self._key = key
    self._value = value
    self._cas_id = cas_id

  @property
  def key(self):
    return self._key

  @property
  def value(self):
    return self._value

  @property
  def cas_id(self):
    return self._cas_id


class _McasRecord(object):

  def __init__(self, mc, entries):
    self.uuid = uuid4().hex
    self.status = _McasStatus.UNDECIDED
    # We need to record CAS ID's, because helpers won't necessarily have
    # read all the items.  The Python memcache API is problematic in that
    # it doesn't expose these ID's officially.  Whether or not and how the
    # ID's are accessed will depend on the client.  We could have users
    # supply old values, as is traditional for CAS, but then the implementation
    # would need to make extra reads, and guard against the ABA problem.
    #
    # In this record we store (key, value, cas_id, new_value) for each
    # item.  This is fairly constraining since all the keys and values involved
    # in the mcas operation must fit into a single memcache entry.  If that's
    # prohibitive it's feasible to store values separately.
    #
    # The MCAS algorithm depends on items being sorted by key.
    self.items = sorted((entry.key, entry.value, entry.cas_id, new_value) for
        entry, new_value in entries)

  def __getstate__(self):
    # No need to store UUID as it's the key.  Unpickling party must restore it.
    state = self.__dict__.copy()
    del state['uuid']
    return state

  def __eq__(self, other):
    return self.__dict__ == other.__dict__

  def __ne__(self, other):
    return self.__dict__ != other.__dict__

  # memcache value prefix used to identify MCAS record references
  REF_SENTINEL = 'memcache-collections-mcas-record:'

  def make_ref(self):
    """Return reference to this MCAS record to be stored in memcache."""
    return self.REF_SENTINEL + self.uuid

  @classmethod
  def deref(cls, dc, value, last_missing_value):
    """Return MCAS record if given value is a reference, else None.

    If the value is a reference, and the memcache entry for the MCAS
    record cannot be found, then last_missing_value comes into play.
    If value == last_missing_value then surface the NodeNotFoundError
    exception.  Otherwise we supress the exception and return
    is_missing = True.

    Returns: (mcas_record, is_missing)
    """
    mcas_record, is_missing = None, False
    if type(value) == str and value.startswith(cls.REF_SENTINEL):
      try:
        mcas_record = dc.LoadNodeFromUuid(value[len(cls.REF_SENTINEL):])
      except NodeNotFoundError, e:
        if (value == last_missing_value):
          raise e
        else:
          is_missing = True
    return mcas_record, is_missing


def _get_cas_ids(mc):
    """Get implementation-specific CAS ID dictionary."""
    # TODO: pluggable CAS ID access
    cas_ids = getattr(mc, 'cas_ids', None)  # python-memcached client
    if cas_ids == None:
      cas_ids = getattr(mc, '_cas_ids', None)  # App Engine
    assert cas_ids != None, 'Explicit CAS not supported for memcache client'
    return cas_ids


def _explicit_cas(mc, key, value, cas_id):
    """Perform CAS given explicit unique ID."""
    cas_ids = _get_cas_ids(mc)
    original_id = cas_ids.get(key)
    cas_ids[key] = cas_id
    result = mc.cas(key, value)
    if original_id == None:
      del cas_ids[key]
    else:
      cas_ids[key] = original_id
    return result


class _ReleaseMcas(Exception):
  pass


def _mcas_help(dc, mcas_record, is_originator=False):
  """Attempt to take given MCAS transaction to completed state.

  Args:
    dc: DequeClient helper object
    mcas_record: existing transaction record, must already be loaded for CAS
    is_originator: True if caller originated the MCAS operation

  Returns:  True if transaction reached successful state.  This status is
      returned only in the is_originator case.
  """
  result_status = None
  mcas_ref = mcas_record.make_ref()
  try:
    if mcas_record.status != _McasStatus.UNDECIDED:
      raise _ReleaseMcas  # MCAS already failed or succeeded
    # phase 1: change all locations to reference MCAS record
    for key, _, cas_id, _ in mcas_record.items:
      # Failing to dereference an MCAS record is an expected race condition,
      # but is unrecoverable if it happens on the same entry consecutively.
      last_missing_mcas_ref = None
      while True:
        # Note unlike original algorithm, we don't need conditional CAS since
        # memcached doesn't have an ABA issue.
        if _explicit_cas(dc.mc, key, mcas_ref, cas_id):
          break  # next location
        # TODO: any scenario where user needs this to be gets()?
        location = dc.mc.get(key)
        if location == mcas_ref:
          break  # someone else succeeded with this location
        else:
          nested_mcas_record, is_missing = _McasRecord.deref(dc, location,
              last_missing_mcas_ref)
          if is_missing:
            last_missing_mcas_ref = location
            continue
          elif nested_mcas_record:
            _mcas_help(dc, nested_mcas_record)
            # TODO: Confirm it's possible to succeed from here--
            # I suspect not since the nested MCAS would void our CAS ID.
          else:
            result_status = _McasStatus.FAILED
            raise _ReleaseMcas
    result_status = _McasStatus.SUCCESSFUL
  except _ReleaseMcas:
    pass
  # update MCAS record status if terminal status reached
  if result_status is not None:
    if not dc.Cas(mcas_record, {'status': result_status},
        update_on_success=True):
      # someone else set a terminal status first, so find out what it was
      try:
        mcas_record = dc.LoadNodeFromUuid(mcas_record.uuid)
      except NodeNotFoundError:
        if is_originator:
          # This shouldn't happen since only originator is allowed to delete
          # the MCAS record.
          raise
        else:
          # We're a helping client and can give up.
          return
  is_success = mcas_record.status == _McasStatus.SUCCESSFUL
  # phase 2: revert locations or roll them forward to new values
  # TODO: Elide gets by noting when they were already invoked by
  # first phase.
  # TODO: Use batch get where supported with CAS (App Engine).
  for (key, value, _, new_value), is_last in _last_iter(mcas_record.items):
    location = dc.mc.gets(key)
    if location == mcas_ref:
      if (dc.mc.cas(key, new_value if is_success else value) and is_last):
        # The client successfully releasing the last node is responsible for
        # deleting the MCAS record.  However we only allow the originating
        # client to do this to prevent the race where a helping client
        # completes phase 2 before originating client exits phase 1, and the
        # latter wouldn't know the final status.  This compromise can yield
        # uncollected MCAS records, e.g. if originating client dies.
        # TODO: consider an expiration time on MCAS records
        # TODO: record stats on how often we don't clean up
        if is_originator:
          dc.DeleteNode(mcas_record)
  if is_originator:
    return is_success


# challenge: say "memcache mcas" quickly five times
# TODO: support mcas across multiple clients
# TODO: translate NodeNotFoundError
def mcas(mc, entries):
  """Multi-entry compare-and-set.

  Synopsis:
    >>> from memcache_collections import mcas
    >>> mc = memcache.Client(['127.0.0.1:11211'], cache_cas=True)
    >>> # initialize a doubly-linked list with two elements
    >>> mc.set_multi({
    ...     'foo': {'next': 'bar'},
    ...     'bar': {'prev': 'foo'}})
    []
    >>> # Always use mcas_get to access entries potentially in MCAS
    >>> # operations.  It returns an object representing a memcache entry
    >>> # snapshot.
    >>> foo_entry, bar_entry = mcas_get(mc, 'foo'), mcas_get(mc, 'bar')
    >>> foo_entry.key, foo_entry.value
    ('foo', {'next': 'bar'})
    >>> # atomically insert new node in our doubly linked list via MCAS
    >>> mc.add('baz', {'prev': 'foo', 'next': 'bar'})
    1
    >>> mcas(mc, [
    ...     (foo_entry, {'next': 'baz'}),
    ...     (bar_entry, {'prev': 'baz'})])
    True

  Function is not thread safe due to implicit CAS ID handling of the
  Python API.

  Args:
    mc: memcache client
    entries: iterable of (Entry, new_value) tuples

  Returns: True if MCAS completed successfully.

  The aggregate size of current and new values for all entries must fit
  within the memcache value limit (typically 1 MB).

  Based on "Practical lock-freedom", Keir Fraser, 2004, pp. 30-34.
  """
  dc = _DequeClient(mc)
  mcas_record = _McasRecord(mc, entries)
  dc.AddNode(mcas_record)
  # very sad that we need to read this back just to get CAS ID
  dc.mc.gets(mcas_record.uuid)
  return _mcas_help(dc, mcas_record, is_originator=True)


# TODO: mcas_get_multi
def mcas_get(mc, key):
  """Safely read a memcache entry which may be involved in MCAS operations.

  Function is not thread safe due to implicit CAS ID handling of the
  Python API.

  Args:
    mc: memcache client
    key: memcache entry key

  Returns: Entry object if memcache entry exists, else None
  """
  dc = _DequeClient(mc)
  # Failing to dereference an MCAS record is an expected race condition,
  # but is unrecoverable if it happens on the same entry consecutively.
  last_missing_mcas_ref = None
  while True:
    value = mc.gets(key)
    cas_id = None if value is None else _get_cas_ids(mc)[key]
    mcas_record, is_missing = _McasRecord.deref(dc, value,
        last_missing_mcas_ref)
    if is_missing:
      last_missing_mcas_ref = value
      continue
    elif mcas_record:
      _mcas_help(dc, mcas_record)
    else:
      return Entry(key, value, cas_id)


def test():
  import doctest
  import mockcache
  doctest.testmod(extraglobs={'memcache': mockcache})


if __name__ == '__main__':
  test()
