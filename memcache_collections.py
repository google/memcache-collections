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

"""Concurrent, distributed data structures on memcache."""

import copy
import threading
from cPickle import dumps
from cPickle import loads
from uuid import uuid4

__author__ = 'John Belmonte <jbelmonte@google.com>'
__all__ = ['deque', 'mcas', 'mcas_get',
    'Error', 'NodeNotFoundError', 'SetError']


class Error(Exception):
  """Base class for exceptions in this module."""
  pass


class NodeNotFoundError(Error):
  """Node not found in storage-- indicates a corrupted collection."""
  pass


class SetError(Error):
  """Memcache set operation failed-- indicates server unavailable (or full, if
  evicitons are disabled)."""
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
    # TODO(jbelmonte): Use protocol buffers or JSON for serialization so that
    # schema is language independent.
    if not self.mc.set(node.uuid, dumps(node)):
      raise SetError

  def DeleteNode(self, node):
    self.mc.delete(node.uuid)

  def LoadNodeFromUuid(self, uuid):
    value = self.mc.gets(uuid)
    if value is None:
      raise NodeNotFoundError
    node = loads(value)
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
    result = self.mc.cas(node.uuid, dumps(new_node))
    # TODO(jbelmonte): pluggable "CAS ID deleter" to avoid unbounded memory use
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

  The implementation assumes that memcache CAS does not suffer from the
  "A->B->A" problem (valid at least for memcached).

  Important note regarding CAS ID management: the Python memcache API
  unfortunately requires client implementations to hold on to CAS ID's
  indefinitely.  This is problematic for our use case since the number of
  memcache entries using CAS is unbounded.  As a consequence, deque users are
  responsible for managing CAS ID lifetime, for example by calling reset_cas()
  on the given memcache client at appropriate times, and perhaps dedicating a
  memcache client instance soley for use by our collections.  As far as this
  API is concerned, it's safe to call reset_cas() outside of any public method.

  This class is thread safe assuming that the given memcache client is.

  Based on "CAS-Based Lock-Free Algorithm for Shared Deques", Maged M. Michael,
  2003, http://www.cs.bgu.ac.il/~mpam092/wiki.files/michael-dequeues.pdf.

  Synopsis:

    >>> import memcache
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
    """Create new collection in memcache, optionally with given unique name."""
    client = _DequeClient(memcache_client)
    # TODO(jbelmonte): create _Anchor class derived from _Node w/util methods
    # TODO(jbelmonte): track deque length
    anchor = _Node(name)
    anchor.value = _Status.STABLE
    client.SaveNode(anchor)
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

    An incoherent deque will require that the penultimate node be adjusted to
    point to the last node.

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
    # TODO(jbelmonte): track success rate
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
    # TODO(jbelmonte): async delete if supported
    self.client.DeleteNode(node)
    return node.value

  def pop(self):
    """Remove and return an element from the right side of the queue.  If no
    elements are present, raises an IndexError."""
    return self._PopCommon(is_right=True)

  def popleft(self):
    """Remove and return an element from the left side of the queue.  If no
    elements are present, raises an IndexError."""
    return self._PopCommon(is_right=False)


def _last_iter(iterable):
  """Transforms given iterable to yield (item, is_last) tuples."""
  it = iter(iterable)
  last = it.next()
  for val in it:
    yield last, False
    last = val
  yield last, True


class CasIdNotFoundError(Error):
  """Performing MCAS on item not loaded for CAS."""
  def __init__(self, key):
    Error.__init__(self, 'CAS ID not found for item with key "%s"' % key)


class _McasStatus(object):
  UNDECIDED = 'UNDECIDED'
  SUCCESSFUL = 'SUCCESSFUL'
  FAILED = 'FAILED'


class _McasRecord(object):

  def __init__(self, mc, items):
    self.uuid = uuid4().hex
    self.status = _McasStatus.UNDECIDED
    # We need to record CAS ID's, because helpers won't necessarily have
    # read all the items.  The Python memcache API is problematic in that
    # it doesn't expose these ID's officially.  Whether or not and how the
    # ID's are accessed will depend on the client.  We could have users
    # supply old values, as is traditional for CAS, but then the implementation
    # would need to make extra reads, and guard against the ABA problem.
    #
    # In this record we store (key, cas_id, current_value, new_value) for each
    # item.  This is fairly constraining since all the keys and values involved
    # in the mcas operation must fit into a single memcache entry.  If that's
    # prohibitive it's feasible to store values separately.
    #
    # TODO(jbelmonte): pluggable "CAS ID extractor"
    self.items = []
    for (key, current_value, new_value) in sorted(items):
      cas_id = mc.cas_ids.get(key)
      if cas_id is None:
        raise CasIdNotFoundError(key)
      self.items.append((key, cas_id, current_value, new_value))

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
  def deref(cls, dc, value):
    """Return MCAS record if given value is a reference, else none."""
    if type(value) == str and value.startswith(cls.REF_SENTINEL):
      return dc.LoadNodeFromUuid(value[len(cls.REF_SENTINEL):])


def _explicit_cas(mc, key, value, cas_id):
    """Perform CAS given explicit unique ID."""
    # TODO(jbelmonte): pluggable "CAS ID injector"
    original_id = mc.cas_ids.get(key)
    mc.cas_ids[key] = cas_id
    result = mc.cas(key, value)
    if original_id == None:
      del mc.cas_ids[key]
    else:
      mc.cas_ids[key] = original_id
    return result


class _ReleaseMcas(Exception):
  pass


def _mcas_help(dc, mcas_record):
  result_status = _McasStatus.SUCCESSFUL
  mcas_ref = mcas_record.make_ref()
  try:
    if mcas_record.status != _McasStatus.UNDECIDED:
      raise _ReleaseMcas  # MCAS already failed or succeeded
    # phase 1: change all locations to reference MCAS record
    for key, cas_id, _, _ in mcas_record.items:
      while True:
        # Note unlike original algorithm, we don't need conditional CAS since
        # memcached doesn't have an ABA issue.
        if _explicit_cas(dc.mc, key, mcas_ref, cas_id):
          break  # next location
        value = dc.mc.get(key)
        if value == mcas_ref:
          break  # someone else succeeded with this location
        else:
          # TODO(jbelmonte): handle MCAS record gone
          nested_mcas_record = _McasRecord.deref(dc, value)
          if nested_mcas_record:
            _mcas_help(dc, nested_mcas_record)
            # TODO(jbelmonte): Confirm it's possible to succeed from here--
            # I suspect not since the nested MCAS would void our CAS ID.
          else:
            raise _ReleaseMcas
  except _ReleaseMcas:
    # From our local view we failed, but note it's possible the MCAS record has
    # already been marked as successful, in which case we'll fail the CAS below.
    result_status = _McasStatus.FAILED
  # phase 2: revert locations or roll them forward to new values
  dc.Cas(mcas_record, {'status': result_status}, update_on_success=True)
  is_success = mcas_record.status == _McasStatus.SUCCESSFUL
  # TODO(jbelmonte): Elide gets by noting when they were already invoked by
  # first phase.
  # TODO(jbelmonte): Use batch get where supported with CAS (App Engine).
  last_cas_result = False
  for (key, _, current_value, new_value), is_last in \
      _last_iter(mcas_record.items):
    value = dc.mc.gets(key)
    if value == mcas_ref:
      if (dc.mc.cas(key, new_value if is_success else current_value)
          and is_last):
        # client successfully releasing the last node is responsible for cleanup
        dc.DeleteNode(mcas_record)
  return is_success


# challenge: say "memcache mcas" quickly five times
# TODO(jbelmonte): support explicit CAS ID's
# TODO(jbelmonte): support mcas across multiple clients
# TODO(jbelmonte): translate NodeNotFoundError
def mcas(mc, items):
  """Multi-item compare-and-set.

  The items must have already been read for CAS via gets().

  Synopsis:
    >>> import memcache
    >>> from memcache_collections import mcas
    >>> mc = memcache.Client(['127.0.0.1:11211'], cache_cas=True)
    >>> mc.set_multi({
    ...     'foo': {'next': 'bar'},
    ...     'bar': {'prev': 'foo'}})
    []
    >>> # always use mcas_get to access items potentially in MCAS operations
    >>> foo, bar = mcas_get(mc, 'foo'), mcas_get(mc, 'bar')
    >>> # atomically insert new node in our doubly linked list via MCAS
    >>> mc.set('baz', {'prev': 'foo', 'next': 'bar'})
    True
    >>> mcas(mc, [
    ...     ('foo', foo, {'next': 'baz'}),
    ...     ('bar', bar, {'prev': 'baz'})])
    True

  Based on "Practical lock-freedom", Keir Fraser, 2004, pp. 30-34.

  Args:
    mc: memcache client
    items: iterable of (key, current_value, new_value) tuples.  Each item must
      have been already loaded into the client via gets(), with current_value
      corresponding to that loaded version.

  Returns: True if MCAS completed successfully.

  Raises:
    CasIdNotFoundError: if one of the given items was not loaded for CAS
  """
  dc = _DequeClient(mc)
  mcas_record = _McasRecord(mc, items)
  dc.SaveNode(mcas_record)
  return _mcas_help(dc, mcas_record)


def mcas_get(mc, key):
  """Safely read a memcache entry which may be involved in MCAS operations.

  Since gets() is used internally, the item can then be used subsequently with
  cas() or mcas().

  Args:
    mc: memcache client
    key: memcache item key

  Returns: value if memcache item exists, else None
  """
  dc = _DequeClient(mc)
  while True:
    value = mc.gets(key)
    # TODO(jbelmonte): handle MCAS record gone
    mcas_record = _McasRecord.deref(dc, value)
    if mcas_record:
      _mcas_help(dc, mcas_record)
    else:
      return value
