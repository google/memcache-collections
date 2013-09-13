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

"""Double-ended queue implemented on memcache."""

__author__ = 'John Belmonte <jbelmonte@google.com>'

import copy
import threading
from cPickle import dumps
from cPickle import loads
from uuid import uuid4

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
  """Ecapsulates all memcache access."""

  def __init__(self, memcache_client):
    self.mc = memcache_client

  def SaveNode(self, node):
    # TODO(jbelmonte): Use protocol buffers for serialization so that
    # schema is language independant.
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
    if result and update_on_success:
      node.__dict__.update(attributes)
    return result


class _Status(object):
  STABLE = 'STABLE'
  R_PUSH = 'R_PUSH'
  L_PUSH = 'L_PUSH'


# TODO(jbelmonte): move to memcache_collections.deque
class MemcacheDeque(object):
  """Lock-free deque on memcache.

  Each node of the queue is held in a separate memcache entry.  Nodes are
  deleted upon pop (as opposed to being re-used).  All operations
  on the queue are serialized (i.e. head/tail access is not disjoint).

  This interface is modeled after collections.deque.  Only append and pop
  operations are supported (no length, iteration, clear, etc.).

  Based on "CAS-Based Lock-Free Algorithm for Shared Deques", Maged M. Michael,
  2003, http://www.cs.bgu.ac.il/~mpam092/wiki.files/michael-dequeues.pdf.

  Synopsis:

    >>> import memcache
    >>> from memcache_deque import MemcacheDeque
    >>> mc = memcache.Client(['127.0.0.1:11211'], cache_cas=True)
    >>> deque = MemcacheDeque.create(mc, 'my_deque')
    >>> deque.appendleft(5)
    >>> deque.appendleft('hello')

  then from some other process or machine:

    >>> deque = MemcacheDeque.bind(mc, 'my_deque')
    >>> deque.pop()
    5
    >>> deque.pop()
    'hello'
    >>> deque.pop()
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
