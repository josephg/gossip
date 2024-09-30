import seedRandom from 'seed-random';
import assert from 'assert/strict'
import { pushSingle, versionContainsVersion, versionsDistinct, versionSubtract, versionUnion, type Version } from './versions';

type Id = number

const idGen = (() => {
  let next = 0

  return () => (next++)
})

type DocType = 'a' | 'b' | 'c'


type Message = {
  type: 'connected',
  version: Version,
} | {
  type: 'update',
  update: Update,
}

const VERBOSE = true

interface Node {
  // Globally unique peer ID.
  id: Id,
  nextSeq: () => number,

  // Versions known to this peer.
  knownVersions: Version,

  docs: Map<Id, {
    type: DocType,
    // For now, a state CRDT where we keep the maximum number.
    value: number,
    version: Version, // This could easily be simplified. Fine for now though.
  }>,

  connections: Map<Node, {
    // Probabably split this into the versions we do & don't locally know.
    // remoteVersion: Version | null, // Null if we don't have a hello message yet.
    messages: Message[],
  }>,
}

const nextDocId = idGen()
const nextNodeId = idGen()

function createNode(id: number): Node {
  return {
    id,
    nextSeq: idGen(),
    knownVersions: new Map(),
    docs: new Map(),
    connections: new Map(),
  }
}

function sendMessage(sender: Node, receiver: Node, msg: Message) {
  assert(sender.connections.has(receiver))

  console.log('SEND', sender.id, '->', receiver.id, msg)

  let mbox = receiver.connections.get(sender)!.messages
  mbox.push(msg)
}

type DocUpdate = {
  value: number, // MAX crdt new state. But this could instead be a set of operations.
  type: DocType,
  version: Version, // Incoming version to union with existing version.
}

type Update = Map<Id, DocUpdate>

const max = (a: number, b: number) => a < b ? b : a

function getOpsSince(node: Node, version: Version): Update {
  let result: Update = new Map()

  for (const [id, doc] of node.docs) {
    let missingVersions = versionSubtract(doc.version, version)
    if (missingVersions.size > 0) {
      result.set(id, {
        value: doc.value,
        type: doc.type,
        version: missingVersions,
      })
    }
  }

  return result
}

// Returns the version range which we changed locally.
function mergeDocUpdates(node: Node, upd: Update): Version {
  let newVersions: Version = new Map()

  for (const [id, du] of upd) {
    let doc = node.docs.get(id)
    if (doc == null) {
      node.docs.set(id, {
        value: du.value,
        type: du.type,
        version: structuredClone(du.version),
      })

      // We're missing the whole thing.
      newVersions = versionUnion(newVersions, du.version)

      if (VERBOSE) console.log('node', node.id, 'got new document', id, 'at value', du.value)
    } else {
      assert.equal(doc.type, du.type)

      let addedVersions = versionSubtract(du.version, doc.version)
      if (addedVersions.size == 0) {
        assert.equal(doc.value, du.value)
      } else {
        doc.value = max(du.value, doc.value)
        doc.version = versionUnion(du.version, doc.version)

        newVersions = versionUnion(newVersions, addedVersions)
      }

      if (VERBOSE) console.log('node', node.id, 'updated document', id, 'to value', doc.value)
    }
  }

  return newVersions
}

function assertUnreachable(x: never): never {
  throw new Error("Didn't expect to get here");
}

function recvMessage(receiver: Node, sender: Node, msg: Message) {
  let conn = receiver.connections.get(sender)
  assert(conn != null)

  assert(receiver != sender)

  console.log('node', receiver.id, 'RECV from', sender.id, msg.type)
  switch (msg.type) {
    case 'connected': {
      // assert(conn.remoteVersion == null)

      // The peer is now 'properly' connected, and we know the remote peer's version. We'll eagerly
      // send the remote peer all the versions its missing.
      //
      // Might be better to do a pull here, but eh.
      let remoteVersion = msg.version
      let update = getOpsSince(receiver, remoteVersion)
      sendMessage(receiver, sender, {
        type: 'update',
        update,
      })

      // We can union these versions now because we'll assume our message is received.
      // conn.remoteVersion = versionUnion(remoteVersion, receiver.knownVersions)
      break
    }

    case 'update': {
      let addedVersions = mergeDocUpdates(receiver, msg.update)

      console.log('kv', receiver.knownVersions, 'msg', msg, 'adv', addedVersions)
      assert(versionsDistinct(receiver.knownVersions, addedVersions))
      // This is a bit lazy.
      let update = getOpsSince(receiver, receiver.knownVersions)
      receiver.knownVersions = versionUnion(receiver.knownVersions, addedVersions)
      console.log('node', receiver.id, 'KV ->', receiver.knownVersions)

      if (update.size > 0) {
        for (const [node, c] of receiver.connections) {
          if (node == sender) continue // Don't send back to sender.

          sendMessage(receiver, node, {
            type: 'update',
            update,
          })

          // The peer now has all our versions.
          // c.remoteVersion = versionUnion(c.remoteVersion, receiver.knownVersions)
        }
      }

      break
    }

    default:
      assertUnreachable(msg)
  }
}

function changeValue(node: Node, docId: Id, value: number) {
  let doc = node.docs.get(docId)!

  // console.log('ov', doc.version, doc.value)

  // Assign a new version.
  let seq = node.nextSeq()
  console.log('CONSUME', node.id, seq)

  pushSingle(node.knownVersions, node.id, seq)
  pushSingle(doc.version, node.id, seq)

  assert(value >= doc.value)
  doc.value = value

  console.log('node', node.id, 'UPDATED', docId, 'val:', value, 'version', doc.version)

  // We'll broadcast the update to our peers. This is ok because the other peers should be
  // at the parent version already.
  const update: Update = new Map()
  update.set(docId, {
    type: doc.type,
    value,
    version: new Map([[node.id, [[seq, seq+1]]]]),
  })

  // console.log('update', update)

  for (const otherNode of node.connections.keys()) {
    sendMessage(node, otherNode, {
      type: 'update', update
    })
  }
}

function insertDoc(node: Node, docId: Id, type: DocType, value: number) {
  assert(!node.docs.has(docId))

  // Assign a new version.
  let seq = node.nextSeq()
  console.log('CONSUME', node.id, seq)

  let version: Version = new Map()
  pushSingle(version, node.id, seq)

  node.docs.set(docId, { type, value, version })

  console.log('node', node.id, 'CREATED', docId, 'type', type, 'val:', value)

  pushSingle(node.knownVersions, node.id, seq)

  // Broadcast the update to our peers.
  const update: Update = new Map()
  update.set(docId, { type, value, version: structuredClone(version), })

  for (const otherNode of node.connections.keys()) {
    sendMessage(node, otherNode, {
      type: 'update', update
    })
  }
}

function connect(a: Node, b: Node) {
  assert(a != b)
  a.connections.set(b, { messages: [] })
  b.connections.set(a, { messages: [] })

  sendMessage(a, b, {type: 'connected', version: structuredClone(a.knownVersions)})
  sendMessage(b, a, {type: 'connected', version: structuredClone(b.knownVersions)})
}

function disconnect(a: Node, b: Node) {
  // We drop all existing messages, assuming (worst case) that they were lost.
  a.connections.delete(b)
  b.connections.delete(a)
}

function randFromIter<T>(iter: IterableIterator<T>, len: number, randBool: (weight: number) => boolean): T {
  assert(len >= 1, "rand from empty iterator")
  for (const val of iter) {
    if (randBool(1 / len)) { return val }
    len--
  }
  throw Error('randFromIter failed - invalid length')
}

function randFromList<T>(list: T[], randBool: (weight: number) => boolean): T {
  return randFromIter(list.values(), list.length, randBool)
}

interface Network {
  nodes: Map<Id, Node>
}

function addNode(network: Network): Node {
  const node = createNode(nextNodeId())
  network.nodes.set(node.id, node)
  return node
}

function checkNode(node: Node) {
  // The knownVersion field should be the union of all the doc fields.
  let version = new Map()

  for (const doc of node.docs.values()) {
    assert(versionsDistinct(doc.version, version))
    version = versionUnion(version, doc.version)
  }

  assert.deepEqual(version, node.knownVersions)
}

function checkNetwork(net: Network) {
  for (const node of net.nodes.values()) {
    checkNode(node)
  }
}

function fuzzer(seed: string) {
  const random = seedRandom(`s ${seed}`)
  // const newId = () => random().toString(36).slice(2)
  const randInt = (n: number) => Math.floor(random() * n)
  const randBool = (weight = 0.5) => random() < weight

  let verbose = true

  const network = {
    nodes: new Map<Id, Node>()
  }
  // We'll start with 1 node, and never go less than that.
  addNode(network)

  const randNode = (): Node => {
    let key = randFromIter(network.nodes.keys(), network.nodes.size, randBool)
    return network.nodes.get(key)!
  }

  for (let i = 0; i < 100; i++) {
    if (verbose) console.log('\n------------------------------------------------\ni', i)
    // Do some actions from:

    if (i == 19) {
      debugger;
    }

    // Connect (random 2 peers). TODO: Bias this!
    if (randBool(0.1)) {
      let node1 = randNode()
      let node2 = randNode()
      if (node1 !== node2 && !node1.connections.has(node2)) {
        assert(!node2.connections.has(node1))

        if (verbose) console.log('connecting', node1.id, node2.id)
        connect(node1, node2)
      }
    }

    // Disconnect & discard all messages (random 2 peers)
    if (randBool(0.1)) {
      let node1 = randNode()
      let node2 = randNode()
      if (node1 !== node2 && node1.connections.has(node2)) {
        assert(node2.connections.has(node1))
        // Disconnect.
        if (verbose) console.log('disconnecting', node1.id, node2.id)
        disconnect(node1, node2)
      }
    }

    // Deliver some messages.
    // We want a balance of message queues growing and messages being handled. We'll pick random
    // nodes until we either decide to stop, or we hit a node which has handled all of its messages.
    let messagesDelivered = 0
    while (randBool(0.8)) {
      let node = randNode()

      let connWithMessages = [...node.connections.entries()]
        .filter(([node, c]) => c.messages.length > 0)

      if (connWithMessages.length == 0) break // Nothing to recv!

      let [otherNode, mbox] = randFromList(connWithMessages, randBool)
      assert(mbox.messages.length > 0)

      let msg = mbox.messages.shift()!

      if (verbose) console.log('RECV', node.id, msg)
      recvMessage(node, otherNode, msg)
      checkNode(node)

      messagesDelivered++
    }
    if (verbose && messagesDelivered > 0) console.log('delivered', messagesDelivered, 'messages')

    // Insert a new document
    if (randBool(0.1)) {
      let node = randNode()
      if (node.docs.size < 10) {
        const type = randFromList(['a', 'b', 'c'] as DocType[], randBool)
        insertDoc(node, nextDocId(), type, randInt(5))
        checkNode(node)
      }
    }

    checkNetwork(network)
    // Change a value
    if (randBool(0.5)) {
      let node = randNode()
      if (node.docs.size > 0) {
        let docId = randFromIter(node.docs.keys(), node.docs.size, randBool)
        let doc = node.docs.get(docId)!
        let newValue = doc.value + randInt(4)

        changeValue(node, docId, newValue)
        if (verbose) console.log('set node', node.id, 'doc id', docId, 'val', newValue)

        console.log('NODE KV', node.knownVersions)
        console.log(node.docs.get(docId))

        checkNode(node)
      }
    }
    checkNetwork(network)

    // Create a node
    if (network.nodes.size < 10 && (network.nodes.size <= 1 || randBool(0.05))) {
      let otherNode = randNode()
      let node = addNode(network)
      // And connect it to a random peer.
      connect(node, otherNode)
      checkNode(node)

      if (verbose) console.log('created node', node.id, 'connected to', otherNode.id)
    }

    // Destroy a node
    if (network.nodes.size > 1 && randBool(0.05)) {
      let node = randNode()
      // Disconnect it from everything.
      for (const other of node.connections.keys()) {
        disconnect(node, other)
      }
      network.nodes.delete(node.id)

      if (verbose) console.log('destroyed', node.id)
    }

    if (verbose) {
      console.log('network size', network.nodes.size)
      console.log('outstanding messages', [...network.nodes.values()].map(node => (
        [...node.connections.values()].map(c => c.messages.length).reduce((a, b) => a+b, 0)
      )).reduce((a, b) => a+b, 0))

      for (const node of network.nodes.values()) {
        console.log('node', node.id)
        console.log('conn', [...node.connections.keys()].map(n2 => n2.id).sort())
        console.log('values', [...node.docs.entries()].map(([id, d]) => [id, d.value]))
      }
    }

    checkNetwork(network)
  }


  // Then continue until the network has reached quiescence

}


fuzzer("1234512")
