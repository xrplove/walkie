const { WebSocketServer } = require('ws')
const fs = require('fs')
const path = require('path')
const os = require('os')

const WALKIE_DIR = process.env.WALKIE_DIR || path.join(os.homedir(), '.walkie')
const LOG_FILE = path.join(WALKIE_DIR, 'relay.log')

function log(...args) {
  const line = `[${new Date().toISOString()}] ${args.join(' ')}\n`
  process.stdout.write(line)
  try { fs.appendFileSync(LOG_FILE, line) } catch {}
}

class WalkieRelay {
  constructor() {
    this.clients = new Map()  // ws -> { id, topics: Set<topicHex> }
    this.topics = new Map()   // topicHex -> Set<ws>
  }

  start(port, host) {
    return new Promise((resolve) => {
      fs.mkdirSync(WALKIE_DIR, { recursive: true })
      this.wss = new WebSocketServer({ port, host }, () => {
        log(`Relay listening on ws://${host}:${port}`)
        resolve()
      })
      this.wss.on('connection', (ws) => this._onClient(ws))
    })
  }

  _onClient(ws) {
    const client = { id: null, topics: new Set() }
    this.clients.set(ws, client)
    log(`Client connected (${this.clients.size} total)`)

    ws.on('message', (raw) => {
      try {
        const msg = JSON.parse(raw.toString())
        this._onMessage(ws, msg)
      } catch {}
    })

    ws.on('close', () => this._onDisconnect(ws))
    ws.on('error', () => ws.terminate())
  }

  _onMessage(ws, msg) {
    const client = this.clients.get(ws)
    if (!client) return

    if (msg.t === 'hello') {
      // Update client identity
      client.id = msg.id

      // Remove from old topics
      for (const t of client.topics) {
        const subs = this.topics.get(t)
        if (subs) {
          subs.delete(ws)
          if (subs.size === 0) this.topics.delete(t)
        }
      }

      // Add to new topics
      const newTopics = new Set(msg.topics || [])
      for (const t of newTopics) {
        if (!this.topics.has(t)) this.topics.set(t, new Set())
        this.topics.get(t).add(ws)
      }
      client.topics = newTopics

      log(`Hello from ${client.id}: ${newTopics.size} topic(s)`)
      return
    }

    // Forward msg, sync_req, sync_resp to all other clients on the same topic
    if (msg.t === 'msg' || msg.t === 'sync_req' || msg.t === 'sync_resp') {
      const subs = this.topics.get(msg.topic)
      if (!subs) return

      const payload = JSON.stringify(msg)
      let forwarded = 0
      for (const other of subs) {
        if (other === ws || other.readyState !== 1) continue
        other.send(payload)
        forwarded++
      }

      if (msg.t === 'msg') {
        log(`Routed msg on ${msg.topic.slice(0, 12)}... from ${msg.from || client.id} to ${forwarded} peer(s)`)
      }
    }
  }

  _onDisconnect(ws) {
    const client = this.clients.get(ws)
    if (client) {
      for (const t of client.topics) {
        const subs = this.topics.get(t)
        if (subs) {
          subs.delete(ws)
          if (subs.size === 0) this.topics.delete(t)
        }
      }
      log(`Client ${client.id || 'unknown'} disconnected (${this.clients.size - 1} remaining)`)
    }
    this.clients.delete(ws)
  }

  shutdown() {
    if (this.wss) this.wss.close()
  }
}

module.exports = { WalkieRelay }
