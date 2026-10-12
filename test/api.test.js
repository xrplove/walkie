const { describe, it, before, after } = require('node:test')
const assert = require('node:assert/strict')
const { createTempDir, startDaemon, stopDaemon, cleanupDir } = require('./helpers')

let tmpDir, sockPath

before(async () => {
  tmpDir = createTempDir()
  process.env.WALKIE_DIR = tmpDir
  const d = await startDaemon(tmpDir)
  sockPath = d.sockPath
})

after(async () => {
  await stopDaemon(sockPath)
  delete process.env.WALKIE_DIR
  cleanupDir(tmpDir)
})

describe('api.listen()', () => {
  it('receives messages from another client', async () => {
    const { listen } = require('../src/api')
    const { request } = require('../src/client')

    const ch = await listen('test-api:secret', { id: 'bot' })
    assert.equal(ch.channel, 'test-api')
    assert.equal(ch.id, 'bot')

    // Send from a different client
    await request({ action: 'join', channel: 'test-api', secret: 'secret', clientId: 'sender' })

    const received = new Promise((resolve) => {
      ch.on('message', (msg) => resolve(msg))
    })

    await request({ action: 'send', channel: 'test-api', message: 'ping', clientId: 'sender' })

    const msg = await received
    assert.equal(msg.from, 'sender')
    assert.equal(msg.data, 'ping')
    assert.ok(msg.ts)

    await ch.close()
  })

  it('ch.send() delivers to other subscribers', async () => {
    const { listen } = require('../src/api')
    const { request } = require('../src/client')

    const ch = await listen('test-send:secret', { id: 'responder' })

    // Join as a reader
    await request({ action: 'join', channel: 'test-send', secret: 'secret', clientId: 'reader' })

    // Send via API
    const result = await ch.send('hello from API')
    assert.equal(result.delivered, 1) // reader gets it

    // Verify reader got it
    const resp = await request({ action: 'read', channel: 'test-send', clientId: 'reader' })
    assert.equal(resp.ok, true)
    const userMsgs = resp.messages.filter(m => m.from !== 'system')
    assert.equal(userMsgs.length, 1)
    assert.equal(userMsgs[0].data, 'hello from API')

    await ch.close()
  })

  it('does not receive own messages', async () => {
    const { listen } = require('../src/api')
    const { request } = require('../src/client')

    const ch = await listen('test-self:secret', { id: 'self-test' })

    const messages = []
    ch.on('message', (msg) => { if (msg.from !== 'system') messages.push(msg) })

    // Send as the same ID — should be filtered
    await ch.send('self-message')

    // Send from someone else — should arrive
    await request({ action: 'join', channel: 'test-self', secret: 'secret', clientId: 'other' })
    await request({ action: 'send', channel: 'test-self', message: 'from-other', clientId: 'other' })

    // Give the stream loop time to deliver
    await new Promise(r => setTimeout(r, 500))

    assert.equal(messages.length, 1)
    assert.equal(messages[0].data, 'from-other')

    await ch.close()
  })
})

describe('api.send()', () => {
  it('sends a one-shot message', async () => {
    const { send } = require('../src/api')
    const { request } = require('../src/client')

    // Set up a listener first
    await request({ action: 'join', channel: 'test-oneshot', secret: 'secret', clientId: 'receiver' })

    const result = await send('test-oneshot:secret', 'fire-and-forget', { id: 'shooter' })
    assert.ok(result.delivered >= 1)

    // Verify receiver got it
    const resp = await request({ action: 'read', channel: 'test-oneshot', clientId: 'receiver' })
    assert.equal(resp.messages.filter(m => m.from !== 'system')[0].data, 'fire-and-forget')
  })
})
