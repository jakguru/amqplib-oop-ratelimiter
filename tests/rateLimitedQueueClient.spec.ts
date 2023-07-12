import { test } from '@japa/runner'
import type { RateLimitedQueueConnectionConfiguration } from '../index'
import RateLimitedQueueClient from '../index'

test.group('RateLimitedQueueClient', () => {
  const connection: RateLimitedQueueConnectionConfiguration = {
    connection: {
      protocol: process.env.PROTOCOL,
      hostname: process.env.HOSTNAME,
      port: process.env.PORT ? parseInt(process.env.PORT) : undefined,
      username: process.env.USERNAME,
      password: process.env.PASSWORD,
      locale: process.env.LOCALE,
      frameMax: process.env.FRAMEMAX ? parseInt(process.env.FRAMEMAX) : undefined,
      heartbeat: process.env.HEARTBEAT ? parseInt(process.env.HEARTBEAT) : undefined,
      vhost: process.env.VHOST,
    },
    queue: {
      durable: false,
      autoDelete: true,
    },
  }
  test('enqueue adds item to the queue', async ({ assert }) => {
    const queue = new RateLimitedQueueClient('test', connection, undefined, {
      spillMethod: 'drop',
      autostart: false,
    })
    const item = { name: 'John Doe', age: 30 }
    const result = await queue.enqueue(item)
    assert.isTrue(result)
    await queue.shutdown()
  })

  test('enqueueBulk adds multiple items to the queue', async ({ assert }) => {
    const queue = new RateLimitedQueueClient('test', connection, undefined, {
      spillMethod: 'drop',
      autostart: false,
    })
    const items = [
      { name: 'John Doe', age: 30 },
      { name: 'Jane Doe', age: 25 },
      { name: 'Bob Smith', age: 40 },
    ]
    const results = await queue.enqueueBulk(items)
    assert.deepEqual(results, [true, true, true])
    await queue.shutdown()
  })

  test('stop stops processing the queue', async ({ assert }) => {
    const queue = new RateLimitedQueueClient('test', connection, undefined, {
      spillMethod: 'drop',
      autostart: false,
    })
    await queue.stop()
    assert.isFalse(queue.running)
    await queue.shutdown()
  })

  test('shutdown stops the queue and closes the connection', async ({ assert }) => {
    const queue = new RateLimitedQueueClient('test', connection, undefined, {
      spillMethod: 'drop',
      autostart: false,
    })
    await queue.shutdown()
    assert.isFalse(queue.running)
  })
})
