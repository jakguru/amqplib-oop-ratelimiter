import type {
  ConnectionConstructorOptions,
  ConnectionInstrumentors,
  InstrumentationHandleFunction,
  Instrumentor,
  Queue,
  QueueAssertionOptions,
  QueueInstrumentors,
  QueueMessage,
} from '@jakguru/amqplib-oop'
import { Connection } from '@jakguru/amqplib-oop'

/**
 * Configuration options for the RateLimitedQueueClient.
 */
export interface RateLimitedQueueConfig {
  /**
   * The number of items to process within the interval period.
   */
  perInterval: number

  /**
   * The interval period in milliseconds.
   */
  interval: number

  /**
   * The spill method to use when the queue is full.
   * - 'drop': send individual items to callback
   * - 'spill': send bulk to callback at once
   */
  spillMethod: 'drop' | 'spill'

  /**
   * Whether to start the queue immediately.
   */
  autostart: boolean
}

/**
 * A callback function that is called when the rate-limited queue spills.
 * @typeparam ItemType The type of items in the queue.
 * @param items An array of items that were spilled from the queue.
 * @returns A Promise that resolves when the callback has completed.
 */
export interface RateLimitedSpillCallback<ItemType = any> {
  /**
   * A callback function that is called when the rate-limited queue spills.
   * @typeparam ItemType The type of items in the queue.
   * @param items An array of items that were spilled from the queue.
   * @returns A Promise that resolves when the callback has completed.
   */
  (items: ItemType[]): Promise<void>
}

export interface TickInstrumentor {
  /**
   * Takes a function handle as input and returns its return type.
   * @param {InstrumentationHandleFunction} handle - The function handle to be instrumented.
   * @param {number} pressure - The amount of messages still in the queue.
   * @returns The return type of the function handle.
   */
  (
    handle: InstrumentationHandleFunction,
    pressure: number
  ): ReturnType<InstrumentationHandleFunction>
}

/**
 * A type that extends the ConnectionInstrumentors and QueueInstrumentors interfaces.
 * This is used to define the instrumentors for the RateLimitedQueueClient.
 */
export type RateLimitedQueueInstrumentors = ConnectionInstrumentors &
  QueueInstrumentors & {
    /**
     * The instrumentation function which is run when a "tick" occurs.
     * @param {InstrumentationHandleFunction} handle - The function handle to be instrumented.
     * @param {number} pressure - The amount of messages still in the queue.
     * @returns The return type of the function handle.
     */
    tick: TickInstrumentor

    /**
     * A function that is called when an error occurs while handling a message.
     * @param {Error} error - The error that occurred.
     * @param {QueueMessage} message - The message that caused the error.
     */
    onHandleMessageError: (error: Error, message: QueueMessage) => void

    /**
     * A function that is called when a drop is spilled from the queue (meaning that the callback has been called).
     * @param {InstrumentationHandleFunction} handle - The function handle to be instrumented.
     * @param {number} pressure - The amount of messages still in the queue.
     * @returns The return type of the function handle.
     */
    drop: Instrumentor
  }

/**
 * Configuration options for the connection and queue used by the RateLimitedQueueClient.
 */
export interface RateLimitedQueueConnectionConfiguration {
  /**
   * The connection configuration options or an existing connection instance.
   */
  connection: Partial<ConnectionConstructorOptions> | Connection
  /**
   * The queue assertion options.
   */
  queue?: Partial<QueueAssertionOptions>
}

/**
 * A rate-limited queue client that allows you to limit the rate at which messages are processed.
 * @typeparam ItemType The type of items in the queue.
 */
export class RateLimitedQueueClient<ItemType = any> {
  readonly #queue: Promise<Queue>
  readonly #config: RateLimitedQueueConfig
  readonly #tickInstrumentor: TickInstrumentor
  readonly #onHandleMessageError: RateLimitedQueueInstrumentors['onHandleMessageError']
  readonly #dropInstrumentor: Instrumentor
  readonly #conn: Connection
  readonly #selfGeneratedConnection: boolean
  #callback?: RateLimitedSpillCallback<ItemType>
  #running: boolean = false
  #working: boolean = false
  #tick?: Promise<void>
  #lastTick: number = 0
  #isShutDown: boolean = false

  /**
   * Creates a new instance of the RateLimitedQueueClient.
   * @param connection The connection and queue configuration options.
   * @param name The name of the queue.
   * @param callback The callback function to call when the queue spills.
   * @param config The configuration options for the queue.
   * @throws An error if the perInterval or interval values are invalid.
   */
  constructor(
    name: string,
    connection: RateLimitedQueueConnectionConfiguration,
    callback?: RateLimitedSpillCallback<ItemType>,
    config: Partial<RateLimitedQueueConfig> = {},
    instrumentors: Partial<RateLimitedQueueInstrumentors> = {}
  ) {
    this.#config = Object.assign(
      {},
      {
        perInterval: 1,
        interval: 1000,
        spillMethod: 'drop',
        autostart: true,
      },
      config
    )
    if (this.#config.perInterval < 1) {
      throw new Error('perInterval must be at least 1')
    }
    if (this.#config.interval < 100) {
      throw new Error('interval must be at least 100')
    }
    this.#tickInstrumentor =
      instrumentors.tick || ((handle: InstrumentationHandleFunction) => handle())
    this.#onHandleMessageError = instrumentors.onHandleMessageError || (() => {})
    this.#dropInstrumentor =
      instrumentors.drop || ((handle: InstrumentationHandleFunction) => handle())
    if (
      connection &&
      'object' === typeof connection &&
      'object' === typeof connection.connection &&
      !(connection.connection instanceof Connection)
    ) {
      this.#conn = new Connection(connection.connection, {
        assertQueue: instrumentors.assertQueue ? instrumentors.assertQueue : (handle) => handle(),
        createChannel: instrumentors.createChannel
          ? instrumentors.createChannel
          : (handle) => handle(),
        eventEmitter: instrumentors.eventEmitter
          ? instrumentors.eventEmitter
          : (handle) => handle(),
        eventListener: instrumentors.eventListener
          ? instrumentors.eventListener
          : (handle) => handle(),
        getQueue: instrumentors.getQueue ? instrumentors.getQueue : (handle) => handle(),
        initialization: instrumentors.initialization
          ? instrumentors.initialization
          : (handle) => handle(),
        shutdown: instrumentors.shutdown ? instrumentors.shutdown : (handle) => handle(),
      })
      this.#selfGeneratedConnection = true
    } else if (
      connection &&
      'object' === typeof connection &&
      'object' === typeof connection.connection &&
      connection.connection instanceof Connection
    ) {
      this.#conn = connection.connection
      this.#selfGeneratedConnection = false
    } else {
      throw new Error(
        'you must provide either the configuration for a connection or an already instantiated connection'
      )
    }
    this.#queue = this.#conn.getQueue(
      name,
      {
        type: 'basic',
        durable: true,
        autoDelete: false,
      },
      {
        preShutDown: instrumentors.preShutDown ? instrumentors.preShutDown : (handle) => handle(),
        shutdown: instrumentors.shutdown ? instrumentors.shutdown : (handle) => handle(),
        check: instrumentors.check ? instrumentors.check : (handle) => handle(),
        delete: instrumentors.delete ? instrumentors.delete : (handle) => handle(),
        purge: instrumentors.purge ? instrumentors.purge : (handle) => handle(),
        enqueue: instrumentors.enqueue ? instrumentors.enqueue : (handle) => handle(),
        ack: instrumentors.ack ? instrumentors.ack : (handle) => handle(),
        nack: instrumentors.nack ? instrumentors.nack : (handle) => handle(),
        get: instrumentors.get ? instrumentors.get : (handle) => handle(),
        listen: instrumentors.listen ? instrumentors.listen : (handle) => handle(),
        pause: instrumentors.pause ? instrumentors.pause : (handle) => handle(),
        eventListener: instrumentors.eventListener
          ? instrumentors.eventListener
          : (handle) => handle(),
        eventEmitter: instrumentors.eventEmitter
          ? instrumentors.eventEmitter
          : (handle) => handle(),
        messageListener: instrumentors.messageListener
          ? instrumentors.messageListener
          : (handle) => handle(),
        tick: instrumentors.tick ? instrumentors.tick : (handle) => handle(),
        consumer: instrumentors.consumer ? instrumentors.consumer : (handle) => handle(),
      }
    )
    this.#queue.then((queue) => {
      queue.$on('deleted', () => {
        this.#isShutDown = true
      })
    })
    this.#callback = callback
    if (this.#config.autostart) {
      this.start()
    }
  }

  /**
   * Returns a boolean indicating whether or not the queue is running.
   * @returns A boolean indicating whether or not the queue is running.
   */
  public get running() {
    return !this.#isShutDown && this.#running
  }

  /**
   * Returns a boolean indicating whether or not the queue is currently processing a tick.
   * @returns A boolean indicating whether or not the queue is currently processing a tick.
   */
  public get working() {
    return !this.#isShutDown && this.#working
  }

  /**
   * Returns a boolean indicating whether or not the queue has been shut down.
   * @returns A boolean indicating whether or not the queue has been shut down.
   */
  public get isShutDown() {
    return this.#isShutDown
  }

  /**
   * Sets the callback function to be called when the rate-limited queue spills.
   * @typeparam ItemType The type of items in the queue.
   * @param callback The callback function to call when the queue spills.
   * @returns void
   */
  public setCallback(callback: RateLimitedSpillCallback<ItemType>): void {
    if (this.#isShutDown) {
      throw new Error('Queue has been shut down and must be reinitialized')
    }
    this.#callback = callback
  }

  /**
   * Gets the total number of jobs in the queue (waiting + active).
   * @returns A Promise that resolves to the total number of jobs in the queue.
   */
  public async getPressure(): Promise<number> {
    if (this.#isShutDown) {
      throw new Error('Queue has been shut down and must be reinitialized')
    }
    const queue = await this.#queue
    try {
      const { messageCount } = await queue.check()
      return messageCount
    } catch {
      return -1
    }
  }

  /**
   * Starts processing the queue.
   * If the queue is already running, it will wait for the current tick to complete before starting a new one.
   * @returns A Promise that resolves when the queue has started.
   */
  public async start(): Promise<void> {
    if (this.#isShutDown) {
      throw new Error('Queue has been shut down and must be reinitialized')
    }
    if (!this.#callback) {
      throw new Error('No callback function set')
    }
    if (this.#running) {
      return
    }
    if (this.#working) {
      await this.#tick
    }
    this.#running = true
    this.#onTick()
  }

  /**
   * Stops processing the queue.
   * If the queue is currently working, it will wait for the current tick to complete before stopping.
   * @returns A Promise that resolves when the queue has stopped.
   */
  public async stop(): Promise<void> {
    if (this.#isShutDown) {
      return
    }
    this.#running = false
    if (this.#working) {
      await this.#tick
    }
    const queue = await this.#queue
    await queue.awaitHandlingOfConfirmations()
  }

  /**
   * Shuts down the queue.
   * @returns A Promise that resolves when the queue has shut down.
   * @remarks This will stop the queue and close the Connection connection.
   */
  public async shutdown(): Promise<void> {
    if (this.#isShutDown) {
      return
    }
    await this.stop()
    let queue: Queue | undefined
    try {
      queue = await this.#queue
    } catch {
      // noop
    }
    if (queue) {
      try {
        await queue.awaitHandlingOfConfirmations()
      } catch {
        // noop
      }
      try {
        await queue.pause(10000)
      } catch {
        // noop
      }
    }
    if (this.#selfGeneratedConnection) {
      await this.#conn.close()
    }
    this.#isShutDown = true
  }

  /**
   * Adds an item to the rate-limited queue.
   * @param item The item to add to the queue.
   * @returns A Promise that resolves when the item has been added to the queue.
   */
  public async enqueue(item: ItemType): Promise<boolean> {
    if (this.#isShutDown) {
      throw new Error('cannot enqueue item, queue is shut down')
    }
    const queue = await this.#queue
    return await queue.enqueue(Buffer.from(JSON.stringify(item)), {
      contentType: 'application/json',
    })
  }

  /**
   * Adds multiple items to the rate-limited queue in bulk.
   * @param items An array of items to add to the queue.
   * @returns A Promise that resolves when all items have been added to the queue.
   */
  public async enqueueBulk(items: ItemType[]): Promise<Array<boolean>> {
    if (this.#isShutDown) {
      throw new Error('cannot enqueue items, queue is shut down')
    }
    return Promise.all(items.map((item) => this.enqueue(item)))
  }

  /**
   * The internal tick function that processes the queue.
   * @returns A Promise that resolves when the tick has completed.
   */
  async #onTick(): Promise<void> {
    const queue = await this.#queue
    const pressure = await this.getPressure()
    // do not start if already working or already stopped
    if (this.#working || !this.#running || !this.#callback || this.#isShutDown) {
      return
    }
    this.#working = true
    if (pressure < 0) {
      if (this.#running) {
        setTimeout(this.#onTick.bind(this), this.#config.interval)
      }
      return
    }
    this.#tick = this.#tickInstrumentor(async () => {
      if (!this.#callback) {
        return
      }
      // if we've stated too soon, wait until we've waited long enough
      const now = Date.now()
      if (now - this.#lastTick < this.#config.interval) {
        await this.#wait(this.#config.interval - (now - this.#lastTick))
      }
      // collect the items to process
      const items: Array<QueueMessage> = []
      /**
       * While we have items in the queue and we haven't reached the perInterval limit, pull jobs from the queue to process.
       */
      // eslint-disable-next-line no-constant-condition -- we're using checks inside the loop to break out of it
      while (true) {
        const item = await queue.get()
        if (item === false) {
          break
        }
        items.push(item)
        if (items.length >= this.#config.perInterval) {
          break
        }
      }
      // handle the processing of the items
      if (items.length > 0) {
        if (this.#config.spillMethod === 'drop') {
          for (const item of items) {
            await this.#handleMessage<ItemType>(item, queue)
          }
        } else {
          await this.#handleMessages<ItemType>(items, queue)
        }
      }
      this.#working = false
      this.#lastTick = Date.now()
      await queue.awaitHandlingOfConfirmations()
    }, pressure)
    await this.#tick
    if (this.#running) {
      setTimeout(this.#onTick.bind(this), this.#config.interval)
    }
  }

  /**
   * A utility function that waits for a specified amount of time.
   * @param time The amount of time to wait in milliseconds.
   * @returns A Promise that resolves after the specified amount of time has elapsed.
   */
  async #wait(time: number) {
    return new Promise((resolve) => {
      setTimeout(resolve, time)
    })
  }

  async #handleMessage<ItemType>(message: QueueMessage, queue: Queue): Promise<void> {
    if (!this.#callback) {
      queue.nack(message, true)
      return
    }
    let data: ItemType
    try {
      data = JSON.parse(message.content.toString()) as ItemType
    } catch (error) {
      this.#onHandleMessageError(error, message)
      queue.nack(message, false)
      return
    }
    try {
      await this.#dropInstrumentor(this.#callback.bind(null, [data]))
      queue.ack(message)
    } catch (error) {
      this.#onHandleMessageError(error, message)
      queue.nack(message, true)
    }
  }

  async #handleMessages<ItemType>(messages: Array<QueueMessage>, queue: Queue): Promise<void> {
    if (!this.#callback) {
      messages.forEach((message) => queue.nack(message, true))
      return
    }
    const items: Array<ItemType> = []
    messages.forEach((message) => {
      let data: ItemType
      try {
        data = JSON.parse(message.content.toString()) as ItemType
        items.push(data)
      } catch (error) {
        this.#onHandleMessageError(error, message)
        queue.nack(message, false)
      }
    })
    try {
      await this.#dropInstrumentor(this.#callback.bind(null, items))
      messages.forEach((message) => queue.ack(message))
    } catch (error) {
      messages.forEach((message) => {
        this.#onHandleMessageError(error, message)
        queue.nack(message, true)
      })
    }
  }
}

/**
 * Represents a rate-limited queue client that can enqueue and process items with a specified rate limit.
 * @typeparam ItemType The type of items in the queue.
 */

export default RateLimitedQueueClient
