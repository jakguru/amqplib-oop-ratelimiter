# amqplib-oop-ratelimiter

Love RabbitMQ but want to be able to have more control over how quickly you consume messages? Then this library is the answer you've been waiting for! A simple, easy to use (and even easy to instrument) client for connecting to RabbitMQ (or really anything that uses amqplib under the hood) and processing messages as quickly (or as slowly) as you allow it to.

This library provides a robust and flexible solution for managing message consumption rate, allowing you to optimize your application's performance and resource usage. It offers a fine-grained control over the rate at which messages are consumed, making it an ideal choice for applications that need to handle high volumes of messages without overwhelming their processing capabilities.

Whether you're dealing with a high-traffic e-commerce platform, a real-time analytics system, or a complex microservices architecture, this library gives you the power to manage your message queues effectively. It's designed to work seamlessly with RabbitMQ and any other systems that use amqplib, providing a consistent and intuitive API that developers will find easy to use.

Moreover, this library is designed with instrumentation in mind, making it easy to integrate with monitoring and observability tools. This means you can keep a close eye on your message queues, spotting potential issues before they become problems and making informed decisions about scaling and resource allocation.

In short, if you're looking for a way to consume RabbitMQ messages at your own pace, this library provides the tools and flexibility you need. Give it a try and take control of your message consumption today!

For more information, view the [documentation](https://jakguru.github.io/amqplib-oop-ratelimiter/).

## Installation

```bash
npm install @jakguru/amqplib-oop-ratelimiter
```

or

```bash
yarn add @jakguru/amqplib-oop-ratelimiter
```

## Usage

### Import / Require the library

```typescript
import { RateLimitedQueueClient } from '@jakguru/amqplib-oop-ratelimiter'
```

or

```typescript
import RateLimitedQueueClient from '@jakguru/amqplib-oop-ratelimiter'
```

or

```javascript
const { RateLimitedQueueClient } = require('@jakguru/amqplib-oop-ratelimiter')
```

### Create a new instance of the client

```typescript
type MyItemType = {
  id: string
  name: string
}

const client = new RateLimitedQueueClient<MyItemType>(
  'my-queue',
  {
    connection: {}, // amqplib connection options
    queue: {
      durable: true,
    },
  },
  async (item: MyItemType) => {
    // do something with the item
  },
  {
    interval: 1000, // ms
    perInterval: 10,
  }
)
```

### Enqueue Items

```typescript
const item: MyItemType = {
  id: '123',
  name: 'My Item',
}
client.enqueue(item)
```
