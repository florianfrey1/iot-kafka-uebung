import KafkaClient from './kafka.js'

const kafka = new KafkaClient('localhost:9091')
await kafka.connect()

await kafka.getAll('T3INF4902')