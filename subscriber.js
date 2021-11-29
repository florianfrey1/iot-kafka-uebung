import mqtt from 'mqtt'
import DateTime from './datetime.js'
import KafkaClient from './kafka.js'

const kafka = new KafkaClient('localhost:9091')
await kafka.connect()

const client = mqtt.connect('mqtt://broker.hivemq.com', {
    clientId: 'ad49cd66d5f7fb0c8c7c1337173d7719',
    clean: true
})

client.on('connect', () => {
    client.subscribe('iotcourse/T3INF4902', { qos: 1 })
})

client.on('message', async (topic, message, options) => {
    console.log(`[${topic}] ${message.toString()} [retain: ${options.retain}, qos: ${options.qos}]`)

    await kafka.add('T3INF4902', JSON.parse(message))
})