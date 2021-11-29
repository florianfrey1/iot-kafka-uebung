import { Kafka } from 'kafkajs'

export default class KafkaClient {
    #producer
    #consumer
    #admin
    constructor(broker) {
        const kafka = new Kafka({
            clientId: 'client-123',
            brokers: [broker]
        })

        this.#producer = kafka.producer()
        this.#consumer = kafka.consumer({ groupId: 'default-group' })
        this.#admin = kafka.admin()
    }
    async connect() {
        await this.#producer.connect()
        await this.#consumer.connect()
        await this.#admin.connect()
    }
    async add(topic, message) {
        await this.#producer.send({
            topic: topic,
            messages: [
                {
                    key: message.id.toString(),
                    value: JSON.stringify(message)
                },
            ],
        })
    }
    async getAll(topic) {
        await this.#consumer.subscribe({ topic: topic, fromBeginning: true })
        await this.#consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                console.log({
                    partition,
                    key: message.key.toString(),
                    offset: message.offset,
                    value: message.value.toString(),
                })
            },
            autoCommit: false
        })
    }
    async createTopic(name) {
        await this.#admin.createTopics({
            topics: [
                {
                    topic: name,
                    numPartitions: 2
                }
            ]
        })
    }
}