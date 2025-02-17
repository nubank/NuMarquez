// filepath: /Users/jonathan.moraes.gft/Projects/new-numarquez/NuMarquez/web/services/kafkaProducer.js
const { Kafka } = require('kafkajs');

const KAFKA_LOADBALANCER_DNS = process.env.KAFKA_LOADBALANCER_DNS
const KAFKA_PORT = process.env.KAFKA_PORT

// Configure Kafka client with your broker list (can be set via environment variables)
const kafka = new Kafka({
  clientId: 'log-producer',
  brokers: [`${KAFKA_LOADBALANCER_DNS}:${KAFKA_PORT}`]
});

const producer = kafka.producer();

const connectProducer = async () => {
  await producer.connect();
  console.log('Kafka producer connected.');
};

const sendLogToKafka = async (log) => {
  try {
    await producer.send({
      topic: process.env.KAFKA_LOG_TOPIC || 'logs-topic',
      messages: [
        { value: JSON.stringify(log) }
      ]
    });
    console.log('Log sent to Kafka.');
  } catch (error) {
    console.error('Error sending log to Kafka:', error);
  }
};

module.exports = { connectProducer, sendLogToKafka, producer };