//see 06.png-->admin service jo bhi events publish kregi into kafka then hmari search service will act as consumer and subscribe to all the events published into kafka
//we also used the same code in notification service when we wanted to send otp to the user then user-service was publishing user data into redis and then notification service was active as consumer and was subscribing to the messages present in kafka topics
const { Kafka, logLevel } = require('kafkajs');
const logger = require('./logger');
const { config } = require('.');

const kafka = new Kafka({
     clientId: config.KAFKA_CLIENT_ID,
     brokers: [config.KAFKA_BROKER],
     logLevel: logLevel.ERROR,
     retry: { initialRetryTime: 300, retries: 8, maxRetryTime: 30000 },
});

const consumer = kafka.consumer({
     groupId: 'search-service-group-v2',
     sessionTimeout: 30000,
     heartbeatInterval: 3000,
});

// Producer (used only for DLQ publishing)
const producer = kafka.producer({
     allowAutoTopicCreation: true,
     retry: { retries: 3 },
});

let isProducerConnected = false;

const connectProducer = async () => {
     if (!isProducerConnected) {
          await producer.connect();
          isProducerConnected = true;
          logger.info('Kafka producer connected (DLQ)');
     }
};

const disconnectAll = async () => {
     await consumer.disconnect();
     if (isProducerConnected) {
          await producer.disconnect();
          isProducerConnected = false;
     }
     logger.info('Kafka consumer disconnected');
};

module.exports = { kafka, consumer, producer, connectProducer, disconnectAll };
