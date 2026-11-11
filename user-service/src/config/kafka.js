const { Kafka, logLevel } = require('kafkajs');
const logger = require('./logger');
const {config} = require('.');
const kafka = new Kafka({       //ye we have created an instance of kafka
     clientId: config.KAFKA_CLIENT_ID,
     brokers: [config.KAFKA_BROKER || 'localhost:9093'],     //for now we are only using a single broker
     logLevel: logLevel.ERROR,
     retry: {
          initialRetryTime: 300,
          retries: 8,
          maxRetryTime: 30000,
     },
});

const producer = kafka.producer({            //producer will generate the events aur phir ye events jayenge in kafka broker aur pboker me hain multiple topics jo in events/messages ko store krenge
     allowAutoTopicCreation: true,     //user ne jis topic ko message bheja hai agr vo topic already exist nhi krta hai then it creates tha topic and sends that event into it
     transactionTimeout: 30000,
     idempotent: true,            //ensures exactly-once delivery so  that ek message baar baar na send ho into topics
     maxInFlightRequests: 5,
     retry: {                     //agr message delivery fail hogyi to we will retry
          retries: 5,
     },
});

let isConnected = false;

const connectProducer = async () => {
     if (!isConnected) {
          await producer.connect();
          isConnected = true;
          logger.info('Kafka producer connected');
     }
};

const disconnectProducer = async () => {
     if (isConnected) {
          await producer.disconnect();
          isConnected = false;
          logger.info('Kafka producer disconnected');
     }
};

module.exports = { kafka, producer, connectProducer, disconnectProducer };