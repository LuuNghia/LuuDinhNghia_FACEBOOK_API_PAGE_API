/**
 * kafka.js - Cấu hình Kafka Producer cho webhook-service
 */
const { Kafka, Partitioners } = require('kafkajs');

const BROKER     = process.env.KAFKA_BROKER || 'localhost:9092';
const CLIENT_ID  = 'webhook-service';

const kafka    = new Kafka({ clientId: CLIENT_ID, brokers: [BROKER] });
const producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner });

module.exports = { producer };
