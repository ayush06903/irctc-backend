//admin service me jo bhi events kafka me bheje gye un events ko search service consume kregi and then send it to elastic search

//we have 3 major tasks
           //1->establishing connection btw kafka and search service so that search service can subscribe to the topics in kafka
           //2->search service mount kre data into elasic search
           //3->search feature ko build krna inside search service

//flow is ki elasticsearch.js me hmm indexes create kr rhe hain
//aur is file me search.consumer.js me hmm un indexes me data store krenge--->ye file route kregi to correct business logic in search.service.js
const { consumer, producer, connectProducer } = require('../../config/kafka');
const searchService = require('../../services/search.service');
const logger = require('../../config/logger');
const { KAFKA_TOPICS } = require('../../../../shared/constants/kafka-topics');
const { withDLQ } = require('../../../../shared/utils/dlqHandler');

class SearchConsumer {
     async start() {
          //connecting our consumer with kafka
          await consumer.connect();
          await connectProducer(); // needed for DLQ publishing
          logger.info('Search consumer connected');

          await consumer.subscribe({
               //ab jo consumer hai(search service) it is subscribing to kafka topics
               //jitne bhi neeche list kre hain ssearch service se vo vo topics ko subscribe kara hai
               topics: [
                    KAFKA_TOPICS.STATION_CREATED,
                    KAFKA_TOPICS.ROUTE_CREATED,
                    KAFKA_TOPICS.SCHEDULE_CREATED,
                    KAFKA_TOPICS.SCHEDULE_CANCELLED,
                    KAFKA_TOPICS.SEAT_AVAILABILITY_UPDATED,
               ],
               fromBeginning: true,
          });

          //ab subscribed topics ke messages ko read krne ke lie we will call this .run method
          await consumer.run({
               //we know ki hmne elasticsearch.js me do indexes define kre the 1->sation index
                                                                             //2->train index-->train index inffo related to route and schedule of train to ab yha pr hme un indexes me data store krna hai

               //For each received Kafka message, the eachMessage handler processes it. Your withDLQ(...) wrapper adds failure-handling behavior around that processing.
               eachMessage: withDLQ(producer, KAFKA_TOPICS.DLQ_SEARCH, logger, async ({ topic, partition, message, parsedValue }) => {
                    logger.info(`Processing ${topic}`, { partition, offset: message.offset });
                    /*1. topic — Where did the message come from?
                              Example: topic = "STATION_CREATED";
                              It tells your consumer which Kafka topic delivered the message.Your switch(topic) uses this to choose the correct business logic.

                      2. partition — Which section of that topic contains the message?
                              Kafka divides a topic into smaller sections called partitions.
                                        Imagine the STATION_CREATED topic has three partitions: Kafka topic: STATION_CREATED
                                                                                                Partition 0 — Message A, Message B
                                                                                                Partition 1 — Message C, Message D
                                                                                                Partition 2 — Your message
                                        If the message came from partition 2:  partition = 2;
                                        This helps identify where the message is stored within Kafka. It is useful for debugging and tracking message processing.

                      3. message — The original Kafka message
                                   This is the actual message object Kafka delivers, containing metadata and the raw payload.
                                         For example, it may look conceptually like this: message = {
                                                                                               offset: "15",
                                                                                               key: null,
                                                                                               value: Buffer.from('{"id":"101","name":"New Delhi"}'),
                                                                                               timestamp: "1793000000000"
                                                                                     
4. How all four work together
Suppose Kafka delivers a station-created event.
Variable	Example value	Purpose
topic	"STATION_CREATED"	Which event category?
partition	2	Which partition?
message	Original Kafka message object	Raw data and metadata
parsedValue	{ id: "101", name: "New Delhi" }	Usable JavaScript payload

The flow is:
Kafka delivers a message---->topic identifies the event--->partition identifies its partition--->message contains raw data and metadata---->parsedValue contains the parsed payload----->searchService.indexStation(parsedValue)


Remember the distinction: topic tells you what kind of event it is, partition tells you where in Kafka it came from, message is the original Kafka message, and parsedValue is the usable data extracted from it.*/

                    //ye consumer.run method phle to ye check kregi ki kafka topic kon tha
                    switch (topic) {
                         case KAFKA_TOPICS.STATION_CREATED:      //agar kafka topic STATION_CREATED hai to hmm is data ki entry  station index me krdenge
                              await searchService.indexStation(parsedValue);
                                                             //we are passing parsedValue to the business logic of indexStation in search.service.js
                              break;
                         case KAFKA_TOPICS.ROUTE_CREATED:        //agar event route created hai to hme train index me entry krni hai
                              await searchService.indexTrainRoute(parsedValue);
                              break;
                         case KAFKA_TOPICS.SCHEDULE_CREATED:     //agar event/messsage SCHEDULE_CREATED se aya hai to hmm is data ki entry schedule index me daalenge
                              await searchService.indexSchedule(parsedValue);
                              break;
                         case KAFKA_TOPICS.SCHEDULE_CANCELLED:
                              await searchService.cancelSchedule(parsedValue);
                              break;
                         case KAFKA_TOPICS.SEAT_AVAILABILITY_UPDATED:
                              await searchService.updateSeatAvailability(parsedValue);
                              break;
                         default:
                              logger.warn(`Unknown topic: ${topic}`);
                    }
               }),
          });

          logger.info('Search consumer running...');
     }
}

module.exports = new SearchConsumer();
