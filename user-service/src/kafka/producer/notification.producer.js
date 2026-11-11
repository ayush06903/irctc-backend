//we use kafka so that we can send messages like otp to user asynchronously
const { producer, connectProducer } = require('../../config/kafka');
const logger = require('../../config/logger');
const { KAFKA_TOPICS } = require('../../../../shared/constants/kafka-topics');

//this class acts as wrapper so that the services like auth.js donot directly touch kafka
class NotificationProducer {
     constructor() {          //this constructor ensures singleton behaviour mtlb as we know we need to establish a connection btw kafka and user service so singleton behaviour meaning it will make sure ki user service aur kafka ke beech me ek hi connection established rhe not multiple--->so it will avoid multiple producer connect
          this.isInitialized = false;
     }

     async initialize() {         //initializing the connection
          if (!this.isInitialized) {
               await connectProducer();
               this.isInitialized = true;
          }
     }

     //jo auth.service.js ke await notificationProducer.sendOtpEmail(email, otp, (config.OTP_TTL) / 60); jo req yha pr ayi hai usko implement krenge neeche which will send otp email to user asyncronously
     async sendMessage(topic, key, value){
          try{
               await this.initialize();        //connecting producer service to kafka

               const message = {
                    topic,
                    messages: [{
                         key: key || `${topic}-${Date.now()}`,
                         value: JSON.stringify(value),
                         timeStamp: Date.now().toString()
                    }]
               }

               const result = await producer.send(message);        //sending message to kafkka broker
               logger.info(`Message sent to kafka topic: ${topic}`, {
                    key,
                    partition: result[0].partition,
                    offset: result[0].offset,
               });
               
               return result;
          }catch(error){
               logger.error(`Failed to send message to kafka topic: ${topic}`, {
                    error: error.message,
                    stack: error.stack,
                    key
               })
               throw error;
          }
     }
     async sendOtpEmail(email, otp, ttlMinutes = 5){
                    //ye sendmessage vo method hai jo kafka broker me present topics ko message bhejegi aur isis sendmessage ko hmm upr implement kra hai-->see 17.png 
          return this.sendMessage(
               KAFKA_TOPICS.OTP_EMAIL,
               `otp-${email}`,
               {email, otp, ttlMinutes}
          )
     }

     async sendWelcomeEmail(email, firstName){
          return this.sendMessage(
               KAFKA_TOPICS.WELCOME_EMAIL,
               `welcome-${email}`,
               {email, firstName}
          )
     }
}

module.exports = new NotificationProducer();