//THIS FILE PROVIDES REDIS CONNECTION TO OUR USER SERVICE
//WE HAVE USESD SINGLETON DESIGN PATTERN HERE---->service ke diffrent diffrent controllers ke lie diffrent redis connection establish krne se accha we do it once and they will just import this from here
const Redis = require('ioredis');
const {config} = require('.');
const logger = require('./logger');

class RedisClient {
     static instance;
     static isConnected = false;

     constructor(){
          // prevent direct instantiation line new RediCllient({}) coz agar hmm direct instantiation alllowed kr denge aur if we do new RedisClient({}) multiple timesa then multiple redis connections will be established 

     }


     //thats why we use getInstance here which checks weather a connection is already established and if yes then we use alreay established redis connection
     static getInstance(){
          if(!RedisClient.instance){
               RedisClient.instance = new Redis(config.REDIS_URL, {
                    retryStrategy: (times) =>{
                         //ye retry srategy redis se connection establish krne ke lie retries krti hai
                         const delay = Math.min(times * 50, 2000);
                         return delay;
                    },
                    maxRetriesPerRequest: 3       //itni baar retry kregi retryStrategy to establish connection
               })

               RedisClient.setupEventListeners();
          }
          return RedisClient.instance;
     }

     static setupEventListeners(){
          RedisClient.instance.on('connect', () =>{
               RedisClient.isConnected = true;
               logger.info("Connected to Redis");
          })

          RedisClient.instance.on('error', (error) =>{
               RedisClient.isConnected = false;
               logger.error("Redis connection error", error);
          })

          RedisClient.instance.on('close', () =>{
               RedisClient.isConnected = false;
               logger.warn("Redis connection closed");
          })

          RedisClient.instance.on('reconnecting', () =>{
               logger.warn("Reconnecting to Redis...");
          })

          RedisClient.instance.on('ready', () =>{
               logger.warn("Redis client is ready");
          })

          RedisClient.instance.on('end', () =>{
               RedisClient.isConnected = false;
               logger.warn("Redis connection ended");
          })
     }

     static async closeConnection(){
          if(RedisClient.instance){
               try{
                    await RedisClient.instance.quit();
                    logger.info("Redis connection closed");
               }catch(error){
                    logger.error("Error closing Redis connection: ", error);
               }
          }
     }

     static isReady(){
          return RedisClient.isConnected;
     }

     static async testConnection(){
          try{
               await RedisClient.instance.ping();
               return true;
          }catch(error){
               logger.error("Redis connection test failed: ", error);
               return false;
          }
     }
}

// Export both the singleton instance and the class

module.exports = {
     redis: RedisClient.getInstance(),
     RedisClient
}