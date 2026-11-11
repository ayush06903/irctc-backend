const { config } = require("../config");
const {redis} = require("../config/redis");
const prisma = require('../config/prisma');
const logger = require('../config/logger');



const getProfile = async(userId) =>{
     logger.info("First check user in Redis");

     //client profile req kr rha hai to phle ye req redis pr jayegi
     const storedUser = await redis.get(`user:${userId}`);
     if(storedUser){
          logger.info("Fetched user profile from redis");
          return JSON.parse(storedUser);        //yha we parsed the data before returning coz redis me store krte smay hmne stringify kara tha to parse krne se ye waps object ke form me aa jayega
     }
     //incase is user ka corresponding data redis me nhi mila to now we will fetch his data from db
     logger.info("If user is not in redis, fetch user from DB");
     const userProfile = await prisma.user.findUnique({
          where: {
               id: userId
          }
     })
     
     logger.info("Exclude password field from the user");        
     const {password: _password, ...safeUser} = userProfile;           //Excluded password field from the user coz we will store this data of this user in redis so in redis we store after removing password from data
                               //safeUser me    //aur ye hmne poora data fetch kra from userProfile upr dekho db se data nikalkr userProfile variable me data save kara tha
                               //data-pass save 
                               //ho gya 
     logger.info("Store user profile in redis for future lookups");
     await redis.set(`user:${userId}`, JSON.stringify(safeUser), 'EX', config.REDIS_USER_TTL);
     return safeUser;
}         //go to 5 hour 15 min for practical demo

module.exports = {getProfile}