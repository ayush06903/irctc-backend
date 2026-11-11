const { TooManyRequestsError } = require("./error");
const {config} = require('../config');
const {redis} = require('../config/redis');
const otpGenerator = require('otp-generator');
const crypto = require('crypto');

const RATE_MAX = parseInt(config.OTP_RATE_MAX_PER_HOUR || '5', 10);
const ATTEMPT_MAX = parseInt(config.OTP_MAX_VERIFY_ATTEMPTS || '5', 10);
const OTP_TTL = parseInt(config.OTP_TTL || '300', 10);
const HMAC_SECRET = config.OTP_HMAC_SECRET

function hmacFor(email, otp){              //this method hashes the given otp
     return crypto.createHmac('sha256', HMAC_SECRET).update(email + ":" + otp).digest('hex');
                                              
}


async function generateAndStoreOtp(meta){         //iss function me hme otp ko generate krna hai aur redis me store krna hai
     // how many otp's you can send in an hour
     const rateKey = `otp:rate:${meta.email}`;
     //ek perticular mail pr kitni baar otp already send hogya hai that we will get from redis and store in sentCount
     const sentCount = parseInt(await redis.get(rateKey) || '0', 10);
     if(sentCount >= RATE_MAX){
          throw new TooManyRequestsError(
               "Too many OTP requests. Try again later.",
               "OTP_RATE_LIMIT"
          )
     }

     //ab hmara next kaam is to generate otp-->for this we use otpgenerator package
     const otp = otpGenerator.generate(6, {
          upperCaseAlphabets: false,
          lowerCaseAlphabets: false,
          specialChars: false
     })
     //since we know we need to create otpSessionId for each session and it will be unique for each session-->will do this using crypto package
     const otpSessionId = crypto.randomUUID();
     //now we will hash the otp
     const hashed = hmacFor(meta.email, otp);
                    //this hmacFor method is defined in this file itself above generateandstoreotp method
     await redis.set(`otp:session:${otpSessionId}`, JSON.stringify({
                     //this is redis key otp:session:${otpSessionId}
          hashedOtp: hashed,
          meta
     }), 'EX', OTP_TTL);
     //yhaa tk ek baar otp generate krlia to we can increment the value of rate key by one
     await redis.incr(rateKey);
     await redis.expire(rateKey, 3600);        //we set expiry time of ratekey so that ye ratekey ki value har ek ghnte me reset ho
     return {otp, otpSessionId};
}

async function verifyOtp(otp, otpSessionId){
     //yha pr we need to implement 2 things 1->ek user single otp ko kitni baar verify krne ki try kr skta hai(since ek otp ka ttl 5 min hai to un 5 min me user kitni baar verify kr skta hai)  2->we need to fetch otp session from redis
     const rawData = await redis.get(`otp:session:${otpSessionId}`);
     if(!rawData) return null;

     const {hashedOtp: storedOtp, meta} = JSON.parse(rawData);     //yha pr hmm jo redis se raw data nikala hai corresponding to perticular otpSessionId we put while calling this function from auth service aur meta(which has stored all basic user related info)

     const attemptsKey = `otp:attempts:${meta.email}`;
     const attemptsCount = parseInt(await redis.get(attemptsKey) || '0', 10);
     if(attemptsCount >= ATTEMPT_MAX){
          throw new TooManyRequestsError("Too many attempts to verify OTP");
     }

     //now we need to hash the otp entered by user 
     const hashedOtp = hmacFor(meta.email, otp);
     if(crypto.timingSafeEqual(
          Buffer.from(hashedOtp, 'hex'),
          Buffer.from(storedOtp, 'hex'))){
               //incase otp provided by user and otp which is stored in redis match

               ///we will firsly delete the otpsessionId and attemptskey
               await redis.del(`otp:session:${otpSessionId}`, attemptsKey);
               await redis.del(`otp:rate:${meta.email}`);
               return meta;
     }else{     //incase otp match nhi hota hai  
          await redis.incr(attemptsKey);      
          await redis.expire(attemptsKey, OTP_TTL);
          return null;
     }
}

module.exports = {generateAndStoreOtp, verifyOtp};