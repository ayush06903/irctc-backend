const { ConflictError, BadRequestError, ForbiddenError, UnauthorizedError } = require("../utils/error")
const {generateAndStoreOtp, verifyOtp} = require('../utils/otp');
const {generateAccessToken, generateRefreshToken, verifyRefreshToken} = require('../utils/auth');
const notificationProducer = require('../kafka/producer/notification.producer')
const bcrypt = require('bcrypt');
const prisma = require('../config/prisma');
const {redis} = require('../config/redis');
const { config } = require("../config");
const logger = require('../config/logger');
const jwt = require('jsonwebtoken');
const {OAuth2Client} = require("google-auth-library");
const client = new OAuth2Client(config.GOOGLE_CLIENT_ID);

const sendOTP = async(firstName, lastName, email, password) =>{
     const existingUser = await prisma.user.findUnique({        //checking weather user trying to register already exists in db or not
          where: {email}
     })

     if(existingUser){      //incase given email ke corresponding data already present hai db me
          throw new ConflictError("user already exists");
     }
     //incase that email is already not registered then now we will proceed with register flow
     const hashedPassword = await bcrypt.hash(password, 12);      //firstly hashing the password
     const meta = {firstName, lastName, email, hashedPassword};    //storing user info into meta variable
     //nextt task is to generate and store otp in redis
     const {otp, otpSessionId} = await generateAndStoreOtp(meta);
     
     //yha pr to asynchronously send messages to user and to remove any kind of inconsistency and reduce latency
     //see 14.png-->ye hai poora flow of kafka
     //see 15.png-->this is the whole low level flow in our usecase
                   //kafka server ke andar jo notification.email.otp aur notification.email.welcome hai they are KAFKA TOPICS
                   //user-service is acting as producer it will create the eventt which will contain messages and ye messages apne respective topics pr jayenge and send it to kafka server/kafka broker
                   //in topics ko subscribe kregi notification service aur phr it will recieve those messages(16.js contains format of welcome message) from topics aur phr is data ki help se notification server user ki email pr email bhej degi

     await notificationProducer.sendOtpEmail(email, otp, (config.OTP_TTL) / 60);
                                             //see 15.png usme notification.email.otp topic kka jo format hai->email,otp,ttl usi format me parameers yha se pass honge
                                             //this will route the req to noification.producer.js
     logger.info(`OTP email queued for : ${email}`);
     return {otpSessionId}            //we returned otpSessionId as we can see ki controller me hmne otpSessionId ko destructure krkr store kara hai
}

const verifyOTP = async(otp, otpSessionId) =>{
     //hmko yha user ke dwara provided otp ko jo redis me hashed otp present hai(which we stored in generateandstoreotp function above)
     const meta = await verifyOtp(otp, otpSessionId);
                        //ye diffrent verifyOTP method hai which is defined in utils/otp.js

     if(meta === null){            //mtlb user ne jo otp dia aur jo redis me stored thi vo dono match nhi kre
          throw new BadRequestError("Invalid or expired OTP", "OTP_INVALID");
     }
     //incase otp match hogya to ab hmmm user create krenge
     const user = await prisma.user.create({
          data: {
               firstName: meta.firstName,
               lastName: meta.lastName,
               email: meta.email,
               password: meta.hashedPassword,
               emailVerified: true
          }
     })

     await notificationProducer.sendWelcomeEmail(meta.email, meta.firstName);
     logger.info(`Welcome email queued for ${meta.email}`);
     return user;      //yha se jo hmm user return krenge this will be returned to auth.controller.js--->const user = await authService.verifyOTP(otp, otpSessionId); around line 45
     
}


//see 08.png--->isme hamara poora login ka flow hai
const login = async(email, password, deviceId) =>{
     //firstly we will check weather this email already exists in prisma
     const existingUser = await prisma.user.findUnique({
          where: {email}
     })
     if(!existingUser){
          throw new UnauthorizedError("Invalid email or password", "INVALID_CREDENTIALS");
     }
     if(!existingUser.password){
          throw new BadRequestError(
               "This account was created with Google. Please sign in with Google.",
               "OAUTH_ONLY_ACCOUNT"
          );
     }
     //checking weather the password entered by client and password stored in prisma db are sma or not
     const doesPasswordMatch = await bcrypt.compare(password, existingUser.password);
     if(!doesPasswordMatch){
          throw new UnauthorizedError("Invalid email or password", "INVALID_CREDENTIALS");
     }
     
     //incase passwords match
     const accessToken = generateAccessToken(existingUser.id);         //we generae access token
     const refreshToken = generateRefreshToken(existingUser.id);        //we generate refresh token

     //ab hmare paas 2 choices hai 
            //1->TRADITIONAL APPROACH-->refresh token ko hash kro aur redis me store krlo
            //2->MODERN APPROACH-->refresh toke ke jti(unique identifier) ko lia aur usko redis me store krlia
     const {jti} = jwt.decode(refreshToken);         //jti ko fetch kia from refreshToken
     await redis.set(`refresh:${existingUser.id}:${deviceId}`, jti, 'EX', config.REFRESH_TOKEN_EXP_SEC);
     //ab hmara agla kaam hai to remove password from existing user ki detail
     const {password: _password, ...safeUser} = existingUser;
     await redis.set(`user:${existingUser.id}`, JSON.stringify(safeUser), 'EX', config.REDIS_USER_TTL);
     return {accessToken, refreshToken, loggedInUser: safeUser};
}

//this is the business login that actually generates new access and refresh token
const rotateRefreshToken = async(refreshToken, deviceId) =>{
     const payload = verifyRefreshToken(refreshToken);
                     //is verifyrefreshtoken ka logic is desined in utils/auth.js 
                     //aur ye auth.js wala verifyrefreshtoken verifies jwt token and returns the payload and see in generate refresh token payload containsus userId and jti(refresh token ki unique id)
     const {id: userId, jti} = payload;           //payload se userId aur jti ko extract krlia 
     const storedJti = await redis.get(`refresh:${userId}:${deviceId}`);       //fetching the jti stored in redis
     if(!storedJti){            
          throw new ForbiddenError("Session Expired", "Login AGAIN")
     }
     
     //checking weaher the jti stored in redis and the jti that we got from user's cokkie payload(which contains jti and userId)
     if(storedJti !== jti){           //incase they dont match-->WHAT DOES TTHIS MEAN THAT THEY DIDNOT MATCH?->timestamp 3 hour 17 min
          await redis.del(`refresh:${userId}:${deviceId}`);
          throw new ForbiddenError("Refresh token reused", "LOGIN AGAIN")
     }
     //incase they matched
     const newAccessToken = generateAccessToken(payload.id);
                             //methods present in utils/auth.js
     const newRefreshToken = generateRefreshToken(payload.id);
     const {jti: newJti} = jwt.decode(newRefreshToken);           //ye naye refresh token ki jti ko layega and store it in newjti variable
     await redis.set(`refresh:${payload.id}:${deviceId}`, newJti, 'EX', config.REFRESH_TOKEN_EXP_SEC);             //storing new jti in redis
     return {newAccessToken, newRefreshToken};
}

const verifyGoogleIdToken = async(idToken, deviceId) =>{
     //backend verifying the id token
     const ticket = await client.verifyIdToken({          //this verifies the id token that we got from frontend
          idToken,
          audience: config.GOOGLE_CLIENT_ID //-->ye hme mila from google-auth-library
     })
     const payload = ticket.getPayload();           //extracting payload-->payload here is rreferring to the jwt token that google sent to frontend and then frontend sent it to backend-->see 13.png to see structure of payload

     if(!payload.sub || !payload.email){
          throw new UnauthorizedError("Invalid Google Token Payload")
     }

    //TO AB HMNE ID TOKEN VERIFY KRLIA HAI TO WE NEED TO DO 2 THINGS 
                     //1->create user
                     //2->generate new access and refresh token
     const googleUser = {
          provider: "google",
          providerId: payload.sub,
          email: payload.email,
          firstName: payload.given_name,
          lastName: payload.family_name,
          emailVerified: payload.email_verified || false
     }

     //1->CREATING USER
     //AB BEFORE CREATING THE ENTRY OF THIS googleUser IN OUR DB-->mtlb kya ye email already hmare auth  provider table me exist krti hai?
            //IF YES then simply generate jw token and let user login
            //IF NO then we check in user table ki kya is email se kisi user ki entry hai ya nhi 
                                  // aur agr user table me bhi entry nhi hai to we create entry in user table and also in auth user table
                                  //if we find the entry of that email in our user table(mtlb hoskta hai user ne phle kbhi simply normal sign up kara ho by filling email,pass,confirm pass,firstname,last name etc)--->is case me hme entry ko link krna hoga and also put entry in auth provider table

     
     const user = await prisma.$transaction(async (tx) =>{
                              //what is this transaction

          //sbse phle checking ki kya auth provider table me is user ki entry hai ya nhi                                               
          let googleAuth = await tx.authProvider.findUnique({
               where: {
                    provider_providerId: {
                         provider: googleUser.provider,
                         providerId: googleUser.providerId
                    }
               },
               include: {user: true}
          })

          //agar auth prvider table me user mil gya 
          if(googleAuth){
               return googleAuth.user;
          }

          //agar auth user table me user ki entry nhi mili to ab we will check weather the entry of this user's email is present in user table
          let existingUser = await tx.user.findUnique({
               where: {email: googleUser.email}
          })

          //agar us user ki entry user table me present hai to we need to link account and also create entry corresponding to this email in auth user table too
          if(existingUser){
               await tx.authProvider.create({
                    data: {
                         provider: googleUser.provider,
                         providerId: googleUser.providerId,
                         userId: existingUser.id         //what is significance of id of existing user
                    }
               })
               return existingUser;
          }

          //CASE 3->is user ki entry naa to auth provider table me hai aur naa to user table me
          //firstly create entry of user in user table
          return await tx.user.create({
               data: {
                    email: googleUser.email,
                    firstName: googleUser.firstName,
                    lastName: googleUser.lastName,
                    emailVerified: googleUser.emailVerified,
                    AuthProviders: {
                         create: {
                              provider: googleUser.provider,
                              providerId: googleUser.providerId
                         }
                    }
               }
          })
     })

     //finally generating access and refresh token
     const accessToken = generateAccessToken(user.id);
     const refreshToken = generateRefreshToken(user.id);
     //now we need to store refresh token in redis
     //timestamp 4 hour 5 min ke baad ka thoda dekh log jb redis implement kra hai
     const {jti} = jwt.decode(refreshToken);
     await redis.set(`refresh:${user.id}:${deviceId}`, jti, 'EX', config.REFRESH_TOKEN_EXP_SEC);
     const {password: _password, ...safeUser} = user;
     await redis.set(`user:${user.id}`, JSON.stringify(safeUser), 'EX', config.REDIS_USER_TTL);
     return {accessToken, refreshToken, loggedInUser: safeUser};
     
}
module.exports = {sendOTP, verifyOTP, login, rotateRefreshToken, verifyGoogleIdToken}