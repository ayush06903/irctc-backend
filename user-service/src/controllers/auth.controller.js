//yhha pr we donot handle any business logic we just route the http req to correct services
const { BadRequestError, UnauthorizedError } = require("../utils/error");
const asyncHandler = require('../utils/asyncHandler');
const {config} = require('../config');
const authService = require('../services/auth.service');
const getDeviceFingerprint = require("../utils/deviceFingerPrint");

const isProd = process.env.NODE_ENV === 'production';

const cookieOptions = (maxAge) => ({
     httpOnly: true,
     secure: isProd,
     sameSite: isProd ? 'strict' : 'lax',
     maxAge,
});

exports.sendOTP = asyncHandler(async(req, res) =>{
     const {firstName, lastName, email, password, confirmPassword} = req.body;
     if(!firstName || !lastName || !email || !password || !confirmPassword){
          throw new BadRequestError("All fields are mandatory");
     }

     if(password !== confirmPassword){
          throw new BadRequestError("Password mismatch");
     }

     const {otpSessionId} = await authService.sendOTP(firstName, lastName, email, password);
     res.cookie("otp_session", otpSessionId, cookieOptions(config.OTP_TTL * 1000)).status(200).json({
          success: true,
          message: "OTP sent successfully"
     })
})
//yha tk hmare backend se otp generate kra usko redis me, user ko otpSessionId cookies ke andar wrap krkr bhej di, aur user ki email pr ek otp bhej dia 
//ab user will enter that otp and try to proceed to now we need to write logic of verifying the otp entered by user
exports.verifyOTP = asyncHandler(async(req, res) =>{
     //yha pr otp user provide karega aur otpSessioId hmm cookies se extract krenge
     const {otp} = req.body;
     const otpSessionId = req.cookies.otp_session;

     if(!otp || !otpSessionId){
          throw new BadRequestError("OTP or OTPSession is missing")
     }

     //now routing the req to auth.service.js jahapr hmne business logic likha hai otp verification kaa
     const user = await authService.verifyOTP(otp, otpSessionId);
     res.clearCookie("otp_session");      //cookie me jo otpsessionId stored hai usko clear krdo
     return res.status(201).json({
          success: true,
          message: "User Account created successfully",
          data: user
     })
})

//see 04.png-->isme hamara poora flow hai login ka 
//ACCESS TOKEN--->See 05.png ---> lets say hmari ek sysem design related website hai which has diffrent difrent sections to so that hme har section me visit krne ke lie alag se authenticate na krna pde to thats the reason why access token was introduced access token which is provided by server when we give email and pass and then we can use that access token to access all the sections wiithout doing any authentication bas hme har section pr access token dikhana hoga agar access token valid hoga to we can access that section basically it acts like id card or simply  enables us to acces resourse wih a website)
//REFRESH TOKEN-->is used to generate access token

//to we can see 04.png and understand the flow of access,refresh token-->see 06.png-->jb server ne user ko access and refresh token cookies me wrap krkr bhej dia hai uske baad user backend pr koi bhii req krega to req will carry access token in it..then backend recieves the request and then it will verify the access token
           //if the access token is valid-->then backen will route the request to payment service(kyuki fronend se paymen service call kri thi)
           //if the access token is invalid to backend will return 401 errorr to frontend

           //access token is short lived--->lets say 15 min
                   //since access token har 15 min me expire ho jata hai to are we going to force user into doing login every 15 min?-->NO and thats why refresh token is for
           //refresh token is long livesd-->lets say 7 days

           //and in case we send http req to server and server sends 401 error telling ki access token expire hogya hai and frontend understands it and hits /auth/refresh route which will issue new access and refresh token---> see 07.png --> and then thse new access and refresh token are stored in cookie

           //and incase 7 din tak user ki side se koi acivity nhi hoti hai to refresh token is expired and then we tell user to login again
                   
//is controller ko implement krne ke lie we have added 2 new fies in utils folder->auth.js and devicefingerprint
exports.login = asyncHandler(async(req, res) =>{
     const {email, password} = req.body;          //since frontend se email and password ayega to hmne dono ke destructure kr kr variable me store krlia 
     if(!email || !password){
          throw new BadRequestError("Email and Password are required")
     }
     const deviceId = getDeviceFingerprint(req);
     //now our next task is to route the incoming http req to correct business logic(login function in auth service)
     const {accessToken, refreshToken, loggedInUser} = await authService.login(email, password, deviceId);   //ye auth service ka login function will provide us 3 things access token,refresh token and third thing is details of loggedin user
     //storingg access and refresh token in cookie 
     res.cookie("accessToken", accessToken, cookieOptions(config.ACCESS_TOKEN_EXP_SEC * 1000))           //storingg access token in cookie 
     res.cookie("refreshToken", refreshToken, cookieOptions(config.REFRESH_TOKEN_EXP_SEC * 1000))        //storing refersh token in cookie
     .status(200).json({
          success: true,
          message: "Logged in successfully",
          loggedInUser
     })
})

//generate new pair of refresh and access token
exports.rotateRefreshToken = asyncHandler(async(req, res) =>{
     const refreshToken = req.cookies.refreshToken;            //cookie se refresh token fetch krenge
     if(!refreshToken){        //checking weaher refresh token mila ya nhi
          throw new UnauthorizedError("Refresh token is missing", "LOGIN AGAIN")
     }
     const deviceId = getDeviceFingerprint(req);
     const {newAccessToken, newRefreshToken} = await authService.rotateRefreshToken(refreshToken, deviceId);
                                                      //calling the function inside authService that actually generates new access and refresh token
     //storing newly generated access and refresh token in cookie and setting their ttl 
     res.cookie("accessToken", newAccessToken, cookieOptions(config.ACCESS_TOKEN_EXP_SEC * 1000))
     res.cookie("refreshToken", newRefreshToken, cookieOptions(config.REFRESH_TOKEN_EXP_SEC * 1000))
     .status(200).json({
          success: true,
          message: "Access and Refresh token reissued"
     })
})

//see 09.png---> 0AUTH 2.0 provodes autherization flow-->ye ek authorization protocol hai jiski help se hmm goohle services/api's ko access kr skte hain
                         //see 10.png->lets say we have a product that we built and is product ke ek specific usecase me it needs google callender to schedule events

//               Google identity service provides authentication flow(login with google)
                          //see 11.png

//see 12.png-->steps involved in google authentication
            //we can see jaise hi hmm step 2 me account select krenge to google returns id token of selected account to frontend
            //and then in step 4 frontend sends id token to backend and tehn backend varifies the id token--->WHY DIDNT FRONTEND VARIFY id_token AND IT SENDS ID TOKEN TO BACKEND FOR VERIFICATION---->frontend me breach ho skta hai hacker

//see 13.png--->13.png me hmare jwt token/id token ka structure hai
exports.verifyGoogleIdToken = asyncHandler(async(req, res) =>{            //yhapr jo hamara frontend se id token aa rha hai usko verify krna hai using google's public key,create user and then create new pair of access and refresh token
     //sbse phle we added auth provider ka schema in schema.prisma-->is schema ki help se hmm future me github,facebook jaise aur bhi auth methods ko use kr skte hain-->timestamp 3 hour 38 min

     const {idToken} = req.body;
     if(!idToken){
          throw new BadRequestError("Invalid Google ID Token", "INVALID TOKEN")
     }

     const deviceId = getDeviceFingerprint(req);
     
     const {accessToken, refreshToken, loggedInUser} = await authService.verifyGoogleIdToken(idToken, deviceId);
     
     //storing newly created access and refresh token in cookies
     res.cookie("accessToken", accessToken, cookieOptions(config.ACCESS_TOKEN_EXP_SEC * 1000))
     res.cookie("refreshToken", refreshToken, cookieOptions(config.REFRESH_TOKEN_EXP_SEC * 1000))
     .status(200).json({
          success: true,
          message: "Logged in successfully",
          loggedInUser
     })
})