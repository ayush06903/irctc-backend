const asyncHandler = require("../utils/asyncHandler");
const { BadRequestError, NotFoundError } = require("../utils/error");
const userService = require('../services/user.service');
const logger = require("../config/logger");

//see 18.png
exports.getProfile = asyncHandler(async(req, res) =>{
     const userId = req.user.id;  //ye aut.middleware.js ke requireAuth wale function me kch hua dekhlo smjh nhi aya 5 hour 6 min
     if(!userId){
          throw new BadRequestError("User Id is missing");
     }

     const user = await userService.getProfile(userId);
     return res.status(200).json({
          success: true,
          message: "Fetched user details",
          data: {
               user
          }
     })
})

exports.updateProfile = asyncHandler(async(req, res) =>{
     // TODO TASK FOR YOU
})

exports.deleteProfile = asyncHandler(async(req, res) =>{
     // TODO TASK FOR YOU
})

exports.getUserInternal = asyncHandler(async(req, res) =>{
     const { userId } = req.params;
     if(!userId){
          throw new BadRequestError("User Id is missing");
     }

     const user = await userService.getProfile(userId);
     if(!user){
          throw new NotFoundError("User not found");
     }

     return res.status(200).json({
          success: true,
          data: {
               id: user.id,
               firstName: user.firstName,
               lastName: user.lastName,
               email: user.email,
          }
     });
})