const asyncHandler = require("../utils/asyncHandler");
const { BadRequestError } = require("../utils/error");
const trainService = require('../services/train.service');


exports.createTrain = asyncHandler(async(req, res) =>{
     const {trainNumber, trainName, coachName, seats} = req.body;

     if(!trainNumber || !trainName || !coachName || !seats){
          throw new BadRequestError("trainNumber, trainName, and seats are required");
     }

     if(seats.length === 0){
          throw new BadRequestError("Atleast one seat must be defined...")
     }
                                     //routing to createTrain service in trainService.js
     const train = await trainService.createTrain({trainNumber, trainName, coachName, seats});
     return res.status(201).json({
          success: true,
          message: "Train added successfully",
          data: train
     })
})

//see 02.png-->do trains ke route hain , one train can have one route and each route contains multiple stations
//             dono trains ki from and to destination same hai but route diffrent hai
exports.createRoute = asyncHandler(async(req, res) =>{         //before implementing the createRoute handler we hsve added both the from and to stations aur jo bhi dono routes ke stations hai in our db and we have also saved both trains 
     const {trainId, stations} = req.body;
          //we fetched trainId from req.body coz we need to know ki hmm ye route kis train ke lie define kr rrhe hain
          //ye stations route ke saare stations hai
     if(!trainId || !stations){
          throw new BadRequestError("Train Id and stations are required");
     }

     //we must atleast hai from and to station to create route
     if(stations.length < 2){
          throw new BadRequestError("A route must have at least 2 stations (origin and destination)");
     }

     //routing this req to createRoute service aur ye createRoute service naya route create krkr dega
     const route = await trainService.createRoute({trainId, stations});
     return res.status(201).json({
          success: true,
          message: "Route Created",
          data: route
     });
});

exports.getAllTrains = asyncHandler(async(req, res) =>{
     const trains = await trainService.getAllTrains();
     return res.status(200).json({
          success: true,
          data: trains
     })
})

exports.getTrainById = asyncHandler(async(req, res) =>{
     const {trainId} = req.params;
     if(!trainId){
          throw new BadRequestError("Train Id is missing");
     }
     const train = await trainService.getTrainById(trainId);
     return res.status(200).json({
          success: true,
          data: train
     })
})