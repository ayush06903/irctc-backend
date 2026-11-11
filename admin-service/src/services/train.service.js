const prisma = require("../config/prisma");
const { BadRequestError, ConflictError, NotFoundError } = require("../utils/error");

const adminProducer = require('../kafka/producer/admin.producer');
const logger = require("../config/logger");

const createTrain = async (data) => {
     const { trainNumber, trainName, coachName, seats } = data;
     //checking weather ye train phle se hi exist to nhi krti in out db
     const existing = await prisma.train.findUnique(
          { where: { trainNumber } }
     )
     //throwing error in case exist krti hai
     if (existing) {
          throw new ConflictError("Train with this number already exists");
     }

     //as we know ki we decided ki hamari coach me multiple trains ho skti hai aur agar kisi train ka number 1 hai to us coach(hamare case me train coz hamari train me ek hi coach hai) ki baaki seat ka number 1 nhi hoskta hai
     const seatNumbers = seats.map((s) => s.seatNumber);             //to yha pr hmne hamari coach me jitni bhi seats hain un saari seats ke seat numberr find kre

     //to ab hmm ye check krenge ki weather saari seas ka sea number same hai ya nhi
     if (new Set(seatNumbers).size !== seatNumbers.length) {                //thoda dhyan se condition dekho smjh aa jayegi
          throw new BadRequestError('Duplicate seat numbers found');
     }

     //creating train in db-->HMARE PROJECT KI YE EK IMPORTANT ASSUMPTION HAI KI OUR TRAIN HAS ONLY ONE COACH
     const train = await prisma.train.create({
          //ye nhi smjh aya ki yha konsa data kis table me jaa rha hai
          data: {
               trainNumber,
               trainName,
               coachName: coachName || 'AC',
               totalSeats: seats.length,
               seats: {
                    create: seats.map((seat) => ({
                         seatNumber: seat.seatNumber,
                         seatType: seat.seatType,
                         price: seat.price
                    }))
               }
          },
          include: { seats: { orderBy: { seatNumber: 'asc' } } }         //iska mtlb hai ki train ki jo bhi seats hai unka seat number ascending order me arranged hoga
     })

     //ab hmara next kaam is to push this train created event in kafka
     await adminProducer.publishTrainCreated(train).catch((err) => {
          logger.error('Failed to publish train created event', { error: err.message });
     });

     return train;
}

const createRoute = async (data) => {
                         //trainId aur stations jo controller se aye hai usko phle hmne data variable me store krlia 
                         /*const data = {
                                   trainId: "T101",
                                   stations: ["Delhi", "Kanpur", "Lucknow"]
                           };*/
     const { trainId, stations } = data;          //jo data me trainId key hai us trainId ki value trainId variable me store hojaye aur stations key ki value stations variable me store ho jaye

     //checcking weather jis train ka route create kr rhe hai vo hmare db me present hai bhi ya nhi(hona necessary hai to create route)
     const train = await prisma.train.findUnique({
          where: { id: trainId }
     });

     if (!train) {   //if train doesnot exist thwow error
          throw new NotFoundError('Train not found');
     }

     //no we will check ki is train ka phle se koi route to present nhi hai(nhi hona chahiye coz is handler ki heading me likha hai ki ek train ka ek hi route hoga)
     const existingRoute = await prisma.route.findUnique({
          where: { trainId }
     })

     if (existingRoute) {          //if route already exist then throw error
          throw new ConflictError("Route already exists for this train")
     }

     const stationIds = stations.map((station) => station.stationId);         //is route ko create krne ke lie jo bhi stations mile hain unki stationId's ko ek jagah store krli
     //checking ki jo bhi stations diye hai route ke andar vo stations hamare db me present hai ki nhi
     const existingStations = await prisma.station.findMany({  
          //agar route me die gye staioins me se ek bhi statin hmare db me present nhi hoga(invalid hoga) to ye condition us station ko existingStations variable se hata degi  
          where: { id: { in: stationIds } }
     })
     if (existingStations.length !== stationIds.length) {           //incase ek bbhi station invalid hua to ye condition true ho jayegi and it will throw error
          throw new BadRequestError('One or more station IDs are invalid');
     }

     //ab hamarra kaam is to arrange these station wrt to their sequence
     const sorted = [...stations].sort((a, b) => a.sequenceNumber - b.sequenceNumber);
     for (let i = 0; i < sorted.length; i++) {            //ye check kr rha hai ki coontroller se jo bhi stations aye hai unki stationId continuous hai ya nhi 
                                                          //so that if contoller se sequence id ka sequence aye 1,3,4,5 to yha pe sequenceId 2 missing hai to we cant create route with these stations
          if (sorted[i].sequenceNumber !== i + 1) {         
               throw new BadRequestError('Sequence numbers must be continuous starting from 1');
          }
     }
     //ab agla kaam hai to create  the complete route  
     const route = await prisma.route.create({
          data: {
               trainId,
               routeStations: {         //ye routeStations able station se relaed data like kon se station ka konsa station number hai,
                                                                                          //id kya hai,
                                                                                          //us station ki origin se distance kya hai etc
                    create: stations.map((s) => ({
                         stationId: s.stationId,
                         sequenceNumber: s.sequenceNumber,
                         arrivalTime: s.arrivalTime || null,
                         departureTime: s.departureTime || null,
                         distanceFromOrigin: s.distanceFromOrigin || 0,
                    }))
               }
          },
          include: {
               routeStations: {             //iska mtlb hai hmm route ko jb return krenge to routestation table me jo bhi data hai is route ke lie use bhi return krenge --?DIDNT GET
                    include: { station: true },
                    orderBy: { sequenceNumber: 'asc' },
               },
          },
     });

     //we already have the trainId, so we are fetching that train from the database along with its associated seats.
     const trainWithSeats = await prisma.train.findUnique({          
          where: { id: trainId },
          include: {
                 seats: { 
                    orderBy: { seatNumber: 'asc' } 
                 }
          },
     });

     //Now we need to publish this createRoute event into kafka
     await adminProducer.publishRouteCreated({ ...route, train: trainWithSeats });            //publishes an event indicating that a route has been created.
     //DIDNOT GET WHAT THESE ABOVE 2 STEPS ARE COLLECTIVELY DOING

     return route;
};

const getAllTrains = async () => {
     return prisma.train.findMany({
          include: {
               seats: { orderBy: { seatNumber: 'asc' } },
               route: {
                    include: {
                         routeStations: {
                              include: { station: true },
                              orderBy: { sequenceNumber: 'asc' },
                         },
                    },
               },
          },
          orderBy: { trainNumber: 'asc' },
     });
};

const getTrainById = async (id) => {
     const train = await prisma.train.findUnique({
          where: { id },
          include: {
               //TO yha pr we are fetching all the details of thi sperticular train that are present in diffrent tables
               seats: { orderBy: { seatNumber: 'asc' } },
               route: { 
                    include: {
                         routeStations: {
                              include: { station: true },
                              orderBy: { sequenceNumber: 'asc' },
                         },
                    },
               },
          },
     });
     if (!train) throw new NotFoundError('Train not found');
     return train;
};
module.exports = { createTrain, createRoute, getAllTrains, getTrainById };