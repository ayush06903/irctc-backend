//elasticsearch.js creates and configures the Elasticsearch indexes, while search.service.js uses those indexes to store, update, and search data.
const { esClient, TRAIN_INDEX, STATION_INDEX } = require('../config/elasticsearch');
const logger = require('../config/logger');

// ═══════════════════════════════════════════════════
//  INDEX OPERATIONS (called by Kafka consumer)
// ═══════════════════════════════════════════════════

/**
 * When admin creates a station, index it for autocomplete.
 * Event shape: { eventType, data: { id, name, code, city, state }, timestamp }
 */
const indexStation = async (event) => {
                          //parsedValue event variable me cach krli
                          //is indexStation me jo hmne parsedValue catch kri hai  usme kya daata hoga vo vari krega ki admin service ki kis service ne data push kara hai agar train service ke create train function ne event push kra to it will contain data that we have passed in .publish insidde that function
     const station = event.data;
     if (!station) return;

     //incase event me data present hai
     try {
          //agar data milta hai to us data ko hmm elastic search me store krenge
          await esClient.index({
               index: STATION_INDEX,         //we are storing this data in STATION_INDEX
               id: station.id,              //TO IDENTIFY THIS STATION UNIQUELY WE guve id to it
               document: {
                    stationId: station.id,
                    name: station.name,
                    code: station.code,
                    city: station.city,
                    suggest: {             //this sugest provides us the feature of auto completion
                         input: [station.name, station.code, station.city].filter(Boolean),
                               //to jo auto completion feature hoga ye hoga on the basis of station name,station code and station city 
                         weight: 10,
                    },
               },
               refresh: true,         //ye refresh elasic search ko ye batata hai ki jaise hi is document index ki stationIndex me entry ho to turant ye searchable ho jaye
          });
          logger.info(`Indexed station ${station.name} (${station.code})`);
     } catch (err) {
          logger.error(`Failed to index station: ${err.message}`);
     }
};

/**
 * When admin creates a route, we get enriched payload with train+seats+routeStations.
 */
const indexTrainRoute = async (routeEvent) => {
                             //createRoute jo method hai inside train.service.js of admin servic usme we have published train+seats+routeStations. in kafka
     //All these parsedValue variables come from Kafka messages, but they can contain different data depending on which event was published.

     const { train, routeStations } = routeEvent;
          //hmne train,seats,routeStaion=routeEvent se train aur routeStation ko extract krlia 
     if (!train || !routeStations) return;

     //now we are dealing if data sahi se recieve hua hai
     const seatSummary = { total: 0, LOWER: 0, MIDDLE: 0, UPPER: 0, SIDE_LOWER: 0, SIDE_UPPER: 0 };            //setting default value to 0 for each type of seats
     //jo hamara payload aya hai usme jakr dekho in createRoute function in train.service.js usme dekho jo train ke form me hmne trainwithseats payload pass kra hai hai us trainwithseats ke andar hi hmne seats model/table ka refrence pass kr rkkha hai to jb hmne trainwithseats ko publish kara to seats bhi iske andar present hai
     (train.seats || []).forEach((s) => {
          //to ab yha pr hmm train ki saari seats ko iterate kr rhe hai aur us perticular type of seat ki quantity ke saath total seats ka count bhi increase kr rhe hain
          seatSummary.total++;
          if (seatSummary[s.seatType] !== undefined) seatSummary[s.seatType]++;
     });

     //ab we are creating the document jo hme index me store/push krna hai
     const doc = {
          trainId: train.id,
          trainNumber: train.trainNumber,
          trainName: train.trainName,
          route: routeStations.map((rs) => ({        //since as wee have ealier discussed ki trainIndex me we have include route ka data ur schedule and we know ki roue se relaed info we got in routeStation
               //to here we are traversing each station present in routeSation and extracttion all these info for each station present in route of that perticular train
               stationId: rs.station.id,
               stationName: rs.station.name,
               stationCode: rs.station.code,
               sequenceNumber: rs.sequenceNumber,
               arrivalTime: rs.arrivalTime,
               departureTime: rs.departureTime,
               distanceFromOrigin: rs.distanceFromOrigin,
          })),
          schedules: [],          //intitially is train ke schedule ko hmne empty rakkha hai
          seatSummary,
     };

     //ab elastic search me is doc ki entry create krni hai
     await esClient.index({
          index: TRAIN_INDEX,
          id: train.id,
          document: doc,
          refresh: true,
     });

     // ab hmm hmare routestation me jo bhi stations the unki entry krenge in our station index coz aisa hoskta hai ki rouesttation me koi aisa station present ho jo abhi hmare station index me present nhi hai
     for (const rs of routeStations) {
          await esClient.index({
               index: STATION_INDEX,
               id: rs.station.id,
               document: {
                    stationId: rs.station.id,
                    name: rs.station.name,
                    code: rs.station.code,
                    city: rs.station.city,
                    suggest: {
                         input: [rs.station.name, rs.station.code, rs.station.city].filter(Boolean),
                         weight: 10,
                    },
               },
               refresh: true,
          });
     }

     logger.info(`Indexed train ${train.trainNumber} with ${routeStations.length} stations`);
};

/**
 * When admin creates a schedule, add it to the train's schedules array.
 */
const indexSchedule = async (scheduleEvent) => {
     const { scheduleId, trainId, departureDate, status, seats } = scheduleEvent;

     const totalSeats = seats ? seats.length : 0;

     try {
          //since phle jb indexTrainRoute ke andar hmne train index me entry kri to scheddule ko hmne empty chhod dia tha(empty array)
          //to yha pr schedule ki entry ek baar hogyi hai ab hmm us entry me values dallenge so we are basically updating the entry whcih has already been created
          await esClient.update({
               index: TRAIN_INDEX,
               id: trainId,
               script: {
                    //ye script elastic search document ko update krdegi 
                    source: `
            if (ctx._source.schedules == null) { ctx._source.schedules = []; }
            // Remove existing schedule with same id (idempotent)
            ctx._source.schedules.removeIf(s -> s.scheduleId == params.scheduleId);
            ctx._source.schedules.add(params.newSchedule);
          `,
                    params: {
                         scheduleId,
                         newSchedule: {
                              scheduleId,
                              departureDate,
                              status,
                              available: totalSeats,
                              locked: 0,
                              booked: 0,
                         },
                    },
               },
               refresh: true,
          });
          logger.info(`Indexed schedule ${scheduleId} for train ${trainId}`);
     } catch (err) {
          logger.warn(`Could not index schedule for train ${trainId}: ${err.message}`);
     }
};

/**
 * When admin cancels a schedule, update its status in ES.
 * Event shape: { eventType, data: { id, trainId, status: 'CANCELLED', ... }, timestamp }
 */
const cancelSchedule = async (event) => {
     const schedule = event.data;
     if (!schedule) return;

     try {
          await esClient.update({
               index: TRAIN_INDEX,
               id: schedule.trainId,
               script: {
                    source: `
            if (ctx._source.schedules != null) {
              for (def s : ctx._source.schedules) {
                if (s.scheduleId == params.scheduleId) {
                  s.status = 'CANCELLED';
                }
              }
            }
          `,
                    params: { scheduleId: schedule.id },
               },
               refresh: true,
          });
          logger.info(`Cancelled schedule ${schedule.id} for train ${schedule.trainId}`);
     } catch (err) {
          logger.warn(`Could not cancel schedule: ${err.message}`);
     }
};

/**
 * When inventory changes (seat booked/released), update availability counts.
 */
const updateSeatAvailability = async (event) => {
     const { scheduleId, trainId, available, locked, booked } = event;

     try {
          await esClient.update({
               index: TRAIN_INDEX,
               id: trainId,
               script: {
                    source: `
            if (ctx._source.schedules != null) {
              for (def s : ctx._source.schedules) {
                if (s.scheduleId == params.scheduleId) {
                  s.available = params.available;
                  s.locked    = params.locked;
                  s.booked    = params.booked;
                }
              }
            }
          `,
                    params: { scheduleId, available: available || 0, locked: locked || 0, booked: booked || 0 },
               },
               refresh: true,
          });
          logger.info(`Updated availability for schedule ${scheduleId}`);
     } catch (err) {
          logger.warn(`Could not update availability: ${err.message}`);
     }
};

// ═══════════════════════════════════════════════════
//  SEARCH OPERATIONS (called by API)
// ═══════════════════════════════════════════════════

/**
 * Search trains running between two stations on a given date.
 * Supports fuzzy matching on station names.
 */
const searchTrains = async (from, to, date) => {
     //to in from/to me use station name,station code,city name teeno me se kch bhi de skta hai to use teeno me se kisi ka bhi use krkr search perform kre we need o write this method in such a way ki teeno cases me perfect search ho
     //also jo search controller me hmne from,to and date ki value req.params se fetch kri wha pr hmne bs value fetch krli hmko ye nhi pata chla ki user ne station name,code,city name likha hai kch bhi likh kr search kara ho we just fetched the details 
                     //so we will create a diffrent function jiska kaam hoga elastic search me searching krkr pata lagana ki jo user ne enter kara hai vo station name hai/code hai/city name hai and then return us whole data related to that station
     const fromStation = await resolveStation(from);
     const toStation = await resolveStation(to);

     if (!fromStation) return { trains: [], message: `Station "${from}" not found` };
     if (!toStation) return { trains: [], message: `Station "${to}" not found` };

     //if we are herre it means hmare paas source aur destinaion station ke documents/data available hain
     //ab we need to find ki kya koi aisi train hai jo source se destination tk jaati hai
                     //for this we need to checck 2 conditions
                                   //1->src aur destination dono present hone chahiye in a route 
                                   //2->src phle ana chahiye from destination
     const query = {
          bool: {
               must: [
                    {
                         nested: {
                              path: 'route',                  //route key hai which store all the station in route of train in the ffoem of array
                              query: { term: { 'route.stationId': fromStation.stationId } },         //route ki station id user ne jo fromStation provide kara hai uski stationId se barabar honi chahiye
                              inner_hits: { name: 'from_station' },          //inner_hits hme us train ka document provide krta hai jisne condition satisfy krdi ho
                         },
                    },
                    {
                         nested: {
                              path: 'route',
                              query: { term: { 'route.stationId': toStation.stationId } },
                              inner_hits: { name: 'to_station' },
                         },
                    },
               ],
          },
     };         //to ye jo query block hai it will find us the trains which have both fromSation and toStation in their route

     //to ab is query ko we will search in train index
     const result = await esClient.search({
          index: TRAIN_INDEX,
          query,
          size: 50,            //source aur destination cover krne wali max 50 trains ka data we will return
     });

     const normalize = (d) => new Date(d).toISOString().slice(0, 10);

     const trains = result.hits.hits
          .map((hit) => {
               const src = hit._source;      //isse traverse krte samyyy jis train pr currently hain uska document which contains all main info we fetched it and saved it in src variable
               const fromHit = hit.inner_hits.from_station.hits.hits[0]?._source;     //yha we are fetching jo user ne from location dali hai us station ka document
               const toHit = hit.inner_hits.to_station.hits.hits[0]?._source;           //to station ka document

               if (!fromHit || !toHit || fromHit.sequenceNumber >= toHit.sequenceNumber) {
                                        //mtlnb agr source station destination station ke baad me aa rha ho(fromhi ka sequence number bada hai from toHit ke sequence number se)
                    return null;
               }

               let scheduleInfo = null;
               if (date && src.schedules && src.schedules.length > 0) {
                    scheduleInfo = src.schedules.find(
                         (s) => s.status === 'ACTIVE' && normalize(s.departureDate) === date
                    ) || null;
               }

               return {
                    trainId: src.trainId,
                    trainNumber: src.trainNumber,
                    trainName: src.trainName,
                    // --- SEGMENT BOOKING: Added stationId and sequenceNumber to from/to for segment-aware booking ---
                    from: { name: fromHit.stationName, code: fromHit.stationCode, departure: fromHit.departureTime, stationId: fromHit.stationId, sequenceNumber: fromHit.sequenceNumber },
                    to: { name: toHit.stationName, code: toHit.stationCode, arrival: toHit.arrivalTime, stationId: toHit.stationId, sequenceNumber: toHit.sequenceNumber },
                    seatSummary: src.seatSummary,
                    schedule: scheduleInfo,
               };
          })
          .filter(Boolean);

     //returning data for frontend
     return {
          from: { resolved: fromStation.name, code: fromStation.code },
          to: { resolved: toStation.name, code: toStation.code },
          date: date || 'any',
          count: trains.length,
          trains,
     };
};

/**
 * Fuzzy-resolve a station name/code to its ID.
 * Three strategies: exact code → completion suggester → fuzzy match
 */
const resolveStation = async (input) => {
     // 1. Phle we check ki user ne code to provide nhi kiya?
     const exactResult = await esClient.search({
          index: STATION_INDEX,
          query: { term: { code: input.toUpperCase() } },      //(we are using code for searching->we defined code as keyword in elasticsearch.js while defining sation index)
          size: 1,
     });
     if (exactResult.hits.hits.length > 0) return exactResult.hits.hits[0]._source;
                  //ye hits.hits samjhne ke lie kibana na interface dekhna hoga ek baaar at timestamp 9:34:35

     // 2. Try completion suggester (handles typos like "dehli" → "Delhi")
     try {
          const suggestResult = await esClient.search({
               index: STATION_INDEX,
               suggest: {
                    station_suggest: {
                         prefix: input,         //prefix is the input entered by user
                         //to yha pr user ne jo input provide kara hmm uske basis pr station_index me searching krenge
                         completion: {
                              field: 'suggest',
                              fuzzy: { fuzziness: 'AUTO' },
                              size: 1,
                         },
                    },
               },
          });
          const options = suggestResult.suggest?.station_suggest?.[0]?.options || [];
          if (options.length > 0) return options[0]._source;
     } catch (err) {
          logger.warn(`Suggest fallback failed: ${err.message}`);
     }

     // 3. Fuzzy match on name
     const fuzzyResult = await esClient.search({
          index: STATION_INDEX,
          query: {
               //we are using multi_match coz hme station name aur city name dono ke lie dekhna hai
               multi_match: {
                    query: input,
                    fields: ['name', 'city'],        //elasticsearch.js me jo station index define kara tha uske andar hmne 'name' aur 'city' dono fields define kri thi..unhi fields ko we are using to perrform searching
                    fuzziness: 'AUTO',
                    prefix_length: 1,
               },
          },
          size: 1,
     });

     return fuzzyResult.hits.hits.length > 0 ? fuzzyResult.hits.hits[0]._source : null;
};

/**
 * Autocomplete station names as user types.
 */
const autocompleteStation = async (prefix) => {
     const result = await esClient.search({
          index: STATION_INDEX,
          suggest: {
               station_suggest: {
                    prefix,
                    completion: {
                         field: 'suggest',
                         fuzzy: { fuzziness: 'AUTO' },
                         size: 10,
                    },
               },
          },
     });

     const options = result.suggest.station_suggest[0]?.options || [];
     return options.map((o) => ({        //incase case user entered "ka" then we are returning all the staions jinka naam ka se stat hota ho?who have "ka" as prefix
          name: o._source.name,
          code: o._source.code,
          stationId: o._source.stationId,
     }));
};

/**
 * Debug: get all indexed stations
 */
const getAllStations = async () => {
     const result = await esClient.search({
          index: STATION_INDEX,
          query: { match_all: {} },
          size: 100,
     });
     return result.hits.hits.map((h) => h._source);
};

/**
 * Debug: get all indexed trains
 */
const getAllTrains = async () => {
     const result = await esClient.search({
          index: TRAIN_INDEX,
          query: { match_all: {} },
          size: 100,
     });
     return result.hits.hits.map((h) => h._source);
};

module.exports = {
     indexStation,
     indexTrainRoute,
     indexSchedule,
     cancelSchedule,
     updateSeatAvailability,
     searchTrains,
     autocompleteStation,
     getAllStations,
     getAllTrains,
};
