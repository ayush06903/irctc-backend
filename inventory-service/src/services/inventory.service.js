const prisma = require('../config/prisma');
const logger = require('../config/logger');
const inventoryProducer = require('../kafka/producer/inventory.producer');
const { retryTransaction } = require('../utils/retryTransaction');
const { withIdempotency, recordIdempotency } = require('../utils/idempotency');
const { BadRequestError, NotFoundError, ConflictError, ForbiddenError } = require('../utils/error');
const { config } = require('../config');

// ─── Kafka Event Handlers ───────────────────────────────────────────────────

const initializeInventory = async (eventData) => {
                               //ye jo eventData me jo data aa rha hai ye kafka me admin service ki schedule.service.js ne store kara hai
                               //aur specifically jo schedule created topic hai usko subscribe kra hai initializeInventory ne
     const { scheduleId, trainId, trainNumber, trainName, departureDate, seats } = eventData;

     if (!scheduleId || !seats || !seats.length) {
          logger.warn('Invalid SCHEDULE_CREATED event — missing scheduleId or seats');
          return;
     }

     const eventKey = `SCHEDULE_CREATED:${scheduleId}`;          

     //checking weather is schedule ke lie inventory phle se to exist nhi krti hai
     const existing = await prisma.idempotencyRecord.findUnique({ where: { eventKey } });
    
     if (existing) {         //incase yes then we need not create new inventory                           
          logger.info(`Duplicate event skipped: ${eventKey}`);
          return;
     }

     //if inventory phle se exist nhi krti hai to we need to create new inventory
     const totalSeats = seats.length;

                //yha pr hmne transaction use kara hai kyuki muliple tables me changes perform krne hai(scheduleInventory aur seatInventory) so we need to perform multiple db operation so ffor that we need rollback if any fails
                //AGAR EK SINGLE SERVICE KI MULTIPLE TABLES KE BEECH KI TRANSACTION MANAGE KRNE KE LIE WE USE THIS $transaction AUR JAB HME MULTIPLE SERVICES KE BEECH KI TRANSACTION KO MANAGE KRNA HOTA HAI THEN WE USE SAGA PATTERN JO KI HMNE BOOKING SERVICE ME USE KARA HAI
     await prisma.$transaction(async (tx) => {
          const schedule = await tx.scheduleInventory.create({
               data: {
                    scheduleId,
                    trainId,
                    trainNumber,
                    trainName,
                    departureDate: new Date(departureDate),
                    totalSeats,
                    available: totalSeats,
                    locked: 0,
                    booked: 0,
                    status: 'ACTIVE',
               },
          });

          //ab hme seatInventory table me entry krni hai foe seat management
          const seatData = seats.map(seat => ({
               scheduleInventoryId: schedule.id,
               scheduleId,
               seatId: seat.seatId,
               seatNumber: seat.seatNumber,
               seatType: seat.seatType,
               price: seat.price,
               status: 'AVAILABLE',
          }));

          await tx.seatInventory.createMany({ data: seatData });

          // --- SEGMENT BOOKING: Persist route topology for segment overlap checks ---
          //segment booking ka mtlb hai maan lo ek train ke routes me 7 stations hai aur passenger 1 ne seat number boook kri from station 1 to station 4 then when that passenger gets off the train on station 4 then that sea would be available for passengers boarding train in station 4 and would be bookable from that station onwards
          if (eventData.route && eventData.route.length > 0) {
              //first we check wheather route is present in eventData
               const routeStopData = eventData.route.map(rs => ({
                    //we use map on all the stations in the route and store the below info for all the stations in routeStopData variable
                    scheduleId,
                    stationId: rs.stationId,
                    stationName: rs.stationName,
                    stationCode: rs.stationCode,
                    sequenceNumber: rs.sequenceNumber,
               }));
               //routeStopData me stored saare stations ke stored data ki enry eksaath krdo in roouteStop table
               await tx.routeStop.createMany({ data: routeStopData });
               logger.info(`Persisted ${routeStopData.length} route stops for schedule ${scheduleId}`);
          }

          //also doing entry in idempotency wali table me which tells us ki hmne is schedule ke lie inventory create krdi hai
          await tx.idempotencyRecord.create({ data: { eventKey } });
     });

     logger.info(`Inventory initialized for schedule ${scheduleId} with ${totalSeats} seats`);

     //see 01.png-->ab hamara next kaam hai to manage consistency btw search and inventory service meaning inventory service me jo bhi change hua seat availability me vo same update we provide to search service as well 
     //we are doing so coz search service ka use krrkr user is finding train btw 2 stations and then user gets all the train data through search service so we want ki user ko search service se jb bhi train data mile vo jitna latest data mil skta hai user ko utna accha
     //we cant achieve strong consistency here but we can achieve eventual consistency
                          //to achieve this we are pushing seat-availability-updated event in kafka and then this search service seat-availability-updated event ko consume kr rhi hai and then it will update the station and trainIndex
     try {
                              //publishing event in kafka
          await inventoryProducer.publishSeatAvailabilityUpdated(scheduleId, trainId, totalSeats, 0, 0);
                                 //this event is defined in inventory.producer.js                                                               //bookedSeats,lockedSeats
     } catch (err) {
          logger.error('Failed to publish initial availability event after retries', { scheduleId, error: err.message });
     }
};

const cancelScheduleInventory = async (eventData) => {
     const data = eventData.data || eventData;
     const scheduleId = data.scheduleId || data.id;

     if (!scheduleId) {
          logger.warn('Invalid SCHEDULE_CANCELLED event — missing scheduleId');
          return;
     }

     //creating event key of this scheduleId coz we need to check in idempotency able weather we have already cancled the schedule corresponding to this scheduleId
     const eventKey = `SCHEDULE_CANCELLED:${scheduleId}`;                    //this is the diffrent eventKey that we are creating it will look something like "SCHEDULE_CANCELLED:SCH101"

     //checking in idempotencyRecord table about this schedule
     const existing = await prisma.idempotencyRecord.findUnique({ where: { eventKey } });
                                   //agar ye cancled wali eventKey for a scheduleId is already present in indempotencyRecord table then scheduleId corresponding to eventKey has already been cancled
                                   //the same idempotencyRecord table is being used for 2 diffrent purpous in function the eventkey is in the form const eventKey = SCHEDULE_CREATED:${scheduleId}; and its purpous is to not create new inventory for a scheduleId whose inventory has already been processed and infunction 2 our eventkey is in the for "SCHEDULE_CANCELLED:SCH101" which helps us to know weather the scheduleId that we are trying to cancle has already been cancled and if yes then this table idempotencyRecord will contain SCHEDULE_CANCELLED:SCH101 and we will satisfy
     if (existing) {   //incase ye eventkey already present hai then we need not to cancle it again
          logger.info(`Duplicate event skipped: ${eventKey}`);
          return;
     }

     //incase entry already exist nhi  krti hai we need to cancle this schedule
     const schedule = await prisma.scheduleInventory.findUnique({ where: { scheduleId } });            //finding the entry of this schedule from scheduleInventory table
     if (!schedule) {              //incase is scheduleId ke correspondin schedule scheduleInventory table me exist nhi krta hai to jo schedule exist hi nhi krta hai usjko delete kaise krenge
          logger.warn(`Schedule ${scheduleId} not found in inventory — skipping cancellation`);
          return;
     }

     //scheduleId me bhi schedule milgya to now we need to cancle the schedule that we found
           //we have used $transaction coz hme same service ki multiple tables ke records ko change krna hai
     await prisma.$transaction(async (tx) => {
          await tx.scheduleInventory.update({          //phle we updated enry in scheduleInventory table
               where: { scheduleId },
               data: { status: 'CANCELLED', available: 0, locked: 0, booked: 0, version: { increment: 1 } },
          });

          await tx.seatInventory.updateMany({          //we are doing this so that cancled schedule wale din these seats are not bookable by user
               where: { scheduleId },
               data: { status: 'CANCELLED' },
          });

          //creating entry of this eventKey in idempotencyRecord table so that agar hmm is schedule ko phir se cancle krne ki koshish kre to it gets handled
          await tx.idempotencyRecord.create({ data: { eventKey } });
     });

     logger.info(`Inventory cancelled for schedule ${scheduleId}`);

     //ab agla kaam hai to manage consistency btw search service and inventory service like we did in previous funcion
     try {
          await inventoryProducer.publishSeatAvailabilityUpdated(scheduleId, schedule.trainId, 0, 0, 0);
     } catch (err) {
          logger.error('Failed to publish cancellation availability event after retries', { scheduleId, error: err.message });
     }
};      //go to timestamp 10 hour 47 min for practical demo 

// ─── Segment Booking Helpers ─────────────────────────────────────────────────

/**
 * Recompute SeatInventory.status for a set of seats based on their current
 * SeatSegmentLock rows.  Each physical seat gets exactly ONE summary status:
 *   - AVAILABLE  → no active segment locks at all
 *   - LOCKED     → at least one LOCKED segment lock exists
 *   - BOOKED     → all active segment locks are BOOKED (none LOCKED)
 *
 * Returns the count of seats whose status actually changed to/from AVAILABLE
 * so the caller can decide whether aggregate counters need adjusting.
 */
async function recomputeSegmentSeatStatuses(tx, scheduleId, seatIds) {
     //is function ka logic is discussed at timestamp 13:46:40
     //but main logic ki outline hai hai ki hme seatSegmentLock table me jana hai aur us ek perticular seat ke lie jitne bhi segment locks present hai unko nikalmna hai us able se and then hme unme dekhna hai ki kya koi aisa segment lock hai jiska status locked hai AGAR HAI TO SEAT KA STATUS HMM LOCKED KRDENGE AUR AGAR SARE SEGMENT LOCKS KA STATUS 
     const statusChanges = { nowAvailable: 0, nowOccupied: 0, lockedToBooked: 0, bookedToLocked: 0 };

     for (const seatId of seatIds) {
          const locks = await tx.seatSegmentLock.findMany({
               where: { scheduleId, seatId, status: { in: ['LOCKED', 'BOOKED'] } },
               select: { status: true },
          });

          // Determine the correct summary status
          let newStatus;
          if (locks.length === 0) {
               newStatus = 'AVAILABLE';
          } else if (locks.some(l => l.status === 'LOCKED')) {
               newStatus = 'LOCKED';
          } else {
               newStatus = 'BOOKED';
          }

          // Read current status to detect transitions
          const current = await tx.$queryRaw`
               SELECT status FROM seat_inventories
               WHERE "scheduleId" = ${scheduleId} AND "seatId" = ${seatId}
               FOR UPDATE NOWAIT
          `;
          const oldStatus = current[0]?.status;

          if (oldStatus === newStatus) continue; // no change needed

          // Track transitions for aggregate counter adjustment
          if (oldStatus === 'AVAILABLE' && newStatus !== 'AVAILABLE') statusChanges.nowOccupied++;
          if (oldStatus !== 'AVAILABLE' && newStatus === 'AVAILABLE') statusChanges.nowAvailable++;
          if (oldStatus === 'LOCKED' && newStatus === 'BOOKED') statusChanges.lockedToBooked++;
          if (oldStatus === 'BOOKED' && newStatus === 'LOCKED') statusChanges.bookedToLocked++;

          // Update the summary row
          await tx.$executeRaw`
               UPDATE seat_inventories
               SET status = ${newStatus}::"SeatStatus",
                   "lockedBy" = CASE WHEN ${newStatus} = 'AVAILABLE' THEN NULL ELSE "lockedBy" END,
                   "lockedAt" = CASE WHEN ${newStatus} = 'AVAILABLE' THEN NULL ELSE "lockedAt" END,
                   "lockExpiresAt" = CASE WHEN ${newStatus} = 'AVAILABLE' THEN NULL ELSE "lockExpiresAt" END,
                   "bookingId" = CASE WHEN ${newStatus} = 'AVAILABLE' THEN NULL ELSE "bookingId" END,
                   version = version + 1, "updatedAt" = NOW()
               WHERE "scheduleId" = ${scheduleId} AND "seatId" = ${seatId}
          `;
     }

     return statusChanges;
}

/**
 * Recount schedule_inventories aggregate columns from actual seat_inventories rows.
 * This is the source of truth — avoids counter drift from arithmetic updates.
 */
async function recountScheduleAggregates(tx, scheduleId) {
     const counts = await tx.$queryRaw`
          SELECT
               COUNT(*) FILTER (WHERE status = 'AVAILABLE')::int AS available,
               COUNT(*) FILTER (WHERE status = 'LOCKED')::int AS locked,
               COUNT(*) FILTER (WHERE status = 'BOOKED')::int AS booked
          FROM seat_inventories
          WHERE "scheduleId" = ${scheduleId}
     `;

     const { available, locked, booked } = counts[0];

     await tx.$executeRaw`
          UPDATE schedule_inventories
          SET available = ${available}, locked = ${locked}, booked = ${booked},
              version = version + 1, "updatedAt" = NOW()
          WHERE "scheduleId" = ${scheduleId}
     `;

     return { available, locked, booked };
}

// ─── REST API Handlers ──────────────────────────────────────────────────────

const getAvailability = async (scheduleId) => {
                     //booking service ke inventoryClient.js ke andar is service ko call krte smay we passed scheduleId so here we have stored that scheduleId in scheduleId variable

     //jo scheduleId hmne recieve kri hai we will store all the data that we have correspoonding to the recieved scheduleId in scheduleInventory table
     const schedule = await prisma.scheduleInventory.findUnique({ where: { scheduleId } });
     //incase we did not have any data corresponding to the recieved scheduleId in our scheduleInventory table
     if (!schedule) throw new NotFoundError('Schedule not found in inventory');

     //incase we have a schedule stored corresponding to this scheduleId then we are just going to return these datails
     return {
          scheduleId: schedule.scheduleId,
          trainId: schedule.trainId,
          trainNumber: schedule.trainNumber,
          trainName: schedule.trainName,
          departureDate: schedule.departureDate,
          status: schedule.status,
          totalSeats: schedule.totalSeats,
          available: schedule.available,
          locked: schedule.locked,
          booked: schedule.booked,
     };
};

const getSeats = async (scheduleId, filters = {}) => {
     //similarly this is the method that return the seats according/wrt too the filters that user selected
     const schedule = await prisma.scheduleInventory.findUnique({ where: { scheduleId } });
     if (!schedule) throw new NotFoundError('Schedule not found in inventory');

     const where = { scheduleId };
     if (filters.status) where.status = filters.status;
     if (filters.seatType) where.seatType = filters.seatType;

     let seats = await prisma.seatInventory.findMany({
          where,
          orderBy: { seatNumber: 'asc' },
          select: {
               seatId: true,
               seatNumber: true,
               seatType: true,
               price: true,
               status: true,
               lockedBy: true,
               lockExpiresAt: true,
               bookingId: true,
          },
     });

     // --- SEGMENT BOOKING: If segment specified, compute per-seat segment availability ---
     if (filters.fromSeq && filters.toSeq) {
          const fromSeq = parseInt(filters.fromSeq);
          const toSeq = parseInt(filters.toSeq);

          // Find all active segment locks that overlap with the requested segment
          const overlappingLocks = await prisma.seatSegmentLock.findMany({
               where: {
                    scheduleId,
                    status: { in: ['LOCKED', 'BOOKED'] },
                    fromSeq: { lt: toSeq },   // overlap condition: a.from < b.to
                    toSeq: { gt: fromSeq },    // overlap condition: b.from < a.to
               },
               select: { seatId: true, status: true },
          });

          const blockedSeatIds = new Set(overlappingLocks.map(l => l.seatId));

          // --- SEGMENT BOOKING: Find seats that have ANY segment locks (to distinguish legacy bookings) ---
          const seatsWithAnyLock = await prisma.seatSegmentLock.findMany({
               where: { scheduleId, status: { in: ['LOCKED', 'BOOKED'] } },
               select: { seatId: true },
               distinct: ['seatId'],
          });
          const seatsWithLocks = new Set(seatsWithAnyLock.map(l => l.seatId));

          // Add segmentStatus: UNAVAILABLE if overlapping lock exists, or if seat is
          // BOOKED/LOCKED with no segment locks at all (legacy pre-segment booking).
          // AVAILABLE only if no overlapping locks AND (seat has segment locks OR seat is AVAILABLE).
          seats = seats.map(seat => {
               if (blockedSeatIds.has(seat.seatId)) {
                    return { ...seat, segmentStatus: 'UNAVAILABLE' };
               }
               // Legacy booking: seat is BOOKED/LOCKED but has no segment lock records
               // → treat as booked for entire journey (backward compat)
               if ((seat.status === 'BOOKED' || seat.status === 'LOCKED') && !seatsWithLocks.has(seat.seatId)) {
                    return { ...seat, segmentStatus: 'UNAVAILABLE' };
               }
               return { ...seat, segmentStatus: 'AVAILABLE' };
          });
     }

     return {
          scheduleId,
          totalSeats: schedule.totalSeats,
          seats,
     };
};

// --- SEGMENT BOOKING: Added fromSeq/toSeq params for segment-aware locking ---


const lockSeats = async (scheduleId, seatIds, userId, ttlSeconds, fromSeq, toSeq, idempotencyKey) => {            //is msethod ko we are calling in saga step 1 as we know saga step 1 me hme seats ko locck krna hai
     //so during saga step 1 booking service sends http post req to inventory service seats ko lock krne ke lie at that time ye lockSeats wala method call hota hai
     //go to timestamp 12 hour 50 minute for better understanding

     const ttl = Math.min(Math.max(ttlSeconds || config.LOCK_TTL_SECONDS, 60), 600);      //ye batayega ki hme seatInventory table me seat ka status Locked kitne time tak rkhna hai
     const lockExpiresAt = new Date(Date.now() + ttl * 1000);                //ttl batati hai ki kitne time baad lock expire hoga whearaas lockExpiresAt give us exact time jis time pr ye lock expire ho jayega

     const result = await withIdempotency(idempotencyKey, (eventKey) => retryTransaction(async () => {
          return prisma.$transaction(async (tx) => {
                       //$tranaction signifies ki is block ke andar jo bhi db operations honge vo atomic honge->ek bhi fail to rollback ho jayega
               // Verify schedule is active
               const schedule = await tx.scheduleInventory.findUnique({ where: { scheduleId } });          // we store schedduleId ke corresponding data ko store kara which is present in scheduleInvenory table 

               //agar hme is given scheduleId ke corresponding data nhi mila inside scheduleInventory
               if (!schedule) throw new NotFoundError('Schedule not found in inventory');
               //agar train us din chlegi tabhi seat book/lock kr skte hain aur agar us din train nhi chlegi to we simply throew the error
               if (schedule.status !== 'ACTIVE') throw new BadRequestError('Schedule is not active');

               //ab hamara next kaam hai seatInventory table me lock apply karna on seats jinki hme seatId's mili hai from booking service(jb booking service ne post req bheji to inventory service indicating ki in seatId's ke corresponding seats pr lock apply krddo)
               const seats = await tx.$queryRaw`
                    SELECT id, "seatId", "seatNumber", status, "lockedBy"
                    FROM seat_inventories
                    WHERE "scheduleId" = ${scheduleId}
                    AND "seatId" = ANY(${seatIds}::text[])
                    FOR UPDATE NOWAIT          
               `;
                       //ye jo FOR UPDATE NOWAIT hai ye ek exclusive lock jisse hmm exclusive row level lock aquire krte hain--> 13 hour 18 min
                       //row level locks me read non blocking hote hain(mtlb kisi usser B ka transaction already locked seat ki row ko read krskta hai) but write blocking hote hai(user B ka transaction us row ke data me change nhi kr skta jis row pr phle se hi lock applied ho by any other user's transaction)
                       //hmm in seats ko tabtak locked raakhenge jbtk 
                                              // 1->ya to user A ka transaction commit na hojaye
                                              // 2->rollback na ho jaye(jb ttl complete hojaye aur till then transaction commit nhi hui hogi to rollback hoga)

               // All seats must exist
               if (seats.length !== seatIds.length) {        //iska mtlb hai we have failed to apply lock on some seat that we were asssigned to
                    const foundIds = new Set(seats.map(s => s.seatId));           //foundId's me we are storing the seatId of seats jinpr hmne lock laga dia hai
                    const missing = seatIds.filter(id => !foundIds.has(id));      //isse jinpr hmm lock nhilaga paye vo mil jayegi(basicallly seatId-fooundId)
                    throw new NotFoundError(`Seats not found: ${missing.join(', ')}`);
               }

               // --- SEGMENT BOOKING: Check segment-level availability instead of full-journey ---
               //fromSeq->station from which user wants to start their journey
               //toSeq->station till which user wants to go to

               //ab hmm ye check krenge ki weather is segment ke lie seat available hai ki nhi
               if (fromSeq && toSeq) {
                    // Check for overlapping segment locks on any of the requested seats-->go to timestamp 13 hour 28 min 25 sec to understand about overrlapping
                    //see img/02.png--->ek train hai jisme 7 stations hai aur 2 users hai user A(trying to boook seat from station 2 to station 5) and user B(trying to book seat from station 1 to station 4) this is the example of overlapping segments... dono ne same time pr booking intialize kri but user B ki transaction phle processed ho gyi to user A ki boooking ko hmm decline krdenge
                    //to incase koi overlapping segments ka case arise hoga to is overlapping me likhi hui script se hme overlapping segment ka pata lg jayega
                    const overlapping = await tx.$queryRaw`
                         SELECT "seatId" FROM seat_segment_locks
                         WHERE "scheduleId" = ${scheduleId}
                         AND "seatId" = ANY(${seatIds}::text[])
                         AND status IN ('LOCKED', 'BOOKED')
                         AND "fromSeq" < ${toSeq}
                         AND "toSeq" > ${fromSeq}
                         FOR UPDATE NOWAIT
                    `;  //agar hme overlapping segment lock mila hai to iska mtlb hai ki jis segment ke lie user seat book krna chah rha hai us segment pr phle se hi present hai

                    if (overlapping.length > 0) {       
                         const blockedIds = [...new Set(overlapping.map(r => r.seatId))];      //is blockedIds me store hogi un ssari seats ki id jinko hmm lock nhi kr skte
                         throw new ConflictError(
                              `Seats already locked/booked for overlapping segment: ${blockedIds.join(', ')}`,
                              'SEATS_UNAVAILABLE'
                         );
                    }

                    //incase we didnot encounter any overlapping we move forward to lock the seats 
                    // Create segment lock rows for the requested segment
                    for (const seat of seats) {  
                         //yha pr we are traversing on the seats jinpr hmne seatInventory table me lock lagaya tha         
                         await tx.seatSegmentLock.create({           //next kaam hai seatSegmentLock table me entry create krna
                              data: {
                                   scheduleId,
                                   seatId: seat.seatId,
                                   fromSeq,         //ye fromSeq and toSeq bata rhe hai ki kis segment ke lie lock apply kara hai
                                   toSeq,
                                   status: 'LOCKED',
                                   lockedBy: userId,
                                   lockedAt: new Date(),
                                   lockExpiresAt,
                              },
                         });
                    }           //go to timestampt 13:46:35
                                //agla kaam hai to update the status of seat in seatInventory table(hmne phle lock krdi thi seat upr but status change nhi kara tha) and schedukeInventort able(jisme hmm ye store krte hai ki kitni seats available hai/locked hain) ko me data update krna

               } else {
                    // Fallback: full-journey lock (no segment) — all seats must be AVAILABLE
                    const unavailable = seats.filter(s => s.status !== 'AVAILABLE');         //ye aisi seats hai jinka koi na koi segment phle se booked hai hence cant be booked for full journey
                    if (unavailable.length > 0) {       //jo seats useer ne book krne ki req kri hai maan lo seatId 1,5,7 ab in 1,5,7 me se kisi seat ka status other than AVAILABLE hua to we cant move further and need to throw error
                         throw new ConflictError(
                              `Seats not available: ${unavailable.map(s => `seat #${s.seatNumber} is ${s.status}`).join(', ')}`,
                              'SEATS_UNAVAILABLE'
                         );
                    }
               }

               // --- SEGMENT BOOKING: Recompute seat statuses from segment locks ---
               if (fromSeq && toSeq) {
                    // Set lockedBy/lockedAt/lockExpiresAt on seats that didn't have it yet
                    const seatPkIds = seats.map(s => s.id);
                    //ye neeche likhi hui query hmari seatInventory table me jo jo seats locked hain un seats ke lie lockedBy,lockedAt,lockExpiresAt,updatedAt set krdegi
                    await tx.$executeRaw`
                         UPDATE seat_inventories
                         SET "lockedBy" = COALESCE("lockedBy", ${userId}),
                             "lockedAt" = COALESCE("lockedAt", NOW()),
                             "lockExpiresAt" = ${lockExpiresAt}::timestamp,
                             "updatedAt" = NOW()
                         WHERE id = ANY(${seatPkIds}::text[])
                    `;

                    // Recompute each seat's summary status from its segment locks
                    //we need to update seat ka status in seatInventory table-->to do so we are using recomputeSegmentSeatStatuses methos
                    const affectedSeatIds = seats.map(s => s.seatId);
                    await recomputeSegmentSeatStatuses(tx, scheduleId, affectedSeatIds);

                    //NEXT kaam hai scheduleInventory table me entries(AVAILABLE/LOCKED/BOOKED) ko update karna
                    const counts = await recountScheduleAggregates(tx, scheduleId);

                    const segmentResult = {
                         scheduleId,
                         trainId: schedule.trainId,
                         lockedSeats: seats.map(s => ({
                              seatId: s.seatId,
                              seatNumber: s.seatNumber,
                              lockExpiresAt,
                         })),
                         lockExpiresAt,
                         counts,
                    };

                    // Same transaction as the work: a rollback discards the key too.
                    await recordIdempotency(tx, eventKey, segmentResult);

                    return segmentResult;
               }

               //UPAR hmne segment locking ke lie logic likha ab neeche hmm full journey booking ke lie logic add krenge
               // Full-journey: unconditional lock (original fast path)
               //full journey ke lie hmm directly arithmatically seatInvenory table me changes kr skte hain we dont need to do write query which does this like we did above for segment booking--->coz ek seat ek single user ko hi assign hogi
               const seatPkIds = seats.map(s => s.id);
               //firstly jin bhi seats ki seatId hme mili hai(that user wants to book) unke status ko lock kara(before locking their status was available)
               await tx.$executeRaw`
                    UPDATE seat_inventories
                    SET status = 'LOCKED', "lockedBy" = ${userId},
                        "lockedAt" = NOW(), "lockExpiresAt" = ${lockExpiresAt}::timestamp,
                        version = version + 1, "updatedAt" = NOW()
                    WHERE id = ANY(${seatPkIds}::text[])
               `;

               //ab hamara agla kaaam hai scheduleInventory table me in entries(AVAILABLE/LOCKED) ko update krna
               await tx.$executeRaw`
                    UPDATE schedule_inventories
                    SET available = available - ${seats.length},
                        locked = locked + ${seats.length},
                        version = version + 1,
                        "updatedAt" = NOW()
                    WHERE "scheduleId" = ${scheduleId}
               `;

               const fullJourneyResult = {
                    //INCLUDE THE DATA RELATED TO SEATS THAT USER HAS BOOKED
                    scheduleId,
                    trainId: schedule.trainId,
                    lockedSeats: seats.map(s => ({
                         seatId: s.seatId,
                         seatNumber: s.seatNumber,
                         lockExpiresAt,
                    })),
                    lockExpiresAt,
                    counts: {
                         available: schedule.available - seats.length,
                         locked: schedule.locked + seats.length,
                         booked: schedule.booked,
                    },
               };

               await recordIdempotency(tx, eventKey, fullJourneyResult);

               return fullJourneyResult;
          }, { timeout: 10000 });
     }));



     // Publish availability update (fire and forget)
     //AB hamara next kaam hai to publish an event in kafka-->hamari search serviice is event ko consume kregi and then elastic search apne andar present jo indexes hain unme changes kregi/daa update kregi
     try {
          await inventoryProducer.publishSeatAvailabilityUpdated(
               result.scheduleId, result.trainId,
               result.counts.available, result.counts.locked, result.counts.booked
          );
     } catch (err) {
          logger.error('Failed to publish availability after lock', { scheduleId: result.scheduleId, error: err.message });
     }

     return result;
};


// --- SEGMENT BOOKING: Added fromSeq/toSeq params for segment-aware unlocking ---
const unlockSeats = async (scheduleId, seatIds, userId, fromSeq, toSeq, idempotencyKey) => {
     const result = await withIdempotency(idempotencyKey, (eventKey) => retryTransaction(async () => {
          return prisma.$transaction(async (tx) => {
               // Row-level lock
               const seats = await tx.$queryRaw`
                    SELECT id, "seatId", "seatNumber", status, "lockedBy"
                    FROM seat_inventories
                    WHERE "scheduleId" = ${scheduleId}
                    AND "seatId" = ANY(${seatIds}::text[])
                    FOR UPDATE NOWAIT
               `;

               if (seats.length !== seatIds.length) {
                    throw new NotFoundError('One or more seats not found');
               }

               // --- SEGMENT BOOKING: Segment-aware unlock ---
               if (fromSeq && toSeq) {
                    // Delete specific segment locks for this user/segment
                    await tx.$executeRaw`
                         DELETE FROM seat_segment_locks
                         WHERE "scheduleId" = ${scheduleId}
                         AND "seatId" = ANY(${seatIds}::text[])
                         AND "lockedBy" = ${userId}
                         AND "fromSeq" = ${fromSeq}
                         AND "toSeq" = ${toSeq}
                         AND status = 'LOCKED'
                    `;

                    // Recompute seat statuses + aggregates from actual segment lock state
                    const affectedSeatIds = seats.map(s => s.seatId);
                    await recomputeSegmentSeatStatuses(tx, scheduleId, affectedSeatIds);
                    const counts = await recountScheduleAggregates(tx, scheduleId);

                    const schedule = await tx.scheduleInventory.findUnique({ where: { scheduleId } });

                    const segmentResult = {
                         scheduleId,
                         trainId: schedule.trainId,
                         unlockedSeats: seats.map(s => s.seatId),
                         counts,
                    };

                    await recordIdempotency(tx, eventKey, segmentResult);

                    return segmentResult;
               }

               // Fallback: full-journey unlock (no segment params)
               // All must be LOCKED
               const notLocked = seats.filter(s => s.status !== 'LOCKED');
               if (notLocked.length > 0) {
                    throw new ConflictError(
                         `Seats not in LOCKED status: ${notLocked.map(s => `seat #${s.seatNumber} is ${s.status}`).join(', ')}`
                    );
               }

               // All must be locked by this user
               const notOwnedByUser = seats.filter(s => s.lockedBy !== userId);
               if (notOwnedByUser.length > 0) {
                    throw new ForbiddenError('Some seats are not locked by you');
               }

               // Unlock
               const seatPkIds = seats.map(s => s.id);
               await tx.$executeRaw`
                    UPDATE seat_inventories
                    SET status = 'AVAILABLE', "lockedBy" = NULL,
                        "lockedAt" = NULL, "lockExpiresAt" = NULL,
                        version = version + 1, "updatedAt" = NOW()
                    WHERE id = ANY(${seatPkIds}::text[])
               `;

               const schedule = await tx.scheduleInventory.findUnique({ where: { scheduleId } });

               await tx.$executeRaw`
                    UPDATE schedule_inventories
                    SET available = available + ${seats.length},
                        locked = locked - ${seats.length},
                        version = version + 1,
                        "updatedAt" = NOW()
                    WHERE "scheduleId" = ${scheduleId}
               `;

               const fullJourneyResult = {
                    scheduleId,
                    trainId: schedule.trainId,
                    unlockedSeats: seats.map(s => s.seatId),
                    counts: {
                         available: schedule.available + seats.length,
                         locked: schedule.locked - seats.length,
                         booked: schedule.booked,
                    },
               };

               await recordIdempotency(tx, eventKey, fullJourneyResult);

               return fullJourneyResult;
          }, { timeout: 10000 });
     }));

     try {
          await inventoryProducer.publishSeatAvailabilityUpdated(
               result.scheduleId, result.trainId,
               result.counts.available, result.counts.locked, result.counts.booked
          );
     } catch (err) {
          logger.error('Failed to publish availability after unlock', { scheduleId: result.scheduleId, error: err.message });
     }

     return result;
};

// --- SEGMENT BOOKING: Added fromSeq/toSeq params for segment-aware confirmation ---
const confirmSeats = async (scheduleId, seatIds, userId, bookingId, fromSeq, toSeq, idempotencyKey) => {
     const result = await withIdempotency(idempotencyKey, (eventKey) => retryTransaction(async () => {
          return prisma.$transaction(async (tx) => {
               const seats = await tx.$queryRaw`
                    SELECT id, "seatId", "seatNumber", status, "lockedBy"
                    FROM seat_inventories
                    WHERE "scheduleId" = ${scheduleId}
                    AND "seatId" = ANY(${seatIds}::text[])
                    FOR UPDATE NOWAIT
               `;

               if (seats.length !== seatIds.length) {
                    throw new NotFoundError('One or more seats not found');
               }

               // --- SEGMENT BOOKING: Confirm segment locks if segment params provided ---
               if (fromSeq && toSeq) {
                    // Transition segment lock rows from LOCKED → BOOKED
                    const updated = await tx.$executeRaw`
                         UPDATE seat_segment_locks
                         SET status = 'BOOKED', "bookingId" = ${bookingId},
                             "lockExpiresAt" = NULL,
                             version = version + 1, "updatedAt" = NOW()
                         WHERE "scheduleId" = ${scheduleId}
                         AND "seatId" = ANY(${seatIds}::text[])
                         AND "lockedBy" = ${userId}
                         AND "fromSeq" = ${fromSeq}
                         AND "toSeq" = ${toSeq}
                         AND status = 'LOCKED'
                    `;

                    if (updated === 0) {
                         throw new ConflictError(
                              'Segment lock expired or not found. Please lock seats again.',
                              'LOCK_EXPIRED'
                         );
                    }
               } else {
                    // Fallback: full-journey confirmation checks
                    const notLocked = seats.filter(s => s.status !== 'LOCKED');
                    if (notLocked.length > 0) {
                         throw new ConflictError(
                              'Lock expired or seats not in LOCKED status. Please lock seats again.',
                              'LOCK_EXPIRED'
                         );
                    }

                    const notOwnedByUser = seats.filter(s => s.lockedBy !== userId);
                    if (notOwnedByUser.length > 0) {
                         throw new ForbiddenError('Some seats are not locked by you');
                    }
               }

               // --- SEGMENT BOOKING: Recompute seat statuses after segment lock transition ---
               if (fromSeq && toSeq) {
                    const affectedSeatIds = seats.map(s => s.seatId);
                    await recomputeSegmentSeatStatuses(tx, scheduleId, affectedSeatIds);
                    const counts = await recountScheduleAggregates(tx, scheduleId);

                    const schedule = await tx.scheduleInventory.findUnique({ where: { scheduleId } });

                    const segmentResult = {
                         scheduleId,
                         trainId: schedule.trainId,
                         bookingId,
                         confirmedSeats: seats.map(s => ({
                              seatId: s.seatId,
                              seatNumber: s.seatNumber,
                              status: 'BOOKED',
                         })),
                         counts,
                    };

                    await recordIdempotency(tx, eventKey, segmentResult);

                    return segmentResult;
               }

               // Full-journey: unconditional confirm (original fast path)
               const seatPkIds = seats.map(s => s.id);
               await tx.$executeRaw`
                    UPDATE seat_inventories
                    SET status = 'BOOKED', "bookingId" = ${bookingId},
                        "lockExpiresAt" = NULL,
                        version = version + 1, "updatedAt" = NOW()
                    WHERE id = ANY(${seatPkIds}::text[])
               `;

               const schedule = await tx.scheduleInventory.findUnique({ where: { scheduleId } });

               await tx.$executeRaw`
                    UPDATE schedule_inventories
                    SET locked = locked - ${seats.length},
                        booked = booked + ${seats.length},
                        version = version + 1,
                        "updatedAt" = NOW()
                    WHERE "scheduleId" = ${scheduleId}
               `;

               const fullJourneyResult = {
                    scheduleId,
                    trainId: schedule.trainId,
                    bookingId,
                    confirmedSeats: seats.map(s => ({
                         seatId: s.seatId,
                         seatNumber: s.seatNumber,
                         status: 'BOOKED',
                    })),
                    counts: {
                         available: schedule.available,
                         locked: schedule.locked - seats.length,
                         booked: schedule.booked + seats.length,
                    },
               };

               await recordIdempotency(tx, eventKey, fullJourneyResult);

               return fullJourneyResult;
          }, { timeout: 10000 });
     }));

     try {
          await inventoryProducer.publishSeatAvailabilityUpdated(
               result.scheduleId, result.trainId,
               result.counts.available, result.counts.locked, result.counts.booked
          );
     } catch (err) {
          logger.error('Failed to publish availability after confirm', { scheduleId: result.scheduleId, error: err.message });
     }

     return result;
};

const cancelBooking = async (scheduleId, bookingId, userId, idempotencyKey) => {
     const result = await withIdempotency(idempotencyKey, (eventKey) => retryTransaction(async () => {
          return prisma.$transaction(async (tx) => {
               // --- SEGMENT BOOKING: Check segment locks FIRST ---
               // For segment bookings, seat_inventories.bookingId is not set
               // (only seat_segment_locks has the bookingId), so we must check here first.
               const segmentLocks = await tx.seatSegmentLock.findMany({
                    where: { scheduleId, bookingId, status: 'BOOKED' },
               });

               if (segmentLocks.length > 0) {
                    await tx.$executeRaw`
                         DELETE FROM seat_segment_locks
                         WHERE "scheduleId" = ${scheduleId}
                         AND "bookingId" = ${bookingId}
                    `;

                    // Recompute seat statuses + aggregates from actual segment lock state
                    const affectedSeatIds = [...new Set(segmentLocks.map(l => l.seatId))];
                    await recomputeSegmentSeatStatuses(tx, scheduleId, affectedSeatIds);
                    const counts = await recountScheduleAggregates(tx, scheduleId);

                    const schedule = await tx.scheduleInventory.findUnique({ where: { scheduleId } });

                    const segmentResult = {
                         scheduleId,
                         trainId: schedule.trainId,
                         bookingId,
                         releasedSeats: affectedSeatIds,
                         counts,
                    };

                    await recordIdempotency(tx, eventKey, segmentResult);

                    return segmentResult;
               }
               // --- END SEGMENT BOOKING ---

               // Fallback: full-journey cancel (no segment locks found)
               const seats = await tx.$queryRaw`
                    SELECT id, "seatId", "seatNumber", status, "lockedBy"
                    FROM seat_inventories
                    WHERE "scheduleId" = ${scheduleId}
                    AND "bookingId" = ${bookingId}
                    AND status = 'BOOKED'
                    FOR UPDATE NOWAIT
               `;

               if (seats.length === 0) {
                    throw new NotFoundError('No booked seats found for this booking');
               }

               const seatPkIds = seats.map(s => s.id);
               await tx.$executeRaw`
                    UPDATE seat_inventories
                    SET status = 'AVAILABLE', "lockedBy" = NULL,
                        "lockedAt" = NULL, "lockExpiresAt" = NULL, "bookingId" = NULL,
                        version = version + 1, "updatedAt" = NOW()
                    WHERE id = ANY(${seatPkIds}::text[])
               `;

               const schedule = await tx.scheduleInventory.findUnique({ where: { scheduleId } });

               await tx.$executeRaw`
                    UPDATE schedule_inventories
                    SET available = available + ${seats.length},
                        booked = booked - ${seats.length},
                        version = version + 1,
                        "updatedAt" = NOW()
                    WHERE "scheduleId" = ${scheduleId}
               `;

               const fullJourneyResult = {
                    scheduleId,
                    trainId: schedule.trainId,
                    bookingId,
                    releasedSeats: seats.map(s => s.seatId),
                    counts: {
                         available: schedule.available + seats.length,
                         locked: schedule.locked,
                         booked: schedule.booked - seats.length,
                    },
               };

               await recordIdempotency(tx, eventKey, fullJourneyResult);

               return fullJourneyResult;
          }, { timeout: 10000 });
     }));

     try {
          await inventoryProducer.publishSeatAvailabilityUpdated(
               result.scheduleId, result.trainId,
               result.counts.available, result.counts.locked, result.counts.booked
          );
     } catch (err) {
          logger.error('Failed to publish availability after cancel-booking', { scheduleId: result.scheduleId, error: err.message });
     }

     return result;
};

// ─── Lock Expiry Helper (used by lockExpiry.js) ─────────────────────────────

const recountAndPublish = async (scheduleId) => {
     const counts = await prisma.$queryRaw`
          SELECT
               COUNT(*) FILTER (WHERE status = 'AVAILABLE')::int AS available,
               COUNT(*) FILTER (WHERE status = 'LOCKED')::int AS locked,
               COUNT(*) FILTER (WHERE status = 'BOOKED')::int AS booked
          FROM seat_inventories
          WHERE "scheduleId" = ${scheduleId}
     `;

     const { available, locked, booked } = counts[0];

     const schedule = await prisma.scheduleInventory.update({
          where: { scheduleId },
          data: { available, locked, booked, version: { increment: 1 } },
     });

     try {
          await inventoryProducer.publishSeatAvailabilityUpdated(scheduleId, schedule.trainId, available, locked, booked);
     } catch (err) {
          logger.error('Failed to publish availability after recount', { scheduleId, error: err.message });
     }

     return { available, locked, booked };
};

module.exports = {
     initializeInventory,
     cancelScheduleInventory,
     getAvailability,
     getSeats,
     lockSeats,
     unlockSeats,
     confirmSeats,
     cancelBooking,
     recountAndPublish,
     recomputeSegmentSeatStatuses,
};
