const prisma = require('../config/prisma');
const logger = require('../config/logger');
const { inventoryClient } = require('./inventoryClient');
const { paymentClient } = require('./paymentClient');

/**
 * Saga orchestrator for booking lifecycle.
 * Each step is logged to SagaLog for auditability and crash recovery.
 *
 * Forward flow: HOLD_SEATS -> CREATE_PAYMENT -> CONFIRM_SEATS -> COMPLETE
 * Compensation: reverse order of completed steps
 */

// ─── Forward Steps ───────────────────────────────────────────────────────────

// --- SEGMENT BOOKING: Added fromSeq/toSeq params ---
async function executeHoldSeats(booking, seatIds, ttlSeconds, fromSeq, toSeq) {
     //is record ko manage krne ke lie we do the entry in sagalog table
     const sagaLog = await prisma.sagaLog.create({
          data: {
               bookingId: booking.id,
               step: 'HOLD_SEATS',
               status: 'PENDING',
                     //once we will lock the seats in inventory this saga step(HOLD_SEATS) will be done/completed so the status of this saga step would be completed....so each saga step has its own status
               request: { scheduleId: booking.scheduleId, seatIds, userId: booking.userId, ttlSeconds, fromSeq, toSeq }, // --- SEGMENT BOOKING
          },
     });

     //next kaam hai inventory me jakr seats ko hold krna
     try {
          const result = await inventoryClient.holdSeats(
                              //is inventoryClient.js ki holdSeats methodd me we send a http req to inventory  service and inventory service ha its own holdSeats method which will hold/lock the seat
               booking.scheduleId,
               seatIds,
               booking.userId,
               ttlSeconds,
               fromSeq,  // --- SEGMENT BOOKING
               toSeq,    // --- SEGMENT BOOKING
               booking.id // idempotency: stable across retries of this hold
          );

          //updating sagalog wali table
          await prisma.sagaLog.update({
               where: { id: sagaLog.id },
               data: { status: 'COMPLETED', response: result },
                          //we change the status to completed signifying ki hmne successfully seat ko lock krdia hai
          });

          //booking table ka status abhi pending hai we will convert that status to SEATS_HELD
          await prisma.booking.update({
               where: { id: booking.id },
               data: { status: 'SEATS_HELD' },
          });

          logger.info(`Saga HOLD_SEATS completed for booking ${booking.id}`);
          return result;           //return krdiya mtlb phla saga step complete ho gya

     } catch (error) {
          const errorMsg = error.response?.data?.message || error.message;
          await prisma.sagaLog.update({
               where: { id: sagaLog.id },
               data: { status: 'FAILED', error: errorMsg },
          });
          throw error;
     }
}

async function executeCreatePayment(booking) {
     const idempotencyKey = `${booking.id}-payment`;            //created idempotencyy key so that user duplicate payment se bache

     //we created entry for this payment in sagalog table
     const sagaLog = await prisma.sagaLog.create({
          data: {
               bookingId: booking.id,
               step: 'CREATE_PAYMENT',
               status: 'PENDING',         //coz we havn yet created payment order
               request: { bookingId: booking.id, amount: booking.totalAmount, userId: booking.userId },
          },
     });

     try {
          const result = await paymentClient.createPaymentOrder(
               booking.id,
               booking.totalAmount,
               booking.userId,
               idempotencyKey
          );

          await prisma.sagaLog.update({
               where: { id: sagaLog.id },
               data: { status: 'COMPLETED', response: result },
          });

          await prisma.booking.update({
               where: { id: booking.id },
               data: {
                    status: 'PAYMENT_PENDING',
                    paymentOrderId: result.paymentOrderId,
               },
          });

          logger.info(`Saga CREATE_PAYMENT completed for booking ${booking.id}`);
          return result;

     } catch (error) {
          const errorMsg = error.response?.data?.message || error.message;
          await prisma.sagaLog.update({
               where: { id: sagaLog.id },
               data: { status: 'FAILED', error: errorMsg },
          });
          throw error;
     }
}

// --- SEGMENT BOOKING: Added fromSeq/toSeq params ---
async function executeConfirmSeats(booking, seatIds, fromSeq, toSeq) {
     const sagaLog = await prisma.sagaLog.create({
          data: {
               bookingId: booking.id,
               step: 'CONFIRM_SEATS',
               status: 'PENDING',
               request: { scheduleId: booking.scheduleId, seatIds, userId: booking.userId, bookingId: booking.id, fromSeq, toSeq }, // --- SEGMENT BOOKING
          },
     });

     try {
          const result = await inventoryClient.confirmSeats(
               booking.scheduleId,
               seatIds,
               booking.userId,
               booking.id,
               fromSeq,  // --- SEGMENT BOOKING
               toSeq     // --- SEGMENT BOOKING
          );

          await prisma.sagaLog.update({
               where: { id: sagaLog.id },
               data: { status: 'COMPLETED', response: result },
          });

          logger.info(`Saga CONFIRM_SEATS completed for booking ${booking.id}`);
          return result;

     } catch (error) {
          const errorMsg = error.response?.data?.message || error.message;
          await prisma.sagaLog.update({
               where: { id: sagaLog.id },
               data: { status: 'FAILED', error: errorMsg },
          });
          throw error;
     }
}

// ─── Compensation Steps ──────────────────────────────────────────────────────

async function compensateHoldSeats(booking, seatIds) {
     logger.info(`Compensating HOLD_SEATS for booking ${booking.id}`);
     try {
          // --- SEGMENT BOOKING: Pass segment params from booking for accurate compensation ---
          await inventoryClient.releaseSeats(booking.scheduleId, seatIds, booking.userId, booking.fromSeq, booking.toSeq, booking.id);

          // Mark saga step as compensated
          await prisma.sagaLog.updateMany({
               where: { bookingId: booking.id, step: 'HOLD_SEATS', status: 'COMPLETED' },
               data: { status: 'COMPENSATED' },
          });
     } catch (error) {
          logger.error(`Failed to compensate HOLD_SEATS for booking ${booking.id}`, {
               error: error.message,
          });
          // Inventory lock expiry will eventually clean this up
     }
}

async function compensateCreatePayment(booking) {
     if (!booking.paymentOrderId) return;

     logger.info(`Compensating CREATE_PAYMENT for booking ${booking.id}`);
     try {
          const idempotencyKey = `${booking.id}-refund-compensation`;
          await paymentClient.initiateRefund(
               booking.paymentOrderId,
               booking.totalAmount,
               'booking_compensation',
               idempotencyKey
          );

          await prisma.sagaLog.updateMany({
               where: { bookingId: booking.id, step: 'CREATE_PAYMENT', status: 'COMPLETED' },
               data: { status: 'COMPENSATED' },
          });
     } catch (error) {
          logger.error(`Failed to compensate CREATE_PAYMENT for booking ${booking.id}`, {
               error: error.message,
          });
     }
}

async function compensateConfirmSeats(booking) {
     logger.info(`Compensating CONFIRM_SEATS for booking ${booking.id}`);
     try {
          await inventoryClient.cancelBooking(booking.scheduleId, booking.id, booking.userId);

          await prisma.sagaLog.updateMany({
               where: { bookingId: booking.id, step: 'CONFIRM_SEATS', status: 'COMPLETED' },
               data: { status: 'COMPENSATED' },
          });
     } catch (error) {
          logger.error(`Failed to compensate CONFIRM_SEATS for booking ${booking.id}`, {
               error: error.message,
          });
     }
}

/**
 * Compensate all completed saga steps in reverse order.
 * Used when a booking needs to be rolled back (failure, timeout, cancellation).
 */
async function compensateAll(booking, seatIds) {
     //we will take all the steps of booking from sagaLog db ->all those steps whose status is completed just to figure out from where we have to rollback
     const completedSteps = await prisma.sagaLog.findMany({
          where: { bookingId: booking.id, status: 'COMPLETED' },
          orderBy: { createdAt: 'desc' },
     });

     for (const step of completedSteps) {
          switch (step.step) {
               case 'CONFIRM_SEATS':
                    //this was our third saga step
                    await compensateConfirmSeats(booking);
                    break;
               case 'CREATE_PAYMENT':
                    //this was our second saga step
                    await compensateCreatePayment(booking);
                    break;
               case 'HOLD_SEATS':
                    //this was our firs saga step-->agar hmara case HOLD_SEATS hai to iska mtlb hai is wale saga step ke baad hamara transaction fail hua hai and is ROLL_BACK wale saga step me hmne seats ko lock kra tha.. to jb hmm roll back krenge to in seats ko hmm release krenge to ye compensateReleaseSeats method seats ko release krta hai
                    await compensateHoldSeats(booking, seatIds);
                    break;
          }
     }
}

module.exports = {
     executeHoldSeats,
     executeCreatePayment,
     executeConfirmSeats,
     compensateHoldSeats,
     compensateCreatePayment,
     compensateConfirmSeats,
     compensateAll,
};
