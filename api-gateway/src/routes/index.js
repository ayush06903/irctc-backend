const express = require('express');
const { requireAuth } = require('../middlewares/auth.middleware');  //THIS WILL AUTHENTICATE ACCESS TOKEN
const { createProxy, getCircuitBreakerStatus } = require('../services/proxy');
const { ipRateLimit, endpointRateLimit, combinedRateLimit } = require('../middlewares/rateLimiting.middleware')         //YE RATE LIMITIN GSRATEGES HAIN THAT WE HAVE DEFINED IN rateLimiing.middleware.js
const { config } = require('../config');

const router = express.Router();

//see 01.png-->we lready seen in user service about private and public routes.... 
              //we know ki there are two types of token 1-ACCESS TOKEN 
              //                                        2-> REFRESH TOKEN
              // to whenever we req to private route for each req we send our access token via cookie and then server verifies tthat access token and in case it is valid then only that route is accessabble to the user-->AUR ACCESS TOKEN KO VERIFY KRNE KA CODE HMNE auth.middleware.js ME LIKHA HAI
    

//PHLE WE WILL UNDERSTAND THE NEED OF API GATEWAY-->lets say client ko ko koi private route ko access krna hai like /profile(fech prfile) jo ki hai ek private route which we cant fetch without doing login(so we will have to verify the access token before processing the /profile route)
           //see 02.png--> user ne /profile route pr req bheji and then user ne via cookie access token bheja aur vo /profile req gyi user service ke andar jo /profile route define kara hai and we did put authentication procedure in auth.middleware.js indide user-service/src/middlewares
           //see 03.png--> ab we also have /booking(private route) which gives user detail about ticket booking to ye req gyi booking service me and since this is also a private route user has to be logged in and we must put a check in booking service which authenticaticates the user before processing the route to hmko booking service me bhi auth.middleware.js add krna hoga and attach it to /booking route

           //BUT WHAT IF WE HAVE 20 SUCH SERVICES KYA HMM HAR SERVICE ME auth.middleware.js KO DEFINE KRENGE--NO rather we will put this auth.middleware.js in API GATEWAY too whenwver client sends a req to any service then the client will send its access token to the api gateway and then api gateway will authenticate tthe client and incase user successfuly authenticate hojayega to api gateway will route the client to the correct endpoint of service it had requested
           //see 05.png__.api gate way ke doosre functions bhi lis kre hai and we will implement them all




// ===========================
// Service Proxy Routes
// ===========================

/**
 * USER SERVICE ROUTES
 * Gateway Path: /api/users/auth/login
 * Service Path: /auth/login
**/
                        
                        //ye jo createProxy function hai ye actual middleware hoga jo incoming requestts ko handle karega and match krega correct route ko correct service se
const userServiceProxy = createProxy('userService', config.SERVICES.USER_SERVICE_URL);

// public routes
router.post(
     '/users/auth/send-otp',        //send-otp.verify-otp,google-auth they all come underr auth routes
     endpointRateLimit(5, 3600000), // 5 requests per hour
     userServiceProxy
);

router.post(
     '/users/auth/verify-otp',
     endpointRateLimit(10, 3600000), // 10 requests per hour 
     //endpointRateLimit is the rate limiting strategy that we are using 
     userServiceProxy
);

router.post(
     '/users/auth/login',
     endpointRateLimit(100, 900000),// 100 requests per 15 minutes
     userServiceProxy
);

router.post(
     '/users/auth/google-auth',
     endpointRateLimit(10, 900000), // 10 requests per 15 minutes
     userServiceProxy
);

router.post(
     '/users/auth/refresh',
     endpointRateLimit(20, 900000), // 20 requests per 15 minutes
     userServiceProxy
);

// private routes
router.get(
     '/users/user/profile',
     requireAuth,                 //since private routes kwe lie we first need to authenicate the user to we have passed the requireAuth function that we defined in imported auth.middleware.js file
     combinedRateLimit(),
     userServiceProxy
)

router.put(
     '/users/user/profile',
     requireAuth,
     combinedRateLimit(),
     userServiceProxy
)

router.delete(
     '/users/user/profile',
     requireAuth,
     combinedRateLimit(),
     userServiceProxy
)

const adminServiceProxy = createProxy('adminService', config.SERVICES.ADMIN_SERVICE_URL);

router.post(
     '/admins/stations/station',
     requireAuth,
     combinedRateLimit(),
     adminServiceProxy
);

router.post(
     '/admins/trains/train',
     requireAuth,
     combinedRateLimit(),
     adminServiceProxy
);

router.post(
     '/admins/trains/route',
     requireAuth,
     combinedRateLimit(),
     adminServiceProxy
)

router.post(
     '/admins/schedules/schedule',
     requireAuth,
     combinedRateLimit(),
     adminServiceProxy
)

router.get(
     '/admins/stations/station',
     requireAuth,
     combinedRateLimit(),
     adminServiceProxy
)

router.get(
     '/admins/trains/train/:trainId',
     requireAuth,
     combinedRateLimit(),
     adminServiceProxy
);

router.put(
     '/admins/schedules/schedule/:scheduleId',
     requireAuth,
     combinedRateLimit(),
     adminServiceProxy
)
// ===========================
// SEARCH SERVICE ROUTES (public - no auth required)
// ===========================
const searchServiceProxy = createProxy('searchService', config.SERVICES.SEARCH_SERVICE_URL);

router.get(
     '/search/trains',
     endpointRateLimit(60, 60000), // 60 requests per minute
     searchServiceProxy
);

router.get(
     '/search/autocomplete',
     endpointRateLimit(120, 60000), // 120 requests per minute
     searchServiceProxy
);

// ===========================
// INVENTORY SERVICE ROUTES (public read-only)
// ===========================
const inventoryServiceProxy = createProxy('inventoryService', config.SERVICES.INVENTORY_SERVICE_URL);

// Public: aggregate availability (used by search results)
router.get(
     '/inventory/schedules/:scheduleId/availability',
     endpointRateLimit(120, 60000), // 120 requests per minute
     inventoryServiceProxy
);

// Authenticated: individual seat statuses
router.get(
     '/inventory/schedules/:scheduleId/seats',
     requireAuth,
     combinedRateLimit(),
     inventoryServiceProxy
);

// Note: lock/unlock/confirm/cancel-booking are now internal-only
// (called by booking-service directly, not through the gateway)

// ===========================
// BOOKING SERVICE ROUTES
// ===========================
const bookingServiceProxy = createProxy('bookingService', config.SERVICES.BOOKING_SERVICE_URL);

router.post(
     '/bookings/bookings',
     requireAuth,
     endpointRateLimit(5, 60000), // 5 booking attempts per minute
     bookingServiceProxy
);

router.get(
     '/bookings/bookings',
     requireAuth,
     combinedRateLimit(),
     bookingServiceProxy
);

router.get(
     '/bookings/bookings/:bookingId',
     requireAuth,
     combinedRateLimit(),
     bookingServiceProxy
);

router.post(
     '/bookings/bookings/:bookingId/verify-payment',
     requireAuth,
     combinedRateLimit(),
     bookingServiceProxy
);

router.post(
     '/bookings/bookings/:bookingId/cancel',
     requireAuth,
     combinedRateLimit(),
     bookingServiceProxy
);

// ===========================
// PAYMENT SERVICE ROUTES (webhook only - public)
// ===========================
const paymentServiceProxy = createProxy('paymentService', config.SERVICES.PAYMENT_SERVICE_URL);

// Razorpay webhook (public — no auth, signature-verified by payment-service)
router.post(
     '/payments/webhooks/razorpay',
     paymentServiceProxy
);

// Gateway Health Status

router.get('/gateway/health', (req, res) => {
     res.status(200).json({
          success: true,
          message: "API Gateway is healthy",
          timestamp: new Date().toISOString()
     });
});

router.get('/gateway/circuit-breakers', (req, res) => {
     const status = getCircuitBreakerStatus();
     res.status(200).json({
          success: true,
          circuitBreakers: status,
     });
});

module.exports = router;