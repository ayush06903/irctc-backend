//YE proxy.js DOES 3 THINGS  1->IMPLEMENTS CIRCUIT BREAKER
                           //2->REQUEST ROUTINGG
                           //3->IMPLEMETS createproxy FUNCTION

const axios = require('axios');
const { config } = require('../config');
const { ServiceUnavailableError, GatewayTimeoutError } = require('../utils/error');
const logger = require('../config/logger');


/**
 * Circuit Breaker implementation--> SEE 06.PNG TO SEE THE NEED OF CIRCUIT BREAKER
 * see 07.png--. to see how circuit breaker solves this problem--->5 ffailed req ke baad gateway us service ke lie(jo currently down hai) req lena hi band krdega
 * Prevents cascading failures when downstream services are down
 */
class CircuitBreaker {
     constructor(serviceName, threshold = config.CIRCUIT_BREAKER_THRESHOLD, timeout = config.CIRCUIT_BREAKER_TIMEOUT) {
          this.serviceName = serviceName;
          this.failureCount = 0;
          this.threshold = threshold;
          this.timeout = timeout;
          this.state = 'CLOSED'; // CLOSED, OPEN, HALF_OPEN--->see 08.png
          this.nextAttempt = Date.now();
     }

     async execute(request) {
          if (this.state === 'OPEN') {        //service unavailable
               if (Date.now() < this.nextAttempt) {  //if currently time is 1:55 pm but our api gateway is next scheduled tto try and establish connection again on 2:00 pm
                    throw new ServiceUnavailableError(
                         `Service ${this.serviceName} is temporarily unavailable. Circuit breaker is OPEN.`
                    );
               }
               //if current time is 2:30 pm and out api gateway was schedduled to try to reestablish connection on 2:00pm
               // Try to close the circuit
               this.state = 'HALF_OPEN';              //whenever we try to close the circuit state would be half open
               logger.info(`Circuit breaker HALF_OPEN for ${this.serviceName}`);
          }

          try {
               const response = await request();       //we try to hit our http req
               this.onSuccess();        //jo http req thi vo successfuly execute ho gyi
               return response;
          } catch (err) {
               this.onFailure();        //http req fail hogyi
               throw err;
          }
     }

     onSuccess() {
          this.failureCount = 0;
          if (this.state === 'HALF_OPEN') {
               this.state = 'CLOSED';
               logger.info(`Circuit breaker CLOSED for ${this.serviceName}`);
          }
     }

     onFailure() {
          this.failureCount++;
          if (this.failureCount >= this.threshold) {
               this.state = 'OPEN';
               this.nextAttempt = Date.now() + this.timeout;
                                               //timeout tells api gateway kki usko kitna time wait krna hai to again retry to establish connection btw api gateway and down service
               logger.error(
                    `Circuit breaker OPEN for ${this.serviceName}. Next attempt at ${new Date(this.nextAttempt).toISOString()}`
               );
          }
     }

     getState() {        //ye perticular service ka poora status return krta hai
          return {
               service: this.serviceName,
               state: this.state,
               failureCount: this.failureCount,
               nextAttempt: this.state === 'OPEN' ? new Date(this.nextAttempt).toISOString() : null,
          };
     }
}

// Circuit breakers for each service
const circuitBreakers = {
     userService: new CircuitBreaker('user-service'),
     searchService: new CircuitBreaker('search-service'),
     adminService: new CircuitBreaker('admin-service'),
     notificationService: new CircuitBreaker('notification-service'),
     bookingService: new CircuitBreaker('booking-service'),
     paymentService: new CircuitBreaker('payment-service'),
     inventoryService: new CircuitBreaker('inventory-service')
};

//before implementing the other two operations of api gateway first we will try to understand the whole flow and which operation happens in what order-->09.png and 10.png
/**
 * 2->Forward request to downstream service--->ye method req ko gateway se conneccted backend microservice pr route/forward krega
 */
async function forwardRequest(serviceUrl, path, method, data, headers, circuitBreaker) {
     const url = `${serviceUrl}${path}`;
     logger.info(url);
     // http://localhost:4001/auth/login
     const requestConfig = {
          method,
          url,
          timeout: config.SERVICE_TIMEOUT_MS,
          headers: {
               ...headers,
               // Remove host header to avoid conflicts
               host: undefined,            //gateway ka host is diffrent from service ka host
               // Remove content-length to let axios recalculate
               'content-length': undefined,
          },
          // Important: Don't validate status, let service response through
          validateStatus: () => true,
          // Set max redirects
          maxRedirects: 5,
     };

     // Add data based on method
     if (method !== 'GET' && method !== 'DELETE' && data) {
          requestConfig.data = data;
     }

     // For GET and DELETE, add params if data exists
     if ((method === 'GET' || method === 'DELETE') && data) {
          requestConfig.params = data;
     }

     logger.debug(`Forwarding ${method} ${url}`, {
          headers: requestConfig.headers,
          hasData: !!data,
          timeout: config.SERVICE_TIMEOUT_MS,
     });

     try {
          const response = await circuitBreaker.execute(() => axios(requestConfig));
                                //ye hmne circuit breaker ke execute method ko call kara aur then axios ka use krkr req ko route krdia to correct microservice

          logger.debug(`Response from ${url}:`, {
               status: response.status,
               statusText: response.statusText,
          });

          return {
               status: response.status,
               data: response.data,
               headers: response.headers,
          };
     } catch (err) {
          logger.error(`Error forwarding to ${serviceUrl}:`, {
               message: err.message,
               code: err.code,
               url: url,
               method: method,
               timeout: config.SERVICE_TIMEOUT_MS,
          });

          if (err.code === 'ECONNABORTED' || err.code === 'ETIMEDOUT') {
               throw new GatewayTimeoutError(`Request to ${serviceUrl} timed out after ${config.SERVICE_TIMEOUT_MS}ms`);
          }

          if (err.code === 'ECONNREFUSED') {
               throw new ServiceUnavailableError(`Cannot connect to ${serviceUrl}. Service may be down.`);
          }

          if (err.response) {
               logger.error(`Service error from ${serviceUrl}:`, {
                    status: err.response.status,
                    data: err.response.data,
               });

               return {
                    status: err.response.status,
                    data: err.response.data,
                    headers: err.response.headers,
               };
          }

          // Network error or service down--You would have seen this in video
          logger.error(`Network error while calling ${serviceUrl}:`, err.message);
          throw new ServiceUnavailableError(`Service temporarily unavailable: ${err.message}`);
     }
}

/**
 * Proxy middleware factory
 */
function createProxy(serviceName, serviceUrl) {            //I DID GET WHY WE ARE TRANSFORMING THE ROUTE HERE BUT DONT HAVEE CLEAR UNDERSANDING RN
     const circuitBreaker = circuitBreakers[serviceName];

     if (!circuitBreaker) {
          throw new Error(`No circuit breaker found for service: ${serviceName}`);
     }

     return async (req, res, next) => {
          try {
               // Extract path (remove /api prefix only)
               // Gateway: /api/users/auth/login -> Service: /auth/login
               // Gateway: /api/users/user/profile -> Service: /user/profile
               logger.info(req.path);         //see 11.png to see result of each line here
               const pathParts = req.path.split('/').filter(Boolean);
               logger.info(pathParts);
               // Remove 'users' (first part), keep the rest
               // ['users', 'auth', 'login'] -> ['auth', 'login'] -> '/auth/login'
               const servicePath = '/' + pathParts.slice(1).join('/');
               logger.info(servicePath);

               const result = await forwardRequest(         //Calling oopr defined forwardRequest method which will route the req from api gateway to correct microserice of our backend
                    serviceUrl,
                    servicePath + (req.url.includes('?') ? req.url.substring(req.url.indexOf('?')) : ''),
                    req.method,
                    req.body,
                    req.headers,
                    circuitBreaker
               );

               // Forward response headers (except some)
               const excludeHeaders = ['connection', 'keep-alive', 'transfer-encoding', 'host'];         //we removed these coz gateway in headers ko khud manage krta hai
               Object.keys(result.headers).forEach((key) => {
                    if (!excludeHeaders.includes(key.toLowerCase())) {
                         res.setHeader(key, result.headers[key]);
                    }
               });

               // Send response
               res.status(result.status).json(result.data);
          } catch (err) {
               next(err);
          }
     };
}

/**
 * Health check endpoint for circuit breakers
 */
function getCircuitBreakerStatus() {
     return Object.values(circuitBreakers).map((cb) => cb.getState());
}

module.exports = {
     createProxy,
     CircuitBreaker,
     circuitBreakers,
     getCircuitBreakerStatus,
};