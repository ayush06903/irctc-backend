//SINCE WE KNOW BROWSER BLOCKS REQ COMING FROM DIFFRENT ORIGINS BUT WHEN WE HAVE MULTIPLE SERVICES AND ALL THE SERVICES ARE RUNNING ON DIFFRENT PORS THEN WE WANT THEM TO INTERACT WITH EACH OTHER
const cors = require('cors');
const { config } = require('../config');

const allowedOrigins = config.ALLOWED_ORIGINS
     ? config.ALLOWED_ORIGINS.split(',').map(o => o.trim())
     : [];

const corsMiddleware = cors({
     origin: function (origin, callback) {

          if (!origin) return callback(null, true);

          if (allowedOrigins.includes(origin)) {
               callback(null, true);
          } else {
               callback(new Error('Not allowed by CORS'));
          }
     },
     credentials: true,
     methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
     allowedHeaders: ['Content-Type', 'Authorization'],
});

module.exports = { corsMiddleware };
