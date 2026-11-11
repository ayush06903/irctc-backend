//diffrent origions(ports/domains) se anee wali request ko handle krne ke lie hai have cors.js-->isme we configure ki kin sources se ane wali req ko our backend will accept and other sources will not be allowed to access our service... we do so for security purpouses
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
