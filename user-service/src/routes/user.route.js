const express = require('express');
const {getUserContext} = require('../middlewares/getUserContext.middleware');
const { internalAuth } = require('../middlewares/internalAuth.middleware');
const { getProfile, updateProfile, deleteProfile, getUserInternal } = require('../controllers/user.controller');

const router = express.Router();
                      //before we introduced api gatway service hmare middleware folder me ek auth.middleware file thi which had logic of validating the access token but after we integrated auth.middleware.js in api gateway(so that we donot have to write seperate auth.middleware.js for each service) so now we dont need auth.middleware.js that was present in user servcice so we deleted user-service/src/middleware/auth.middleware.js
                      //understand how getUserContext is doing the work that auth.middleware.js was doing 
router.get("/profile", getUserContext, getProfile);
router.put("/profile", getUserContext, updateProfile);
router.delete("/profile", getUserContext, deleteProfile);

router.get("/internal/:userId", internalAuth, getUserInternal);

module.exports = router;