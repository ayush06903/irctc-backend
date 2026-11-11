//since we are storing refresh token/refresh token id in redis to ye mapped rkhne ke lie kis device ka kon sa refresh token hai we also need device id aur also ek device ke multiple sessions ho skte hai vo bhi map krne ke lie we need device id->timestamp 2 hour 50 min
const crypto = require("crypto");

function getDeviceFingerprint(req) {
     const userAgent = req.headers["user-agent"] || "";
     const ip = req.ip || "";
     const accept = req.headers["accept"] || "";

     const raw = `${userAgent}|${ip}|${accept}`;

     return crypto
          .createHash("sha256")
          .update(raw)
          .digest("hex")
          .slice(0, 16); // short device id
}

module.exports = getDeviceFingerprint;
