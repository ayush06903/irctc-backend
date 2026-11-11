//agar user koi undefined/galat route hit karta hai to to handle such cases this notfound wali middleware run hotti hai
const { NotFoundError } = require('../utils/error');

function notFound(req, res, next) {
  next(new NotFoundError(`Route ${req.method} ${req.path} not found`));
}

module.exports = { notFound };