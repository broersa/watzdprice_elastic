'use strict';

module.exports = function MyError(code, method, message, params, error) {
  Error.captureStackTrace(this, this.constructor);
  this.name = this.constructor.name;
  this.code = code;
  this.method = method;
  this.message = message;
  this.params = params;
  this.error = error;
};

require('util').inherits(module.exports, Error);
