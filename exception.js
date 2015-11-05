var ttypes = require("./thrift/CliService_types.js");

module.exports = function(status) {
    Error.call(this);
    Error.captureStackTrace(this, this.constructor);
    this.name = this.constructor.name;
    this.message = status.errorMessage;
    this.sqlState = status.sqlState;
    this.errorCode = status.errorCode;
    this.infoMessages = status.infoMessages;
};

