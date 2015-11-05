var thrift = require('thrift');
var os = require('os');
var net = require('net');
var ttypes = require('./thrift/CliService_types');
var TCLIService = require('./thrift/TCLIService');
var Statement = require('./statement');

qcClient = module.exports = function() {
    this.connection = null;
    this.client = null;
    this.serverInfo = null;
    this.sessionHandle = null;
    this.promisedClient = null;
};

qcClient.prototype = {};
qcClient.prototype.open = function*(backendType, host, port, username, password) {
    var hostInfo = new ttypes.THostInfo({
        hostname: os.hostname(),
        ipaddr: null,
        portnum: null
    });
    var req = new ttypes.TOpenSessionReq({
        clientProtocol: 0,
        url: 'jdbc:' + backendType,
        username: username,
        password: password,
        hostInfo: hostInfo,
        clientVersion: 'querycache-nodejs 0.20.0'
    });
    var getConnection = new Promise(function (resolve, reject) {
        var callback = function(err, connection) {
            if (err !== null) return reject(err);
            resolve(connection);
        };

        var connection = thrift.createConnection("localhost", 8655, {
            transport : thrift.TBufferedTransport,
            protocol : thrift.TCompactProtocol
        });
        connection.on('error', function(err) {
            callback(err);
        });
        connection.on('connect', function() {
            callback(null, connection);
        });
    });
    this.connection = yield getConnection;
    hostInfo.ipaddr = this.connection.connection.localAddress;
    hostInfo.portnum = this.connection.connection.localPort;
    this.client = thrift.createClient(TCLIService.Client, this.connection);
    var resp = yield this.client.OpenSession(req);
    this.serverInfo = resp.hostInfo;
    this.sessionHandle = resp.sessionHandle;

    console.log("connected to querycache at " + this.serverInfo.hostname + ":" + this.serverInfo.portnum);
    return true;
};

qcClient.prototype.close = function*() {
    // TODO: close statements.
    if (this.connection != null) {
        this.connection.end();
        this.connection = null;
        this.client = null;
        this.serverInfo = null;
        this.sessionHandle = null;
    }
};

qcClient.prototype.createStatement = function() {
    // TODO: record statement instances to close when connection is closed.
    return new Statement(this.client, this.sessionHandle);
};