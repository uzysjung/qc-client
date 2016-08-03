var thrift = require('thrift');
var os = require('os');
var net = require('net');
var ttypes = require('./thrift/CliService_types');
var TCLIService = require('./thrift/TCLIService');
var Statement = require('./statement');
var url = require('url');

qcClient = module.exports = function() {
    this.connection = null;
    this.client = null;
    this.serverInfo = null;
    this.sessionHandle = null;
    this.promisedClient = null;
    this.connectionError = null;
    //this.timeOutOccured = null;
    this.connErrCB;
    this.connEndCB;
};

qcClient.prototype = {};

qcClient.prototype.parseURL = function(url) {
    var components = {};
    var tokens = url.split("://");
    components['protocol'] = tokens[0];
    var startOfPath = tokens[1].indexOf('/');
    var hostAndPort = null;
    var pathAndQuery = null;
    if (startOfPath >= 0) {
        hostAndPort = tokens[1].substring(0, startOfPath);
        pathAndQuery = tokens[1].substring(startOfPath);
    }
    else {
        hostAndPort = tokens[1];
    }

    tokens = hostAndPort.split(':');
    components['host'] = tokens[0];
    if (tokens[1]) {
        components['port'] = Number(tokens[1]);
    }
    if (pathAndQuery != null) {
        tokens = pathAndQuery.split('?');
        components['path'] = tokens[0];
        components['query'] = tokens[1];
    }
    return components;
};

qcClient.prototype.open = function*(jdbcUrl, username, password) {
    var self = this;
    var urlComponents = this.parseURL(jdbcUrl);
    if (!urlComponents.port)
        urlComponents.port = 8655;

    var hostInfo = new ttypes.THostInfo({
        hostname: os.hostname(),
        ipaddr: null,
        portnum: null
    });
    var req = new ttypes.TOpenSessionReq({
        clientProtocol: 0,
        url: urlComponents.protocol,
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

        var connection = thrift.createConnection(urlComponents.host, urlComponents.port, {
            transport : thrift.TBufferedTransport,
            protocol : thrift.TCompactProtocol
        });
        var cbErr = function(err) {
            //console.log('cbErr',err);
            // connection.removeListener('connect',cbConnect);
            // connection.removeListener('error',cbErr);
            reject(err);
        };
        var cbConnect = function() {
            //console.log('connect ok');
            // connection.removeListener('connect',cbConnect);
            // connection.removeListener('error',cbErr);
            resolve(connection);
        };

        connection.once('error', cbErr);
        connection.once('connect',cbConnect);
    });
    this.connection = yield getConnection;
    this.connErrCB = function(err){
        self.connectionError = err;
        console.log('qc-client connection error :',err.stack);
    };
    this.connEndCB = function(){
        self.connectionError = new Error("FIN from destination");
        // console.log('qc-client connection end :',self.connectionError.stack);
    };
    this.connection.on('error',self.connErrCB);
    this.connection.connection.on('end',self.connEndCB);


    hostInfo.ipaddr = this.connection.connection.localAddress;
    hostInfo.portnum = this.connection.connection.localPort;
    this.client = thrift.createClient(TCLIService.Client, this.connection);
    var resp;
    if(req) {
        resp = yield this.client.OpenSession(req);
    }
    this.serverInfo = resp.hostInfo;
    this.sessionHandle = resp.sessionHandle;

    //console.log("connected to querycache at " + this.serverInfo.hostname + ":" + this.serverInfo.portnum);
    return true;
};

qcClient.prototype.close = function*() {
    // TODO: close statements.
    if (this.connection != null) {
        // console.log('called close');

        this.connection.removeListener('error',this.connErrCB);
        this.connection.connection.removeListener('end',this.connEndCB);
        this.connection.end();
        this.connection = null;
        this.client = null;
        this.serverInfo = null;
        this.sessionHandle = null;
        this.connectionError = null;
        this.connErrCB = null;
        this.connEndCB = null;
    }
};

qcClient.prototype.createStatement = function() {
    // TODO: record statement instances to close when connection is closed.
    return new Statement(this.client, this.sessionHandle);
};