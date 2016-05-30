var ttypes = require('./thrift/CliService_types');
var ResultSet = require('./resultset');
var QueryCacheException = require('./exception');

Statement = module.exports = function(client, sessionHandle) {
    this.client = client;
    this.sessionHandle = sessionHandle;
    this.operationHandle = null;

    this.hasResultSet = false;
    this.updateRowCount = -1;

    this.resultSet = null;
};

Statement.prototype.execute = function*(query) {
    var req = new ttypes.TExecuteStatementReq({
        sessionHandle: this.sessionHandle,
        statement: query,
        configuration: null,
        asyncMode: true
    });
    var resp = yield this.client.ExecuteStatement(req);
    this.operationHandle = resp.operationHandle;
    var osReq = new ttypes.TGetOperationStatusReq({
        operationHandle: this.operationHandle
    });

    var osResp;
    do {
        osResp = yield this.client.GetOperationStatus(osReq);
    } while (osResp.status.statusCode == ttypes.TOperationState.RUNNING_STATE);

    if (osResp.operationState == ttypes.TOperationState.FINISHED_STATE) {
        this.hasResultSet = osResp.operationHandle.hasResultSet;
        if (this.hasResultSet)
            this.updateRowCount = -1;
        else
            this.updateRowCount = osResp.operationHandle.updateRowCount;
        return this.hasResultSet;
    }

    throw new QueryCacheException({
        message: "operation was not successful. state is " + osResp.operationState
    });
};

Statement.prototype.getResultSet = function*() {
    if (this.hasResultSet == false) {
        return null;
    }
    if (this.resultSet != null)
        return resultSet;

    var req = new ttypes.TGetResultSetMetadataReq({
        operationHandle: this.operationHandle
    });

    var resp = yield this.client.GetResultSetMetadata(req);
    this.resultSet = new ResultSet(this.client, this.operationHandle, resp.schema);
    return this.resultSet;
};

Statement.prototype.setCommit = function*() {

    var req = new ttypes.TCommitReq({
        sessionHandle: this.sessionHandle
    });
    var resp = yield this.client.Commit(req);
    resp.updateRowCount = this.updateRowCount;

    this.sessionHandle = null;
    this.client = null;
    this.hasResultSet = false;
    this.updateRowCount = -1;
    //console.log('commit res:',resp);

    return resp;

};


Statement.prototype.close = function*() {
    this.sessionHandle = null;
    this.hasResultSet = false;
    this.updateRowCount = -1;

    if (this.resultSet != null) {
        yield this.resultSet.close();
        this.resultSet = null;
        this.operationHandle = null;
        this.client = null;
        return;
    }

    if (this.operationHandle) {
        var req = ttypes.TCloseOperationReq({
            operationHandle: this.operationHandle
        });
        if(req) {
            yield this.client.CloseOperation(req);
        }
    }
};