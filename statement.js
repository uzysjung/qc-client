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
    this.queryLogs = [];
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
        var logs = yield this.getQueryLogs();
        this.queryLogs = this.queryLogs.concat(logs.queryLogs);
        console.log('queryLogs:::',this.queryLogs)

    } while (
            (osResp.operationState == ttypes.TOperationState.INITIALIZED_STATE || osResp.operationState == ttypes.TOperationState.RUNNING_STATE)
            && osResp.status.statusCode == ttypes.TStatusCode.SUCCESS_STATUS
        );

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
Statement.prototype.excuteStatement = function*(query){

    var req = new ttypes.TExecuteStatementReq({
        sessionHandle: this.sessionHandle,
        statement: query,
        configuration: null,
        asyncMode: true
    });
    var resp = yield this.client.ExecuteStatement(req);
    this.operationHandle = resp.operationHandle;

    return resp;

};

Statement.prototype.checkExcuteStatus = function*(executeStatemenResp){

    var ret = { hasResultSet : null , logs : null , finished : false};
    var osReq = new ttypes.TGetOperationStatusReq({
        operationHandle: this.operationHandle
    });

    var osResp = yield this.client.GetOperationStatus(osReq);
    var logs = yield this.getQueryLogs();
    if(  (osResp.operationState == ttypes.TOperationState.INITIALIZED_STATE || osResp.operationState == ttypes.TOperationState.RUNNING_STATE)
        && osResp.status.statusCode == ttypes.TStatusCode.SUCCESS_STATUS )
    {
        ret.finished = false;
    } else {
        ret.finished = true;
    }

    var osResp = yield this.client.GetOperationStatus(osReq);
    ret.logs = yield this.getQueryLogs();



    if (osResp.operationState == ttypes.TOperationState.FINISHED_STATE) {
        this.hasResultSet = osResp.operationHandle.hasResultSet;
        if (this.hasResultSet)
            this.updateRowCount = -1;
        else
            this.updateRowCount = osResp.operationHandle.updateRowCount;
       ret.hasResultSet = this.hasResultSet ;
    }
    return ret;

}

Statement.prototype.getResultSet = function*() {
    if (this.hasResultSet == false) {
        return null;
    }
    if (this.resultSet != null)
        return resultSet;

    var req = new ttypes.TGetResultSetMetadataReq({
        operationHandle: this.operationHandle
    });
    var resp;
    if(req) {
        resp = yield this.client.GetResultSetMetadata(req);
    }
    this.resultSet = new ResultSet(this.client, this.operationHandle, resp.schema);
    return this.resultSet;
};
Statement.prototype.getQueryLogs = function*() {

    var req = new ttypes.TFetchQueryLogReq({
        operationHandle: this.operationHandle
    });
    var resp;
    if(req) {
        resp = yield this.client.FetchQueryLogs(req)

    }
    // console.log('resp:::!!!!!',resp);

    return resp;

};
Statement.prototype.setCommit = function*() {

    var req = new ttypes.TCommitReq({
        sessionHandle: this.sessionHandle
    });
    var resp = yield this.client.Commit(req);
    resp.updateRowCount = this.updateRowCount;
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
        var req = new ttypes.TCloseOperationReq({
            operationHandle: this.operationHandle
        });
        if(req) {
            yield this.client.CloseOperation(req);
            this.operationHandle = null;
            this.client = null;
        }
    }
};