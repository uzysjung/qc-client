var TCLIService = require('./thrift/TCLIService');
var ttypes = require('./thrift/CliService_types');

ResultSet = module.exports = function(client, operationHandle, schema) {
    this.client = client;
    this.operationHandle = operationHandle;

    this.columns = {};
    this.columnCount = 0;
    this.columnNameIndex = {};

    this.rowQueue = [];
    this.currentRow = null;
    this.lastColumnWasNull = false;
    this.hasMoreRows = true;

    this.closed = false;

    for (var c in schema.columns) {
        this.columnNameIndex[schema.columns[c].columnName.toUpperCase()] = schema.columns[c].position-1;
        this.columns[schema.columns[c].position-1] = {
            columnName: schema.columns[c].columnName,
            // querycache server doesn't support for types other than primitive types, currently.
            type: schema.columns[c].typeDesc.types[0].primitiveEntry.type,
            comment: schema.columns[c].comment
        };
        this.columnCount++;
    }
};

ResultSet.prototype.getColumnCount = function() {
    return this.columnCount;
};

ResultSet.prototype.getRowsFromServer = function*() {
    var req = new ttypes.TFetchResultsReq({
        operationHandle: this.operationHandle,
        orientation: ttypes.TFetchOrientation.FETCH_NEXT,
        maxRows: 1024
    });
    var resp;
    if(req) {
        resp = yield this.client.FetchResults(req);
    }
    this.hasMoreRows = resp.hasMoreRows;
    if (resp.results && resp.results.rows && resp.results.rows.length > 0) {
        return resp.results.rows;
    }
    return null;
};

ResultSet.prototype.next = function*() {
    if (this.closed) {
        return false;
    }
    if (this.rowQueue.length > 0) {
        this.currentRow = this.rowQueue.pop();
        return true;
    }
    if (!this.hasMoreRows) {
        return false;
    }

    var newRows = yield this.getRowsFromServer();
    if (newRows != null) {
        this.rowQueue = this.rowQueue.concat(newRows);
        this.currentRow = this.rowQueue.pop();
        return true;
    }
    return false;
};

ResultSet.prototype.close = function *() {
    if (this.closed)
        return true;

    this.closed = true;

    if (this.operationHandle != null) {
        var req = new ttypes.TCloseOperationReq({
            operationHandle: this.operationHandle
        });
        if(req) {
            yield this.client.CloseOperation(req);
        }
    }
};

ResultSet.prototype.getColumnNames = function() {
    return this.columnNameIndex;
};

ResultSet.prototype.getObject = function(index) {
    var zIndex;
    if (typeof(index) == "string") {
        var upper = index.toUpperCase();
        if ( upper in this.columnNameIndex )
            zIndex = this.columnNameIndex[upper];
        else
            throw new RangeError("column name " + index + " is invalid");
    } else {
        zIndex = Number(index) - 1;
    }
    if (zIndex < 0 || zIndex >= this.columns.length) {
        throw new RangeError("index out of range. valid range is 1<=index<=" + this.columns.length);
    }
    if (this.currentRow == null) {
        throw new ReferenceError("Referencing column before calling next()");
    }

    var columnDesc = this.columns[zIndex];
    var column = this.currentRow.colVals[zIndex];
    if (column.stringVal != null && column.stringVal.value == null ) {
        this.lastColumnWasNull = true;
        return null;
    }
    var value = null;
    switch (columnDesc.type) {
        case 2: // ttypes.TTypeId.BOOLEAN
            value = column.boolVal;
            break;
        case 3: // ttypes.TTypeId.TINYINT
            value = column.byteVal;
            break;
        case 4: // ttypes.TTypeId.SMALLINT
            value = column.i16Val;
            break;
        case 5: // ttypes.TTypeId.INT
            value = column.i32Val;
            break;
        case 6: // ttypes.TTypeId.BIGINT
            value = column.i64Val;
            break;
        case 7: // ttypes.TTypeId.FLOAT
        case 8: // ttypes.TTypeId.DOUBLE
            value = column.doubleVal;
            break;
        case 9: // ttypes.TTypeId.DATE
        case 10: // ttypes.TTypeId.DATETIME
        case 11: // ttypes.TTypeId.TIMESTAMP
            value = new Date(column.i64Val);
            break;
        case 12: // ttypes.TTypeId.STRING
        case 14: // ttypes.TTypeId.DECIMAL
        case 15: // ttypes.TTypeId.CHAR
            value = column.stringVal.value;
            break;
        case 13: // ttypes.TTypeId.BINARY
            value = [];
            for (var i=0; i<column.stringVal.value.length; ++i) {
                value.push(column.stringVal.value.charCodeAt(i));
            }
            break;
        default:
            value = "__unknown__value__";
            break;
    }
    if (value == null) {
        this.lastColumnWasNull = true;
    }
    return value;
};

ResultSet.prototype.getRowDict = function() {
    if (this.currentRow == null) {
        throw new ReferenceError("Referencing column before calling next()");
    }

    var row = {};
    for (var i = 0; i<this.columnCount; i++) {
        row[this.columns[i].columnName] = this.getObject(i+1);
    }
    return row;
};

ResultSet.prototype.getRowArray = function() {
    if (this.currentRow == null) {
        throw new ReferenceError("Referencing column before calling next()");
    }

    var row = [];
    for (var i = 0; i<this.columnCount; i++) {
        row.push(this.getObject(i+1));
    }
    return row;
};

ResultSet.prototype.wasNull = function() {
    return this.lastColumnWasNull;
};
