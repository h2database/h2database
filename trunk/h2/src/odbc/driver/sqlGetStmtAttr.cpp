/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 */

#include "h2odbc.h"

SQLRETURN  SQL_API SQLGetStmtAttr(SQLHSTMT StatementHandle,
           SQLINTEGER Attribute, SQLPOINTER Value,
           SQLINTEGER BufferLength, SQLINTEGER* StringLength) {
    trace("SQLGetStmtAttr");
    Statement* stat=Statement::cast(StatementHandle);
    if(stat==0) {
        return SQL_INVALID_HANDLE;
    }    
    stat->setError(0);        
    switch(Attribute) {
    case SQL_ATTR_APP_ROW_DESC:
        trace(" SQL_ATTR_APP_ROW_DESC");
        returnPointer(Value,stat->getAppRowDesc());
        break;
    case SQL_ATTR_APP_PARAM_DESC:
        trace(" SQL_ATTR_APP_PARAM_DESC");
        returnPointer(Value,stat->getAppParamDesc());
        break;
    case SQL_ATTR_IMP_ROW_DESC:
        trace(" SQL_ATTR_IMP_ROW_DESC");
        returnPointer(Value,stat->getImpRowDesc());
        break;
    case SQL_ATTR_IMP_PARAM_DESC:
        trace(" SQL_ATTR_IMP_PARAM_DESC");
        returnPointer(Value,stat->getImpParamDesc());
        break;
    case SQL_ATTR_QUERY_TIMEOUT:
        trace(" SQL_ATTR_QUERY_TIMEOUT");
        returnInt(Value,StringLength,0);
        break;
    case SQL_ATTR_ASYNC_ENABLE:
        trace(" SQL_ATTR_ASYNC_ENABLE TODO");
        break;
    case SQL_ATTR_CONCURRENCY:
        trace(" SQL_ATTR_CONCURRENCY");
        returnInt(Value, SQL_CONCUR_READ_ONLY);
        break;
    case SQL_ATTR_CURSOR_SCROLLABLE:
        trace(" SQL_ATTR_CURSOR_SCROLLABLE TODO");
        break;
    case SQL_ATTR_CURSOR_SENSITIVITY:
        trace(" SQL_ATTR_CURSOR_SENSITIVITY");
        returnInt(Value, SQL_INSENSITIVE);
        break;
    case SQL_ATTR_CURSOR_TYPE:
        trace(" SQL_ATTR_CURSOR_TYPE");
        returnInt(Value, SQL_CURSOR_FORWARD_ONLY);
        break;
    case SQL_ATTR_ENABLE_AUTO_IPD:
        trace(" SQL_ATTR_ENABLE_AUTO_IPD TODO");
        break;
    case SQL_ATTR_FETCH_BOOKMARK_PTR:
        trace(" SQL_ATTR_FETCH_BOOKMARK_PTR TODO");
        break;
    case SQL_ATTR_KEYSET_SIZE:
        trace(" SQL_ATTR_KEYSET_SIZE TODO");
        break;
    case SQL_ATTR_MAX_LENGTH:
        trace(" SQL_ATTR_MAX_LENGTH TODO");
        break;
    case SQL_ATTR_MAX_ROWS:
        trace(" SQL_ATTR_MAX_ROWS TODO");
        break;
    case SQL_ATTR_METADATA_ID:
        trace(" SQL_ATTR_METADATA_ID TODO");
        break;
    case SQL_ATTR_NOSCAN:
        trace(" SQL_ATTR_NOSCAN TODO");
        break;
    case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
        trace(" SQL_ATTR_PARAM_BIND_OFFSET_PTR TODO");
        break;
    case SQL_ATTR_PARAM_BIND_TYPE:
        trace(" SQL_ATTR_PARAM_BIND_TYPE TODO");
        break;
    case SQL_ATTR_PARAM_OPERATION_PTR:
        trace(" SQL_ATTR_PARAM_OPERATION_PTR TODO");
        break;
    case SQL_ATTR_PARAM_STATUS_PTR:
        trace(" SQL_ATTR_PARAM_STATUS_PTR TODO");
        break;
    case SQL_ATTR_PARAMS_PROCESSED_PTR:
        trace(" SQL_ATTR_PARAMS_PROCESSED_PTR TODO");
        break;
    case SQL_ATTR_PARAMSET_SIZE:
        trace(" SQL_ATTR_PARAMSET_SIZE TODO");
        break;
    case SQL_ATTR_RETRIEVE_DATA:
        trace(" SQL_ATTR_RETRIEVE_DATA TODO");
        break;
    case SQL_ATTR_ROW_ARRAY_SIZE:
        trace(" SQL_ATTR_ROW_ARRAY_SIZE TODO");
        break;
    case SQL_ATTR_ROW_BIND_OFFSET_PTR:
        trace(" SQL_ATTR_ROW_BIND_OFFSET_PTR TODO");
        break;
    case SQL_ATTR_ROW_BIND_TYPE:
        trace(" SQL_ATTR_ROW_BIND_TYPE TODO");
        break;
    case SQL_ATTR_ROW_NUMBER: {
        trace(" SQL_ATTR_ROW_NUMBER");
        returnInt(Value, stat->getRowId());
        break;
    }
    case SQL_ATTR_ROW_OPERATION_PTR:
        trace(" SQL_ATTR_ROW_OPERATION_PTR TODO");
        break;
    case SQL_ATTR_ROW_STATUS_PTR:
        trace(" SQL_ATTR_ROW_STATUS_PTR");
        returnInt(Value, (int)stat->getAppRowDesc()->getStatusPointer());
        break;
    case SQL_ATTR_ROWS_FETCHED_PTR:
        trace(" SQL_ATTR_ROWS_FETCHED_PTR TODO");
        break;
    case SQL_ATTR_SIMULATE_CURSOR:
        trace(" SQL_ATTR_SIMULATE_CURSOR TODO");
        break;
    case SQL_ATTR_USE_BOOKMARKS:
        trace(" SQL_ATTR_USE_BOOKMARKS");
        returnInt(Value, stat->getUseBookmarks() ? SQL_UB_VARIABLE : SQL_UB_OFF);
        break;
    default:
        trace(" ? %d TODO",(int)Attribute);
        return SQL_ERROR;
    }
    return SQL_SUCCESS;
}
