/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 */

#include "h2odbc.h"

SQLRETURN  SQL_API SQLSetStmtAttr(SQLHSTMT StatementHandle,
           SQLINTEGER Attribute, SQLPOINTER Value,
           SQLINTEGER StringLength) {
    trace("SQLSetStmtAttr");
    Statement* stat=Statement::cast(StatementHandle);
    if(stat==0) {
        return SQL_INVALID_HANDLE;
    }    
    stat->setError(0);        
    switch(Attribute) {
    case SQL_ATTR_APP_PARAM_DESC:
        trace(" SQL_ATTR_APP_ROW_DESC TODO");
        // todo
        return SQL_ERROR;
    case SQL_ATTR_APP_ROW_DESC:
        trace(" SQL_ATTR_APP_ROW_DESC TODO");
        // todo
        return SQL_ERROR;
    case SQL_ATTR_ASYNC_ENABLE:
        trace(" SQL_ATTR_ASYNC_ENABLE TODO not supported");
        // TODO should support that!
        if((SQLUINTEGER)Value != SQL_ASYNC_ENABLE_OFF) {
            stat->setError(E_01S02);
            return SQL_SUCCESS_WITH_INFO;   
        }
        break;
    case SQL_ATTR_CONCURRENCY: {
        trace(" SQL_ATTR_CONCURRENCY");
        // option value can be changed
        SQLUINTEGER type = (SQLUINTEGER)Value;
        switch(type) {
        case SQL_CONCUR_READ_ONLY:
            trace("  SQL_CONCUR_READ_ONLY");
            break;
        case SQL_CONCUR_LOCK:
            trace("  SQL_CONCUR_LOCK");
            stat->setError(E_01S02);
            return SQL_SUCCESS_WITH_INFO;            
            break;
        case SQL_CONCUR_ROWVER:
            trace("  SQL_CONCUR_ROWVER");
            stat->setError(E_01S02);
            return SQL_SUCCESS_WITH_INFO;            
            break;
        case SQL_CONCUR_VALUES:
            trace("  SQL_CONCUR_ROWVER");
            stat->setError(E_01S02);
            return SQL_SUCCESS_WITH_INFO;            
            break;
        default:
            trace("  =? %d TODO", type);
            return SQL_ERROR;
        }
        break;
    }
    case SQL_ATTR_CURSOR_SCROLLABLE:
        trace(" SQL_ATTR_CURSOR_SCROLLABLE TODO");
        break;
    case SQL_ATTR_CURSOR_SENSITIVITY: {
        trace(" SQL_ATTR_CURSOR_SENSITIVITY TODO");
        // it seems this must be supported!
        SQLUINTEGER type = (SQLUINTEGER)Value;
        switch(type) {
        case SQL_UNSPECIFIED:
            trace("  SQL_UNSPECIFIED");
            break;
        case SQL_INSENSITIVE:
            trace("  SQL_INSENSITIVE");
            stat->setError(E_01S02);
            return SQL_SUCCESS_WITH_INFO;            
            break;
        case SQL_SENSITIVE:
            trace("  SQL_SENSITIVE");
            stat->setError(E_01S02);
            return SQL_SUCCESS_WITH_INFO;            
            break;
        default:
            trace("  =? %d TODO", type);
            return SQL_ERROR;
        }
        break;
    }
    case SQL_ATTR_CURSOR_TYPE: {
        trace(" SQL_ATTR_CURSOR_TYPE");
        // option value can be changed
        SQLUINTEGER type = (SQLUINTEGER)Value;
        switch(type) {
        case SQL_CURSOR_FORWARD_ONLY:
            trace("  SQL_CURSOR_FORWARD_ONLY");
            break;
        case SQL_CURSOR_STATIC:
            trace("  SQL_CURSOR_STATIC");
            stat->setError(E_01S02);
            return SQL_SUCCESS_WITH_INFO;            
            break;
        case SQL_CURSOR_KEYSET_DRIVEN:
            trace("  SQL_CURSOR_KEYSET_DRIVEN");
            stat->setError(E_01S02);
            return SQL_SUCCESS_WITH_INFO;            
            break;
        case SQL_CURSOR_DYNAMIC:
            trace("  SQL_CURSOR_DYNAMIC");
            stat->setError(E_01S02);
            return SQL_SUCCESS_WITH_INFO;
            break;
        default:
            trace("  =? %d TODO", type);
            return SQL_ERROR;
        }
        break;
    }
    case SQL_ATTR_IMP_PARAM_DESC:
        trace(" SQL_ATTR_IMP_PARAM_DESC TODO");
        return SQL_ERROR;
    case SQL_ATTR_ROW_NUMBER:
        trace(" SQL_ATTR_ROW_NUMBER TODO");
        return SQL_ERROR;
    case SQL_ATTR_IMP_ROW_DESC:
        trace(" SQL_ATTR_IMP_ROW_DESC TODO");
        return SQL_ERROR;
    case SQL_ATTR_QUERY_TIMEOUT:
        trace(" SQL_ATTR_QUERY_TIMEOUT");
        // option value can be changed
        if((SQLUINTEGER)Value != 0) {
            stat->setError(E_01S02);
            return SQL_SUCCESS_WITH_INFO;
        }
        break;
    case SQL_ATTR_ENABLE_AUTO_IPD:
        trace(" SQL_ATTR_ENABLE_AUTO_IPD TODO");
        break;
    case SQL_ATTR_FETCH_BOOKMARK_PTR:
        trace(" SQL_ATTR_FETCH_BOOKMARK_PTR TODO");
        break;
    case SQL_ATTR_KEYSET_SIZE:
        trace(" SQL_ATTR_KEYSET_SIZE");
        // option value can be changed
        if((SQLUINTEGER)Value != 0) {
            stat->setError(E_01S02);
            return SQL_SUCCESS_WITH_INFO;
        }
        break;
    case SQL_ATTR_MAX_LENGTH:
        trace(" SQL_ATTR_MAX_LENGTH %d", Value);
        // option value can be changed
        if((SQLUINTEGER)Value != 0) {
            stat->setError(E_01S02);
            return SQL_SUCCESS_WITH_INFO;
        }
        break;
    case SQL_ATTR_MAX_ROWS:
        trace(" SQL_ATTR_MAX_ROWS");
        // option value can be changed
        if((SQLUINTEGER)Value != 0) {
            stat->setError(E_01S02);
            return SQL_SUCCESS_WITH_INFO;
        }
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
        // TODO it seems all drivers must support arrays, but the MySQL driver doesn't support it
        if((SQLUINTEGER)Value != 1) {
            stat->setError(E_01S02);
            return SQL_SUCCESS_WITH_INFO;
        }
        break;
    case SQL_ATTR_RETRIEVE_DATA:
        trace(" SQL_ATTR_RETRIEVE_DATA TODO");
        break;
    case SQL_ATTR_ROW_ARRAY_SIZE:
        trace(" SQL_ATTR_ROW_ARRAY_SIZE");
        // option value can be changed
        if((SQLUINTEGER)Value != 1) {
            stat->setError(E_01S02);
            return SQL_SUCCESS_WITH_INFO;
        }
        break;
    case SQL_ATTR_ROW_BIND_OFFSET_PTR:
        trace(" SQL_ATTR_ROW_BIND_OFFSET_PTR TODO");
        break;
    case SQL_ATTR_ROW_BIND_TYPE:
        trace(" SQL_ATTR_ROW_BIND_TYPE TODO");
        if(Value == SQL_BIND_BY_COLUMN) {
            // column wise binding
            // SQL_BIND_BY_COLUMN = 0UL
            trace("  SQL_BIND_BY_COLUMN TODO");
            stat->getAppRowDesc()->setBindingType(false, 0);
        } else {
            // row wise binding
            trace("  row wise binding size=%d", Value);
            stat->getAppRowDesc()->setBindingType(true, (int)Value);
        }
        break;
    case SQL_ATTR_ROW_OPERATION_PTR:
        trace(" SQL_ATTR_ROW_OPERATION_PTR TODO");
        break;
    case SQL_ATTR_ROW_STATUS_PTR:
        trace(" SQL_ATTR_ROW_STATUS_PTR");
        stat->getAppRowDesc()->setStatusPointer((SQLUSMALLINT*)Value);        
        break;
    case SQL_ATTR_ROWS_FETCHED_PTR:
        trace(" SQL_ATTR_ROWS_FETCHED_PTR");
        stat->getImpRowDesc()->setRowsProcessedPointer((SQLUINTEGER*)Value);
        break;
    case SQL_ATTR_SIMULATE_CURSOR: {
        trace(" SQL_ATTR_SIMULATE_CURSOR");
        // option value can be changed
        if((SQLUINTEGER)Value != SQL_SC_NON_UNIQUE) {
            stat->setError(E_01S02);
            return SQL_SUCCESS_WITH_INFO;
        }
        break;
    }
    case SQL_ATTR_USE_BOOKMARKS: {
        trace(" SQL_ATTR_USE_BOOKMARKS");
        switch((int)Value) {
        case SQL_UB_OFF:
            trace("  SQL_UB_OFF");
            stat->setUseBookmarks(false);
            break;        
        case SQL_UB_FIXED:
            trace("  SQL_UB_FIXED TODO");
            break;        
        case SQL_UB_VARIABLE:
            trace("  SQL_UB_VARIABLE");
            stat->setUseBookmarks(true);
            break;   
        default:
            trace("  ? %d TODO", Value); 
        }
        break;
    }
    default:
        trace(" ? %d TODO",Attribute);
        break;

    }
    return SQL_SUCCESS;
}

