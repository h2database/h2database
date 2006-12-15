/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 */

#include "h2odbc.h"

SQLRETURN  SQL_API SQLSetConnectAttr(SQLHDBC ConnectionHandle,
           SQLINTEGER Attribute, SQLPOINTER Value,
           SQLINTEGER StringLength) {
    trace("SQLSetConnectAttr",(int)Value);
    Connection* conn=Connection::cast(ConnectionHandle);
    if(conn==0) {
        return SQL_INVALID_HANDLE;
    }
    conn->setError(0);        
    SQLUINTEGER uint=(SQLUINTEGER)Value;
    switch(Attribute) {
    case SQL_ATTR_ACCESS_MODE:
        trace(" SQL_ATTR_ACCESS_MODE");
        if(uint==SQL_MODE_READ_ONLY) {
            conn->setReadOnly(true);
        }
        break;
    case SQL_ATTR_ASYNC_ENABLE:
        trace(" SQL_ATTR_ASYNC_ENABLE (TODO not supported)");
        conn->setError(E_HYC00);
        return SQL_ERROR;
        break;
    case SQL_ATTR_ENABLE_AUTO_IPD:
        trace(" SQL_ATTR_AUTO_IPD TODO");
        conn->setError(E_HYC00);
        return SQL_ERROR;
    case SQL_ATTR_AUTO_IPD:
        trace(" SQL_ATTR_AUTO_IPD TODO");
        conn->setError(E_HYC00);
        return SQL_ERROR;
    case SQL_ATTR_AUTOCOMMIT:
        trace(" SQL_ATTR_AUTOCOMMIT");
        if(uint==SQL_AUTOCOMMIT_OFF) {
            conn->setAutoCommit(false);
        } else if(uint==SQL_AUTOCOMMIT_ON) {
            conn->setAutoCommit(true);
        }
        break;
    case SQL_ATTR_CONNECTION_DEAD:
        trace(" SQL_ATTR_CONNECTION_DEAD TODO");
        // read only
        return SQL_ERROR;
    case SQL_ATTR_CONNECTION_TIMEOUT:
        trace(" SQL_ATTR_CONNECTION_TIMEOUT");
        break;
    case SQL_ATTR_CURRENT_CATALOG:
        trace(" SQL_ATTR_CURRENT_CATALOG TODO");
        break;
    case SQL_ATTR_LOGIN_TIMEOUT:
        trace(" SQL_ATTR_LOGIN_TIMEOUT");
        break;
    case SQL_ATTR_METADATA_ID:
        trace(" SQL_ATTR_METADATA_ID");
        break;
    case SQL_ATTR_ODBC_CURSORS:
        trace(" SQL_ATTR_ODBC_CURSORS");
        if((SQLUINTEGER)Value != SQL_CUR_USE_ODBC) {
            trace("  not SQL_CUR_USE_ODBC %d", (SQLUINTEGER)Value);
            conn->setError(E_01S02);
            return SQL_ERROR;
        }
        break;
    case SQL_ATTR_PACKET_SIZE:
        trace(" SQL_ATTR_PACKET_SIZE");
        break;
    case SQL_ATTR_QUIET_MODE:
        trace(" SQL_ATTR_QUIET_MODE");
        break;
    case SQL_ATTR_TRACE:
        trace(" SQL_ATTR_TRACE");
        break;
    case SQL_ATTR_TRACEFILE:
        trace(" SQL_ATTR_TRACEFILE");
        break;
    case SQL_ATTR_TRANSLATE_LIB:
        trace(" SQL_ATTR_TRANSLATE_LIB");
        break;
    case SQL_ATTR_TRANSLATE_OPTION:
        trace(" SQL_ATTR_TRANSLATE_OPTION");
        break;
    case SQL_ATTR_TXN_ISOLATION:
        trace(" SQL_ATTR_TXN_ISOLATION");
        break;
    default:
        trace(" ? %d TODO",Attribute);
        conn->setError(E_HY092);
        return SQL_ERROR;
    }
    return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLGetConnectAttr(SQLHDBC ConnectionHandle,
           SQLINTEGER Attribute, SQLPOINTER Value,
           SQLINTEGER BufferLength, SQLINTEGER *StringLength) {
    trace("SQLGetConnectAttr");
    Connection* conn=Connection::cast(ConnectionHandle);
    if(conn==0) {
        return SQL_INVALID_HANDLE;
    }
    conn->setError(0);
    switch(Attribute) {
    case SQL_ATTR_ACCESS_MODE:
        trace(" SQL_ATTR_ACCESS_MODE");
        if(conn->getReadOnly()) {
            returnInt(Value,StringLength,SQL_MODE_READ_ONLY);
        } else {
            returnInt(Value,StringLength,SQL_MODE_READ_WRITE);
        }
        break;
    case SQL_ATTR_ASYNC_ENABLE:
        trace(" SQL_ATTR_ASYNC_ENABLE");
        returnInt(Value, SQL_ASYNC_ENABLE_OFF);
        break;
    case SQL_ATTR_AUTO_IPD:
        trace(" SQL_ATTR_AUTO_IPD");
        returnInt(Value, SQL_FALSE);
        break;
    case SQL_ATTR_AUTOCOMMIT:
        trace(" SQL_ATTR_AUTOCOMMIT");
        if(conn->getAutoCommit()) {
            returnInt(Value,StringLength,SQL_AUTOCOMMIT_ON);
        } else {
            returnInt(Value,StringLength,SQL_AUTOCOMMIT_OFF);
        }
        break;
    case SQL_ATTR_CONNECTION_DEAD:
        trace(" SQL_ATTR_CONNECTION_DEAD");
        // TODO: should test if connection is active. now say it's active
        returnInt(Value, SQL_CD_FALSE);
        break;
    case SQL_ATTR_CONNECTION_TIMEOUT:
        trace(" SQL_ATTR_CONNECTION_TIMEOUT");
        returnInt(Value, 0);
        break;
    case SQL_ATTR_CURRENT_CATALOG:
        trace(" SQL_ATTR_CURRENT_CATALOG (temp:DATA)");
        returnString(Value, BufferLength, StringLength, "DATA");
        break;
    case SQL_ATTR_LOGIN_TIMEOUT:
        trace(" SQL_ATTR_LOGIN_TIMEOUT");
        returnInt(Value, 0);
        break;
    case SQL_ATTR_METADATA_ID:
        trace(" SQL_ATTR_METADATA_ID");
        // TODO: how catalogs are treated. FALSE: catalog can contain patterns (%)
        returnInt(Value, SQL_FALSE);
        break;
    case SQL_ATTR_ODBC_CURSORS:
        trace(" SQL_ATTR_ODBC_CURSORS");
        returnInt(Value, SQL_CUR_USE_IF_NEEDED);
        break;
    case SQL_ATTR_PACKET_SIZE:
        trace(" SQL_ATTR_PACKET_SIZE");
        // TODO should return correct packet size
        returnInt(Value, 100);
        break;
    case SQL_ATTR_QUIET_MODE:
        trace(" SQL_ATTR_QUIET_MODE");
        // TODO should be a hwnd, 0 means no dialogs, other value means dialogs
        returnInt(Value, 0);
        break;
    case SQL_ATTR_TRACE:
        trace(" SQL_ATTR_TRACE");
        // TODO should support tracing
        returnInt(Value, SQL_OPT_TRACE_OFF);
        break;
    case SQL_ATTR_TRACEFILE:
        trace(" SQL_ATTR_TRACEFILE TODO");
        break;
    case SQL_ATTR_TRANSLATE_LIB:
        trace(" SQL_ATTR_TRANSLATE_LIB TODO");
        break;
    case SQL_ATTR_TRANSLATE_OPTION:
        trace(" SQL_ATTR_TRANSLATE_OPTION TODO");
        break;
    case SQL_ATTR_TXN_ISOLATION:
        trace(" SQL_ATTR_TXN_ISOLATION TODO");
        break;
    default:
        conn->setError(E_HY092);
        trace(" ? %d TODO", Attribute);
        return SQL_ERROR;
    }
    conn->setError(0);
    return SQL_SUCCESS;
}

