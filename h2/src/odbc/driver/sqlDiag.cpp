/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 */

#include "h2odbc.h"

SQLRETURN  SQL_API SQLGetDiagField(SQLSMALLINT HandleType, SQLHANDLE Handle,
           SQLSMALLINT RecNumber, SQLSMALLINT DiagIdentifier,
           SQLPOINTER DiagInfo, SQLSMALLINT BufferLength,
           SQLSMALLINT* StringLength) {
    trace("SQLGetDiagField");
    if(RecNumber < 0) {
        return SQL_ERROR;
    }    
    if(RecNumber > 1) {
        trace(" SQL_NO_DATA");
        return SQL_NO_DATA;
    }    
    switch(HandleType) {
    case SQL_HANDLE_ENV: {
        trace(" SQL_HANDLE_ENV TODO");
        Environment* env=Environment::cast(Handle);
        if(env==0) {
            return SQL_INVALID_HANDLE;
        }
        return SQL_NO_DATA;
    }
    case SQL_HANDLE_DBC: {
        trace(" SQL_HANDLE_DBC TODO");
        Connection* conn=Connection::cast(Handle);
        if(conn==0) {
            return SQL_INVALID_HANDLE;
        }
        const char* error = conn->getError();
        switch(DiagIdentifier) {
        case SQL_DIAG_NUMBER:
            trace("  SQL_DIAG_NUMBER %d", (error ? 1 : 0));
            returnInt(DiagInfo, error ? 1 : 0);
            break;
        case SQL_DIAG_MESSAGE_TEXT:
            trace("  SQL_DIAG_MESSAGE_TEXT");
            if(error==0) {
                return SQL_NO_DATA;
            }
            returnString((SQLCHAR*)DiagInfo, BufferLength, StringLength, error + 6);
            break;
        case SQL_DIAG_NATIVE:
            trace("  SQL_DIAG_NATIVE");
            if(error==0) {
                return SQL_NO_DATA;
            }            
            returnInt(DiagInfo, 0);
            break;
        case SQL_DIAG_SERVER_NAME:
            trace("  SQL_DIAG_SERVER_NAME %s", conn->getDataSourceName().data());
            returnString((SQLCHAR*)DiagInfo, BufferLength, StringLength, conn->getDataSourceName().data());
            break;
        case SQL_DIAG_SQLSTATE:
            trace("  SQL_DIAG_SQLSTATE");
            if(error==0) {
                return SQL_NO_DATA;
            }            
            returnString((SQLCHAR*)DiagInfo, max((int)BufferLength, 6), StringLength, error);
            break;
        case SQL_DIAG_SUBCLASS_ORIGIN:
            trace("  SQL_DIAG_SUBCLASS_ORIGIN");
            if(error==0) {
                return SQL_NO_DATA;
            }            
            returnString((SQLCHAR*)DiagInfo, BufferLength, StringLength, "ODBC 3.0");
            break;
        default:
            trace("  ? %d", DiagIdentifier);
            return SQL_ERROR;
        }
        return SQL_SUCCESS;
    }
    case SQL_HANDLE_STMT: {
        trace(" SQL_HANDLE_STMT");
        Statement* stat=Statement::cast(Handle);
        if(stat==0) {
            return SQL_INVALID_HANDLE;
        }
        char* error = stat->getError();
        switch(DiagIdentifier) {
        case SQL_DIAG_CURSOR_ROW_COUNT:
            trace("  SQL_DIAG_CURSOR_ROW_COUNT");
            returnInt(DiagInfo, 1);
            break;
        case SQL_DIAG_DYNAMIC_FUNCTION:
            trace("  SQL_DIAG_DYNAMIC_FUNCTION");
            returnString((SQLCHAR*)DiagInfo, BufferLength, StringLength, stat->getSQL());
            break;
        case SQL_DIAG_DYNAMIC_FUNCTION_CODE:
            trace("  SQL_DIAG_DYNAMIC_FUNCTION_CODE");
            returnInt(DiagInfo, SQL_DIAG_UNKNOWN_STATEMENT);
            break;
        case SQL_DIAG_NUMBER:
            trace("  SQL_DIAG_NUMBER %d", (error ? 1 : 0));
            returnInt(DiagInfo, error ? 1 : 0);
            break;
        case SQL_DIAG_RETURNCODE:
            trace("  SQL_DIAG_RETURNCODE TODO");
            break;
        case SQL_DIAG_ROW_COUNT:
            trace("  SQL_DIAG_ROW_COUNT");
            returnInt(DiagInfo, stat->getUpdateCount());
            break;
        case SQL_DIAG_CLASS_ORIGIN:
            trace("  SQL_DIAG_CLASS_ORIGIN TODO");
            returnString((SQLCHAR*)DiagInfo, BufferLength, StringLength, "ISO 9075");
            break;
        case SQL_DIAG_COLUMN_NUMBER:
            trace("  SQL_DIAG_COLUMN_NUMBER");
            returnInt(DiagInfo, stat->getColumnCount());
            break;
        case SQL_DIAG_CONNECTION_NAME:
            trace("  SQL_DIAG_CONNECTION_NAME");
            returnString((SQLCHAR*)DiagInfo, BufferLength, StringLength, "");
            break;
        case SQL_DIAG_MESSAGE_TEXT:
            trace("  SQL_DIAG_MESSAGE_TEXT");
            if(error==0) {
                return SQL_NO_DATA;
            }            
            returnString((SQLCHAR*)DiagInfo, BufferLength, StringLength, error + 6);
            break;
        case SQL_DIAG_NATIVE:
            trace("  SQL_DIAG_NATIVE");
            if(error==0) {
                return SQL_NO_DATA;
            }            
            returnInt(DiagInfo, 0);
            break;
        case SQL_DIAG_ROW_NUMBER:
            trace("  SQL_DIAG_ROW_NUMBER");
            returnInt(DiagInfo, SQL_ROW_NUMBER_UNKNOWN);
            break;
        case SQL_DIAG_SERVER_NAME:
            trace("  SQL_DIAG_SERVER_NAME");
            returnString((SQLCHAR*)DiagInfo, BufferLength, StringLength, "");
            break;
        case SQL_DIAG_SQLSTATE:
            trace("  SQL_DIAG_SQLSTATE");
            if(error==0) {
                return SQL_NO_DATA;
            }            
            returnString((SQLCHAR*)DiagInfo, max((int)BufferLength, 6), StringLength, error);
            break;
        case SQL_DIAG_SUBCLASS_ORIGIN:
            trace("  SQL_DIAG_SUBCLASS_ORIGIN");
            if(error==0) {
                return SQL_NO_DATA;
            }            
            returnString((SQLCHAR*)DiagInfo, BufferLength, StringLength, "ODBC 3.0");
            break;
        default:
            trace("  ? %d TODO", DiagIdentifier);
            return SQL_ERROR;
        }
        return SQL_SUCCESS;;
    }
    case SQL_HANDLE_DESC: {
        trace(" SQL_HANDLE_DESC");
        Descriptor* desc=Descriptor::cast(Handle);
        if(desc==0) {
            return SQL_INVALID_HANDLE;
        }
        desc->setError(0);
        return SQL_NO_DATA;
    }
    default:
        trace(" ? %d",HandleType);
        return SQL_INVALID_HANDLE;
    }
}

SQLRETURN  SQL_API SQLGetDiagRec(SQLSMALLINT HandleType, SQLHANDLE Handle,
           SQLSMALLINT RecNumber, SQLCHAR *Sqlstate,
           SQLINTEGER *NativeError, SQLCHAR *MessageText,
           SQLSMALLINT BufferLength, SQLSMALLINT *TextLength) {
    trace("SQLGetDiagRec %d",RecNumber);
    if(RecNumber < 0) {
        return SQL_ERROR;
    }
    if(RecNumber > 1) {
        trace(" SQL_NO_DATA");
        return SQL_NO_DATA;
    }
    if(BufferLength < 0) {
        return SQL_ERROR;
    }
    char* error = 0;
    switch(HandleType) {
    case SQL_HANDLE_ENV: {
        trace(" SQL_HANDLE_ENV");
        Environment* env=Environment::cast(Handle);
        if(env==0) {
            trace(" SQL_INVALID_HANDLE");
            return SQL_INVALID_HANDLE;
        }
        error = env->getError();
        break;
    }
    case SQL_HANDLE_DBC: {
        trace(" SQL_HANDLE_DBC");
        Connection* conn=Connection::cast(Handle);
        if(conn==0) {
            trace(" SQL_INVALID_HANDLE");
            return SQL_INVALID_HANDLE;
        }    
        error = conn->getError();          
        break;
    }
    case SQL_HANDLE_STMT: {
        trace(" SQL_HANDLE_STMT");
        Statement* stat=Statement::cast(Handle);
        if(stat==0) {
            trace(" SQL_INVALID_HANDLE");
            return SQL_INVALID_HANDLE;
        }    
        error = stat->getError();
        break;
    }
    case SQL_HANDLE_DESC: {
        trace(" SQL_HANDLE_DESC");
        Descriptor* desc=Descriptor::cast(Handle);
        if(desc==0) {
            trace(" SQL_INVALID_HANDLE");
            return SQL_INVALID_HANDLE;
        }    
        error = desc->getError();         
        break;
    }
    default:
        trace(" ? %d TODO", HandleType);        
        return SQL_INVALID_HANDLE;
    }
    if(error == 0) {
        trace(" SQL_NO_DATA");
        return SQL_NO_DATA;
    } else {
        returnString(Sqlstate, 6, 0, error);
        returnString(MessageText, BufferLength, TextLength, error + 6);
        if(NativeError != 0) {
            *NativeError = 0;
        }
        trace(" SQL_SUCCESS %s", MessageText);
        return SQL_SUCCESS;
    }
}
