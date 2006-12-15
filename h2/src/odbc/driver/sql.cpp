/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 */

#include "h2odbc.h"

SQLRETURN  SQL_API SQLAllocHandle(SQLSMALLINT HandleType,
           SQLHANDLE InputHandle, SQLHANDLE* OutputHandle) {
    trace("SQLAllocHandle");
    if(OutputHandle==0) {
        return SQL_ERROR;
    }
    switch(HandleType) {
    case SQL_HANDLE_ENV:
        trace(" SQL_HANDLE_ENV");
        *OutputHandle=new Environment();
        break;
    case SQL_HANDLE_DBC: {
        trace(" SQL_HANDLE_DBC");
        Environment* env=Environment::cast(InputHandle);
        if(env==0) {
            return SQL_INVALID_HANDLE;
        }
        env->setError(0);
        Connection* conn = env->createConnection();
        *OutputHandle = conn;
//        trace(" use ODBC cursor library if possible");
//        SQLSetConnectAttr(conn, SQL_ATTR_ODBC_CURSORS, (void*)SQL_CUR_USE_ODBC);
        break;
    }
    case SQL_HANDLE_STMT: {
        trace(" SQL_HANDLE_STMT");
        Connection* conn = Connection::cast(InputHandle);
        if(conn==0) {
            return SQL_INVALID_HANDLE;
        }
        conn->setError(0);
        if(conn->isClosed()) {
            conn->setError(E_08003);
            return SQL_ERROR;
        }
        *OutputHandle=new Statement(conn);
        break;
    }
    case SQL_HANDLE_DESC: {
        trace(" SQL_HANDLE_DESC");
        Connection* conn = Connection::cast(InputHandle);
        if(conn==0) {
            return SQL_INVALID_HANDLE;
        }
        conn->setError(0);
        if(conn->isClosed()) {
            conn->setError(E_08003);
            return SQL_ERROR;
        }        
        *OutputHandle=new Descriptor(conn);
        break;
    }
    default:
        trace(" ? %d TODO ", HandleType);
        return SQL_ERROR;
    }
    return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLBindCol(SQLHSTMT StatementHandle, 
           SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType, 
           SQLPOINTER TargetValue, SQLINTEGER BufferLength, 
           SQLINTEGER* StrLen_or_Ind) {
    trace("SQLBindCol");
    Statement* stat=Statement::cast(StatementHandle);
    if(stat==0) {
        return SQL_INVALID_HANDLE;
    }
    stat->setError(0);
    trace(" getAppRowDesc");
    Descriptor* desc = stat->getAppRowDesc();
    if(desc == 0) {
        trace(" desc=null");
        return SQL_ERROR;
    }        
    ColumnNumber--;
    trace(" ColumnNumber=%d", ColumnNumber);
    if(ColumnNumber > stat->getColumnCount()) {
        trace(" columnCount=%d", stat->getColumnCount());
        return SQL_ERROR;
    }
    trace(" getRecord");
    if(ColumnNumber >= desc->getRecordCount()) {
        trace(" wrong column; cols=%d", desc->getRecordCount());
    }
    DescriptorRecord* rec = desc->getRecord(ColumnNumber);
    if(rec == 0) {
        trace(" rec out of range");
        return SQL_ERROR;
    }    
    trace(" setTargetDataType");
    rec->setCDataType(TargetType);
    trace(" setTargetPointer");
    rec->setTargetPointer(TargetValue);
    trace(" setTargetBufferLength");
    rec->setTargetBufferLength(BufferLength);
    trace(" setTargetStatusPointer");
    rec->setTargetStatusPointer(StrLen_or_Ind);
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLBindParameter(
    SQLHSTMT StatementHandle,
    SQLUSMALLINT ParameterNumber,
    SQLSMALLINT InputOutputType,
    SQLSMALLINT ValueType,
    SQLSMALLINT ParameterType,
    SQLUINTEGER ColumnSize,
    SQLSMALLINT DecimalDigits,
    SQLPOINTER ParameterValuePtr,
    SQLINTEGER BufferLength,
    SQLINTEGER* StrLen_or_IndPtr) {
    trace("SQLBindParameter");
    Statement* stat=Statement::cast(StatementHandle);
    if(stat==0) {
        return SQL_INVALID_HANDLE;
    }
    stat->setError(0);
    switch(InputOutputType) {
    case SQL_PARAM_INPUT:
        trace(" SQL_PARAM_INPUT");
        break;
    case SQL_PARAM_INPUT_OUTPUT:
        trace(" SQL_PARAM_INPUT_OUTPUT");
        break;
    case SQL_PARAM_OUTPUT:
        trace(" SQL_PARAM_OUTPUT");
        break;
    default:
        trace(" ? %d TODO ", InputOutputType);
        return SQL_ERROR;
    }
    Descriptor* desc = stat->getAppParamDesc();
    ParameterNumber--;
    while(desc->getRecordCount() <= ParameterNumber) {
        stat->addParameter();
    }
    DescriptorRecord* rec = desc->getRecord(ParameterNumber);
    if(rec==0) {
        trace("  rec=0 TODO");
    }
    if(ValueType == SQL_C_DEFAULT) {
        trace("  SQL_C_DEFAULT");
        ValueType = getDefaultCType(ParameterType);
    }
    trace("  ValueType=%d, ParameterType=%d", ValueType, ParameterType);
    rec->setSqlDataType(ParameterType);
    rec->setCDataType(ValueType);
    rec->setTargetPointer(ParameterValuePtr);
    rec->setTargetBufferLength(ColumnSize);
    rec->setTargetStatusPointer(StrLen_or_IndPtr);    
    return SQL_SUCCESS;
}                        

SQLRETURN  SQL_API SQLCancel(SQLHSTMT StatementHandle) {
    trace("SQLCancel TODO");
    return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLCloseCursor(SQLHSTMT StatementHandle) {
    trace("SQLCloseCursor");
    Statement* stat=Statement::cast(StatementHandle);
    if(stat==0) {
        return SQL_INVALID_HANDLE;
    }
    stat->setError(0);
    stat->closeCursor();
    return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLDriverConnect(
           SQLHDBC ConnectionHandle,
           SQLHWND WindowHandle,
           SQLCHAR* InConnectionString,
           SQLSMALLINT StringLength1,
           SQLCHAR* OutConnectionString,
           SQLSMALLINT BufferLength,
           SQLSMALLINT* StringLength2Ptr,
           SQLUSMALLINT DriverCompletion) {
    trace("SQLDriverConnect");
    Connection* conn=Connection::cast(ConnectionHandle);
    if(conn==0) {
        return SQL_INVALID_HANDLE;
    }
    conn->setError(0);
    
    char connect[1024];
    char url[1024];
    char user[1024];
    char password[1024];
    strcpy(url, "");
    strcpy(user, "");
    strcpy(password, "");
    setString(connect,sizeof(connect),InConnectionString,StringLength1);
    char* start = strstr(connect, "DSN=");
    if(start != 0) {
        char dns[1024];
        strncpy(dns, start+4, strlen(start+4));
        char* end = strstr(dns, ";");
        if(end != 0) {
            *end = 0;
        }
        conn->setDataSourceName(dns);
        SQLGetPrivateProfileString(
            dns, 
            "URL", "", url, MAX_STRING_LEN,
            "ODBC.INI");
        SQLGetPrivateProfileString(
            dns, 
            "User", "", user, MAX_STRING_LEN,
            "ODBC.INI");
        SQLGetPrivateProfileString(
            dns, 
            "Password", "", password, MAX_STRING_LEN,
            "ODBC.INI");
    }
    trace(" url=%s user=%s password=%s", url, user, password);
    trace(" connect=%s DSN=<%s>", connect, conn->getDataSourceName().data());
    strcat(connect,"UID=sa;PWD=;DRIVER=h2odbc");
    
    conn->open(url, user, password);
    if(conn->getError() != 0) {
        return SQL_ERROR;
    }

    returnString(OutConnectionString,BufferLength,StringLength2Ptr,connect);
    trace(" %s",connect);
    return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLConnect(SQLHDBC ConnectionHandle,
           SQLCHAR* ServerName, SQLSMALLINT NameLength1,
           SQLCHAR* UserName, SQLSMALLINT NameLength2,
           SQLCHAR* Authentication, SQLSMALLINT NameLength3) {
    trace("SQLConnect");
    Connection* conn = Connection::cast(ConnectionHandle);
    if(conn==0) {
        return SQL_INVALID_HANDLE;
    }
    conn->setError(0);        
    char name[512];
    char user[512];
    char password[512];
    setString(name,sizeof(name),ServerName,NameLength1);
    setString(user,sizeof(user),UserName,NameLength2);
    setString(password,sizeof(password),Authentication,NameLength3);
    trace(" dns=%s user=%s", name, user);
    conn->setDataSourceName(name);
    conn->open(name,user,password);
    return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLCopyDesc(SQLHDESC SourceDescHandle,
           SQLHDESC TargetDescHandle) {
    trace("SQLCopyDesc TODO");
    return SQL_ERROR;
}

SQLRETURN  SQL_API SQLDescribeCol(SQLHSTMT StatementHandle,
           SQLUSMALLINT ColumnNumber, SQLCHAR* ColumnName,
           SQLSMALLINT BufferLength, SQLSMALLINT* NameLengthPtr,
           SQLSMALLINT* DataTypePtr, SQLUINTEGER* ColumnSizePtr,
           SQLSMALLINT* DecimalDigitsPtr, SQLSMALLINT* NullablePtr) {
    trace("SQLDescribeCol");
    Statement* stat=Statement::cast(StatementHandle);
    if(stat==0) {
        return SQL_INVALID_HANDLE;
    }
    stat->setError(0);  
    Descriptor* desc = stat->getImpRowDesc();
    ColumnNumber--;
    if(ColumnNumber >= desc->getRecordCount()) {
        trace("SQLDescribeCol E_07009 %d", ColumnNumber);
        stat->setError(E_07009);
        return SQL_ERROR;
    }
    trace("  column %d", ColumnNumber);
    DescriptorRecord* rec = desc->getRecord(ColumnNumber);
    returnString((SQLCHAR*)ColumnName, BufferLength, NameLengthPtr, rec->getColumnName().data());
    trace("   =%s", (SQLCHAR*)ColumnName);
    returnSmall(DataTypePtr, (SQLINTEGER*)0, rec->getSqlDataType());
    returnInt(ColumnSizePtr, rec->getDisplaySize());
    trace("   =%d", rec->getDisplaySize());
    returnSmall(DecimalDigitsPtr, (SQLINTEGER*)0, 0);
    returnSmall(NullablePtr, (SQLINTEGER*)0, SQL_NULLABLE_UNKNOWN);
    return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLDisconnect(SQLHDBC ConnectionHandle) {
    trace("SQLDisconnect");
    Connection* conn = Connection::cast(ConnectionHandle);
    if(conn==0) {
        return SQL_INVALID_HANDLE;
    }
    conn->setError(0);
    conn->close();
    return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLEndTran(SQLSMALLINT HandleType, SQLHANDLE Handle,
           SQLSMALLINT CompletionType) {
    trace("SQLEndTran");
    switch(HandleType) {
    case SQL_HANDLE_ENV: {
        trace(" SQL_HANDLE_ENV TODO");
        Environment* env = Environment::cast(Handle);
        if(env==0) {
            return SQL_INVALID_HANDLE;   
        }
        env->setError(0);
        break;
    }
    case SQL_HANDLE_DBC: {
        trace(" SQL_HANDLE_DBC");
        Connection* conn = Connection::cast(Handle);
        if(conn==0) {
            return SQL_INVALID_HANDLE;
        }
        conn->setError(0);    
           switch(CompletionType) {
        case SQL_COMMIT:
            trace("  SQL_COMMIT"); 
            conn->commit();
            break;
        case SQL_ROLLBACK:
            trace("  SQL_ROLLBACK"); 
            conn->rollback();
            break;
        default:
            conn->setError(E_HY012);
            return SQL_ERROR;
            break;
        }
        break;
    }    
    default:
        trace(" ? %d TODO ", HandleType);
        return SQL_ERROR;
    }
    return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLExecDirect(SQLHSTMT StatementHandle,
           SQLCHAR* StatementText, SQLINTEGER TextLength) {
    trace("SQLExecDirect");
    Statement* stat=Statement::cast(StatementHandle);
    if(stat==0) {
        return SQL_INVALID_HANDLE;
    }
    stat->setError(0);        
    char sql[512];
    setString(sql,sizeof(sql),StatementText,TextLength);
    trace(" %s", sql);
    if(stat->execute(sql)) {
        return SQL_SUCCESS;
    } else {
        return SQL_ERROR;
    }
}

SQLRETURN  SQL_API SQLExecute(SQLHSTMT StatementHandle) {
    trace("SQLExecute");
    Statement* stat=Statement::cast(StatementHandle);
    if(stat==0) {
        return SQL_INVALID_HANDLE;
    }
    stat->setError(0);
    if(stat->executePrepared()) {
        return SQL_SUCCESS;
    } else {
        return SQL_ERROR;
    }
}

SQLRETURN  SQL_API SQLFetch(SQLHSTMT StatementHandle) {
    trace("SQLFetch");
    Statement* stat=Statement::cast(StatementHandle);
    if(stat==0) {
        return SQL_INVALID_HANDLE;
    }
    stat->setError(0);
    if(!stat->next()) {
        trace(" SQL_NO_DATA");
        return SQL_NO_DATA;
    } else {
        trace(" SQL_SUCCESS");
        return SQL_SUCCESS;
    }
}

SQLRETURN  SQL_API SQLFetchScroll(SQLHSTMT StatementHandle,
           SQLSMALLINT FetchOrientation, SQLINTEGER FetchOffset) {
    trace("SQLFetchScroll");
    Statement* stat=Statement::cast(StatementHandle);
    if(stat==0) {
        return SQL_INVALID_HANDLE;
    }
    stat->setError(0);
    switch(FetchOrientation) {
    case SQL_FETCH_NEXT: {
        trace(" SQL_FETCH_NEXT");
        if(stat->next()) {
            return SQL_SUCCESS;
        }
        return SQL_NO_DATA;
    }
    case SQL_FETCH_PRIOR:
        trace(" SQL_FETCH_PRIOR TODO");
        return SQL_ERROR;
        break;
    case SQL_FETCH_RELATIVE:
        trace(" SQL_FETCH_RELATIVE TODO");
        return SQL_ERROR;        
        break;
    case SQL_FETCH_ABSOLUTE:
        trace(" SQL_FETCH_ABSOLUTE TODO");
        return SQL_ERROR;        
        break;
    case SQL_FETCH_FIRST:
        trace(" SQL_FETCH_FIRST TODO");
        return SQL_ERROR;        
        break;
    case SQL_FETCH_LAST:
        trace(" SQL_FETCH_LAST TODO");
        return SQL_ERROR;        
        break;
    case SQL_FETCH_BOOKMARK:
        trace(" SQL_FETCH_BOOKMARK TODO");
        return SQL_ERROR;        
        break;
    default:
        trace(" ? %d", FetchOrientation);
        return SQL_ERROR;        
        break;
    }
    return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLFreeHandle(SQLSMALLINT HandleType, SQLHANDLE Handle) {
    trace("SQLFreeHandle");
    switch(HandleType) {
    case SQL_HANDLE_ENV: {
        trace(" SQL_HANDLE_ENV");
        Environment* env=Environment::cast(Handle);
        if(env==0) {
            return SQL_INVALID_HANDLE;
        }
        env->setError(0);        
        if(env->getOpenConnectionCount()>0) {
            return SQL_ERROR;
        }
        delete env;
        break;
    }
    case SQL_HANDLE_DBC: {
        trace(" SQL_HANDLE_DBC");
        Connection* conn=Connection::cast(Handle);
        if(conn==0) {
            return SQL_INVALID_HANDLE;
        }
        conn->setError(0);    
        if(!conn->isClosed()) {
            return SQL_ERROR;
        }
        Environment* env=conn->getEnvironment();
        env->closeConnection(conn);
        break;
    }
    case SQL_HANDLE_STMT: {
        trace(" SQL_HANDLE_STMT");
        Statement* stat=Statement::cast(Handle);
        if(stat==0) {
            return SQL_INVALID_HANDLE;
        }
        stat->setError(0);            
        delete stat;
        break;
    }
    case SQL_HANDLE_DESC: {
        trace(" SQL_HANDLE_DESC");
        Descriptor* desc=Descriptor::cast(Handle);
        if(desc==0) {
            return SQL_INVALID_HANDLE;
        }
        desc->setError(0);            
        delete desc;
        break;
    }
    default:
        trace(" ? %d TODO", HandleType);
        return SQL_INVALID_HANDLE;
    }
    return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLFreeStmt(SQLHSTMT StatementHandle,
           SQLUSMALLINT Option) {
    trace("SQLFreeStmt");
    Statement* stat=Statement::cast(StatementHandle);
    if(stat==0) {
        return SQL_INVALID_HANDLE;
    }
    stat->setError(0);        
    switch(Option) {
    case SQL_CLOSE:
        trace(" SQL_CLOSE");
        stat->closeCursor();
        break;
    case SQL_UNBIND:
        trace(" SQL_UNBIND TODO");
        break;
    case SQL_RESET_PARAMS:
        trace(" SQL_RESET_PARAMS TODO");
        break;
    default:
        trace("%d TODO",Option);
        return SQL_ERROR;
    }
    return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLGetCursorName(SQLHSTMT StatementHandle,
           SQLCHAR* CursorName, SQLSMALLINT BufferLength,
           SQLSMALLINT* NameLength) {
    trace("SQLGetCursorName TODO");
    Statement* stat=Statement::cast(StatementHandle);
    if(stat==0) {
        return SQL_INVALID_HANDLE;
    }
    stat->setError(E_IM001);        
    return SQL_ERROR;
}

SQLRETURN  SQL_API SQLGetData(SQLHSTMT StatementHandle,
           SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType,
           SQLPOINTER TargetValue, SQLINTEGER BufferLength,
           SQLINTEGER* StrLen_or_Ind) {
    trace("SQLGetData col=%d", ColumnNumber, TargetType);
    Statement* stat=Statement::cast(StatementHandle);
    if(stat==0) {
        return SQL_INVALID_HANDLE;
    }
    stat->setError(0);
    Descriptor* desc = stat->getImpRowDesc();
    ColumnNumber--;
    if(ColumnNumber > stat->getColumnCount()) {
        trace(" columnCount=%d", stat->getColumnCount());
        return SQL_ERROR;
    }
    DescriptorRecord* rec = desc->getRecord(ColumnNumber);
    if(rec->wasNull()) {
        if(StrLen_or_Ind==0) {
            trace(" wasNull error");       
            stat->setError(E_22002);
            return SQL_ERROR;
        }
        trace(" wasNull");
        *StrLen_or_Ind=SQL_NULL_DATA;
        return SQL_SUCCESS;
    }
    if(TargetType == SQL_C_DEFAULT) {
        TargetType = getDefaultCType(rec->getSqlDataType());
        trace("  SQL_C_DEFAULT set to %d", TargetType); 
    }
    switch(TargetType) {
    case SQL_CHAR:
    case SQL_VARCHAR:
        trace("  SQL_CHAR / SQL_VARCHAR"); 
        returnString((SQLCHAR*)TargetValue, BufferLength, StrLen_or_Ind, rec->getString());
        break;
    case SQL_INTEGER:
        trace("  SQL_INTEGER"); 
        returnInt(TargetValue, StrLen_or_Ind, rec->getInt());
        break;
    case SQL_SMALLINT:
        trace("  SQL_SMALLINT");         
        returnSmall((SQLSMALLINT*)TargetValue, StrLen_or_Ind, rec->getInt());
        break;
    default:
        trace("  type=%d TODO", TargetType);
    }
    return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLGetDescField(SQLHDESC DescriptorHandle,
           SQLSMALLINT RecNumber, SQLSMALLINT FieldIdentifier,
           SQLPOINTER Value, SQLINTEGER BufferLength,
           SQLINTEGER* StringLength) {
    trace("SQLGetDescField TODO");
    Descriptor* desc = Descriptor::cast(DescriptorHandle);
    if(desc==0) {
        return SQL_INVALID_HANDLE;
    }    
    desc->setError(0);    
    RecNumber--;
    if(RecNumber < 0 || desc->getRecordCount() >= RecNumber) {
        desc->setError(E_07009);
        return SQL_ERROR;
    }
 
    return SQL_ERROR;
}

SQLRETURN  SQL_API SQLGetDescRec(SQLHDESC DescriptorHandle,
           SQLSMALLINT RecNumber, SQLCHAR* Name,
           SQLSMALLINT BufferLength, SQLSMALLINT* StringLength,
           SQLSMALLINT* Type, SQLSMALLINT* SubType, 
           SQLINTEGER* Length, SQLSMALLINT* Precision, 
           SQLSMALLINT* Scale, SQLSMALLINT* Nullable) {
    trace("SQLGetDescRec TODO");
    return SQL_ERROR;
}

SQLRETURN  SQL_API SQLGetEnvAttr(SQLHENV EnvironmentHandle,
           SQLINTEGER Attribute, SQLPOINTER Value,
           SQLINTEGER BufferLength, SQLINTEGER* StringLength) {
    trace("SQLGetEnvAttr %d",Attribute);
    Environment* env=Environment::cast(EnvironmentHandle);
    if(env==0) {
        return SQL_INVALID_HANDLE;
    }    
    env->setError(0);    
    switch(Attribute) {
    case SQL_ATTR_ODBC_VERSION:
        trace(" SQL_ATTR_ODBC_VERSION");
        returnInt(Value,StringLength,env->getBehavior());
        break;
    case SQL_ATTR_CONNECTION_POOLING:
        trace(" SQL_ATTR_CONNECTION_POOLING");
        break;
    case SQL_ATTR_CP_MATCH:
        trace(" SQL_ATTR_CP_MATCH");
        break;
    case SQL_ATTR_OUTPUT_NTS:
        trace(" SQL_ATTR_OUTPUT_NTS");
        returnInt(Value,StringLength,SQL_TRUE);
        break;
    default:
        trace(" %d",Attribute);
        return SQL_ERROR;
    }
    return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLNumResultCols(SQLHSTMT StatementHandle,
           SQLSMALLINT* ColumnCount) {
    trace("SQLNumResultCols");
    Statement* stat=Statement::cast(StatementHandle);
    if(stat==0) {
        return SQL_INVALID_HANDLE;
    }    
    stat->setError(0);    
    int count = stat->getColumnCount();
    returnInt(ColumnCount, count);
    trace(" %d", count);
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLNativeSql(
    SQLHDBC ConnectionHandle,
    SQLCHAR* InStatementText,
    SQLINTEGER TextLength1,
    SQLCHAR* OutStatementText,
    SQLINTEGER BufferLength,
    SQLINTEGER* TextLength2Ptr) {
    trace("SQLNativeSql");
    Connection* conn=Connection::cast(ConnectionHandle);
    if(conn==0) {
        return SQL_INVALID_HANDLE;
    }    
    conn->setError(0);        
    char sql[512];
    setString(sql,sizeof(sql),InStatementText,TextLength1);
    string translated = conn->getNativeSQL(sql);
    returnString(OutStatementText,BufferLength,TextLength2Ptr,translated.data());
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLNumParams(
    SQLHSTMT StatementHandle,
    SQLSMALLINT* ParameterCountPtr ) {
    trace("SQLNumParams");
    Statement* stat=Statement::cast(StatementHandle);
    if(stat==0) {
        return SQL_INVALID_HANDLE;
    }    
    stat->setError(0);
    int params = stat->getParametersCount();
    returnInt(ParameterCountPtr, params);
    return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLParamData(SQLHSTMT StatementHandle,
           SQLPOINTER* Value) {
    trace("SQLParamData TODO");
    return SQL_ERROR;
}

SQLRETURN  SQL_API SQLPrepare(SQLHSTMT StatementHandle,
           SQLCHAR* StatementText, SQLINTEGER TextLength) {
    trace("SQLPrepare");
    // TODO should support longer statements
    char sql[512];
    setString(sql,sizeof(sql),StatementText,TextLength);
    trace(" %s", sql);
    Statement* stat=Statement::cast(StatementHandle);
    if(stat==0) {
        return SQL_INVALID_HANDLE;
    }    
    stat->setError(0);        
    if(stat->prepare(sql)) {
        return SQL_SUCCESS;
    } else {
        return SQL_ERROR;
    }
}

SQLRETURN  SQL_API SQLPutData(SQLHSTMT StatementHandle,
           SQLPOINTER Data, SQLINTEGER StrLen_or_Ind) {
    trace("SQLPutData TODO");
    return SQL_ERROR;
}

SQLRETURN  SQL_API SQLRowCount(SQLHSTMT StatementHandle, 
           SQLINTEGER* RowCount) {
    trace("SQLRowCount");
    Statement* stat=Statement::cast(StatementHandle);
    if(stat==0) {
        return SQL_INVALID_HANDLE;
    }    
    stat->setError(0);    
    trace(" %d", stat->getUpdateCount());
    returnInt(RowCount, (SQLINTEGER*)0, stat->getUpdateCount());
    return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLSetCursorName(SQLHSTMT StatementHandle,
           SQLCHAR* CursorName, SQLSMALLINT NameLength) {
    trace("SQLSetCursorName TODO");
    return SQL_ERROR;
}

SQLRETURN  SQL_API SQLSetDescField(SQLHDESC DescriptorHandle,
           SQLSMALLINT RecNumber, SQLSMALLINT FieldIdentifier,
           SQLPOINTER Value, SQLINTEGER BufferLength) {
    trace("SQLSetDescField TODO");
    return SQL_ERROR;
}

SQLRETURN  SQL_API SQLSetDescRec(SQLHDESC DescriptorHandle,
           SQLSMALLINT RecNumber, SQLSMALLINT Type,
           SQLSMALLINT SubType, SQLINTEGER Length,
           SQLSMALLINT Precision, SQLSMALLINT Scale,
           SQLPOINTER Data, SQLINTEGER* StringLength,
           SQLINTEGER* Indicator) {
    trace("SQLSetDescRec TODO");
    return SQL_ERROR;
}

SQLRETURN  SQL_API SQLSetEnvAttr(SQLHENV EnvironmentHandle,
           SQLINTEGER Attribute, SQLPOINTER Value,
           SQLINTEGER StringLength) {
    trace("SQLSetEnvAttr");
    Environment* env=Environment::cast(EnvironmentHandle);
    if(env==0) {
        return SQL_INVALID_HANDLE;
    }    
    env->setError(0);        
    switch(Attribute) {
    case SQL_ATTR_ODBC_VERSION:
        env->setBehavior((int)Value);
        trace(" SQL_ATTR_ODBC_VERSION");
        return SQL_SUCCESS;
    case SQL_ATTR_CONNECTION_POOLING:
        trace(" SQL_ATTR_CONNECTION_POOLING TODO");
    case SQL_ATTR_CP_MATCH:
        trace(" SQL_ATTR_CP_MATCH TODO");
    case SQL_ATTR_OUTPUT_NTS:
        trace(" SQL_ATTR_OUTPUT_NTS TODO");
        if((int)Value==SQL_TRUE) {
            return SQL_ERROR;
        } else {
            return SQL_ERROR;
        }
    default:
        trace(" ? %d TODO",Attribute);
        return SQL_ERROR;
    }
}

