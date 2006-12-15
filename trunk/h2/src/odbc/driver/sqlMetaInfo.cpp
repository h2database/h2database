/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 */

#include "h2odbc.h"

SQLRETURN  SQL_API SQLColumns(SQLHSTMT StatementHandle,
           SQLCHAR* CatalogName, SQLSMALLINT NameLength1,
           SQLCHAR* SchemaName, SQLSMALLINT NameLength2,
           SQLCHAR* TableName, SQLSMALLINT NameLength3,
           SQLCHAR* ColumnName, SQLSMALLINT NameLength4) {
    trace("SQLColumns");
    Statement* stat=Statement::cast(StatementHandle);
    if(stat==0) {
        return SQL_INVALID_HANDLE;
    }
    stat->setError(0);    
    char catalog[512];
    setString(catalog,sizeof(catalog),CatalogName,NameLength1);
    char schema[512];
    setString(schema,sizeof(schema),SchemaName,NameLength2);
    char table[512];
    setString(table,sizeof(table),TableName,NameLength3);
    char column[512];
    setString(column,sizeof(column),ColumnName,NameLength4);
    trace(" catalog=%s schema=%s table=%s column=%s",catalog,schema,table,column);
    stat->getMetaColumns(catalog, schema, table, column);
    return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLSpecialColumns(SQLHSTMT StatementHandle,
           SQLUSMALLINT IdentifierType, SQLCHAR* CatalogName,
           SQLSMALLINT NameLength1, SQLCHAR* SchemaName,
           SQLSMALLINT NameLength2, SQLCHAR* TableName,
           SQLSMALLINT NameLength3, SQLUSMALLINT Scope,
           SQLUSMALLINT Nullable) {
    trace("SQLSpecialColumns");
    Statement* stat=Statement::cast(StatementHandle);
    if(stat==0) {
        return SQL_INVALID_HANDLE;
    }
    stat->setError(0);
    char catalog[512];
    setString(catalog,sizeof(catalog),CatalogName,NameLength1);
    char schema[512];
    setString(schema,sizeof(schema),SchemaName,NameLength2);
    char table[512];
    setString(table,sizeof(table),TableName,NameLength3);
    switch(IdentifierType) {
    case SQL_BEST_ROWID: {
        trace(" SQL_BEST_ROWID");
        bool nullable = Nullable == SQL_NULLABLE;
        stat->getMetaBestRowIdentifier(catalog, schema, table, Scope, nullable);
        break;
    }
    case SQL_ROWVER: {
        trace(" SQL_ROWVER");
        stat->getMetaVersionColumns(catalog, schema, table);
        break;
    }
    default:
        stat->setError(E_HY097);
        return SQL_ERROR;
    }    
    return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLStatistics(SQLHSTMT StatementHandle,
           SQLCHAR* CatalogName, SQLSMALLINT NameLength1,
           SQLCHAR* SchemaName, SQLSMALLINT NameLength2,
           SQLCHAR* TableName, SQLSMALLINT NameLength3,
           SQLUSMALLINT Unique, SQLUSMALLINT Reserved) {
    trace("SQLStatistics");
    Statement* stat=Statement::cast(StatementHandle);
    if(stat==0) {
        return SQL_INVALID_HANDLE;
    }
    stat->setError(0);
    char catalog[512];
    setString(catalog,sizeof(catalog),CatalogName,NameLength1);
    char schema[512];
    setString(schema,sizeof(schema),SchemaName,NameLength2);
    char table[512];
    setString(table,sizeof(table),TableName,NameLength3);
    bool unique = Unique == SQL_INDEX_UNIQUE;
    bool approximate = Reserved == SQL_QUICK;
    stat->getMetaIndexInfo(catalog, schema, table, unique, approximate);
    return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLTables(SQLHSTMT StatementHandle,
           SQLCHAR* CatalogName, SQLSMALLINT NameLength1,
           SQLCHAR* SchemaName, SQLSMALLINT NameLength2,
           SQLCHAR* TableName, SQLSMALLINT NameLength3,
           SQLCHAR* TableType, SQLSMALLINT NameLength4) {
    trace("SQLTables");
    Statement* stat=Statement::cast(StatementHandle);
    if(stat==0) {
        return SQL_INVALID_HANDLE;
    }    
    stat->setError(0);    
    char catalog[512];
    setString(catalog,sizeof(catalog),CatalogName,NameLength1);
    char schema[512];
    setString(schema,sizeof(schema),SchemaName,NameLength2);
    char table[512];
    setString(table,sizeof(table),TableName,NameLength3);
    char tabletypes[512];
    setString(tabletypes,sizeof(tabletypes),TableType,NameLength4);
    trace(" catalog=%s schema=%s table=%s tabletypes=%s",catalog, schema, table, tabletypes);
    stat->getMetaTables(catalog, schema, table, tabletypes);
    return SQL_SUCCESS;
}

SQLRETURN  SQL_API SQLGetTypeInfo(SQLHSTMT StatementHandle,
           SQLSMALLINT DataType) {
    trace("SQLGetTypeInfo");
    Statement* stat=Statement::cast(StatementHandle);
    if(stat==0) {
        return SQL_INVALID_HANDLE;
    }    
    stat->setError(0);    
    switch(DataType) {
    case SQL_ALL_TYPES:
        trace(" SQL_ALL_TYPES");
        stat->getMetaTypeInfoAll();
        break;
        /*
    case SQL_CHAR:
        trace(" SQL_CHAR");
        break;
    case SQL_VARCHAR:
        trace(" SQL_VARCHAR");
        break;
    case SQL_LONGVARCHAR:
        trace(" SQL_LONGVARCHAR");
        break;
    case SQL_WCHAR:
        trace(" SQL_WCHAR");
        break;
    case SQL_WVARCHAR:
        trace(" SQL_WVARCHAR");
        break;
    case SQL_WLONGVARCHAR:
        trace(" SQL_WLONGVARCHAR");
        break;
    case SQL_DECIMAL:
        trace(" SQL_DECIMAL");
        break;
    case SQL_NUMERIC:
        trace(" SQL_NUMERIC");
        break;
    case SQL_INTEGER:
        trace(" SQL_INTEGER");
        break;
    case SQL_BINARY:
        trace(" SQL_BINARY");
        break;
    case SQL_SMALLINT:
        trace(" SQL_SMALLINT");
        break;
    case SQL_REAL:
        trace(" SQL_REAL");
        break;
    case SQL_FLOAT:
        trace(" SQL_FLOAT");
        break;
    case SQL_DOUBLE:
        trace(" SQL_DOUBLE");
        break;
    case SQL_BIT:
        trace(" SQL_BIT");
        break;
    case SQL_TINYINT:
        trace(" SQL_TINYINT");
        break;
    case SQL_BIGINT:
        trace(" SQL_BIGINT");
        break;
    case SQL_VARBINARY:
        trace(" SQL_VARBINARY");
        break;
    case SQL_LONGVARBINARY:
        trace(" SQL_LONGVARBINARY");
        break;
    case SQL_TYPE_DATE:
        trace(" SQL_TYPE_DATE");
        break;
    case SQL_TYPE_TIME:
        trace(" SQL_TYPE_TIME");
        break;
    case SQL_TYPE_TIMESTAMP:
        trace(" SQL_TYPE_TIMESTAMP");
        break;
    case SQL_INTERVAL_MONTH:
    case SQL_INTERVAL_YEAR:
    case SQL_INTERVAL_YEAR_TO_MONTH:
    case SQL_INTERVAL_DAY:
    case SQL_INTERVAL_HOUR:
    case SQL_INTERVAL_MINUTE:
    case SQL_INTERVAL_SECOND:
    case SQL_INTERVAL_DAY_TO_HOUR:
    case SQL_INTERVAL_DAY_TO_MINUTE:
    case SQL_INTERVAL_DAY_TO_SECOND:
    case SQL_INTERVAL_HOUR_TO_MINUTE:
    case SQL_INTERVAL_HOUR_TO_SECOND:
    case SQL_INTERVAL_MINUTE_TO_SECOND:
        trace(" SQL_INTERVAL_ %d",DataType);
        break;
    case SQL_GUID:
        trace(" SQL_GUID");
        break;
        */
    default:
        trace(" type=%d", DataType);
        stat->getMetaTypeInfo(DataType);
        break;
    }
    return SQL_SUCCESS;
}

