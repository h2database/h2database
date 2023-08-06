/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc.meta;

import java.io.IOException;
import java.util.ArrayList;

import org.h2.api.ErrorCode;
import org.h2.engine.SessionRemote;
import org.h2.message.DbException;
import org.h2.mode.DefaultNullOrdering;
import org.h2.result.ResultInterface;
import org.h2.result.ResultRemote;
import org.h2.value.Transfer;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;
import org.h2.value.ValueVarchar;

/**
 * Remote implementation of database meta information.
 */
public class DatabaseMetaRemote extends DatabaseMeta {

    static final int DEFAULT_NULL_ORDERING = 0;

    static final int GET_DATABASE_PRODUCT_VERSION = 1;

    static final int GET_SQL_KEYWORDS = 2;

    static final int GET_NUMERIC_FUNCTIONS = 3;

    static final int GET_STRING_FUNCTIONS = 4;

    static final int GET_SYSTEM_FUNCTIONS = 5;

    static final int GET_TIME_DATE_FUNCTIONS = 6;

    static final int GET_SEARCH_STRING_ESCAPE = 7;

    static final int GET_PROCEDURES_3 = 8;

    static final int GET_PROCEDURE_COLUMNS_4 = 9;

    static final int GET_TABLES_4 = 10;

    static final int GET_SCHEMAS = 11;

    static final int GET_CATALOGS = 12;

    static final int GET_TABLE_TYPES = 13;

    static final int GET_COLUMNS_4 = 14;

    static final int GET_COLUMN_PRIVILEGES_4 = 15;

    static final int GET_TABLE_PRIVILEGES_3 = 16;

    static final int GET_BEST_ROW_IDENTIFIER_5 = 17;

    static final int GET_VERSION_COLUMNS_3 = 18;

    static final int GET_PRIMARY_KEYS_3 = 19;

    static final int GET_IMPORTED_KEYS_3 = 20;

    static final int GET_EXPORTED_KEYS_3 = 21;

    static final int GET_CROSS_REFERENCE_6 = 22;

    static final int GET_TYPE_INFO = 23;

    static final int GET_INDEX_INFO_5 = 24;

    static final int GET_UDTS_4 = 25;

    static final int GET_SUPER_TYPES_3 = 26;

    static final int GET_SUPER_TABLES_3 = 27;

    static final int GET_ATTRIBUTES_4 = 28;

    static final int GET_DATABASE_MAJOR_VERSION = 29;

    static final int GET_DATABASE_MINOR_VERSION = 30;

    static final int GET_SCHEMAS_2 = 31;

    static final int GET_FUNCTIONS_3 = 32;

    static final int GET_FUNCTION_COLUMNS_4 = 33;

    static final int GET_PSEUDO_COLUMNS_4 = 34;

    private final SessionRemote session;

    private final ArrayList<Transfer> transferList;

    public DatabaseMetaRemote(SessionRemote session, ArrayList<Transfer> transferList) {
        this.session = session;
        this.transferList = transferList;
    }

    @Override
    public DefaultNullOrdering defaultNullOrdering() {
        ResultInterface result = executeQuery(DEFAULT_NULL_ORDERING);
        result.next();
        return DefaultNullOrdering.valueOf(result.currentRow()[0].getInt());
    }

    @Override
    public String getDatabaseProductVersion() {
        ResultInterface result = executeQuery(GET_DATABASE_PRODUCT_VERSION);
        result.next();
        return result.currentRow()[0].getString();
    }

    @Override
    public String getSQLKeywords() {
        ResultInterface result = executeQuery(GET_SQL_KEYWORDS);
        result.next();
        return result.currentRow()[0].getString();
    }

    @Override
    public String getNumericFunctions() {
        ResultInterface result = executeQuery(GET_NUMERIC_FUNCTIONS);
        result.next();
        return result.currentRow()[0].getString();
    }

    @Override
    public String getStringFunctions() {
        ResultInterface result = executeQuery(GET_STRING_FUNCTIONS);
        result.next();
        return result.currentRow()[0].getString();
    }

    @Override
    public String getSystemFunctions() {
        ResultInterface result = executeQuery(GET_SYSTEM_FUNCTIONS);
        result.next();
        return result.currentRow()[0].getString();
    }

    @Override
    public String getTimeDateFunctions() {
        ResultInterface result = executeQuery(GET_TIME_DATE_FUNCTIONS);
        result.next();
        return result.currentRow()[0].getString();
    }

    @Override
    public String getSearchStringEscape() {
        ResultInterface result = executeQuery(GET_SEARCH_STRING_ESCAPE);
        result.next();
        return result.currentRow()[0].getString();
    }

    @Override
    public ResultInterface getProcedures(String catalog, String schemaPattern, String procedureNamePattern) {
        return executeQuery(GET_PROCEDURES_3, getString(catalog), getString(schemaPattern),
                getString(procedureNamePattern));
    }

    @Override
    public ResultInterface getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
            String columnNamePattern) {
        return executeQuery(GET_PROCEDURE_COLUMNS_4, getString(catalog), getString(schemaPattern),
                getString(procedureNamePattern), getString(columnNamePattern));
    }

    @Override
    public ResultInterface getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) {
        return executeQuery(GET_TABLES_4, getString(catalog), getString(schemaPattern), getString(tableNamePattern),
                getStringArray(types));
    }

    @Override
    public ResultInterface getSchemas() {
        return executeQuery(GET_SCHEMAS);
    }

    @Override
    public ResultInterface getCatalogs() {
        return executeQuery(GET_CATALOGS);
    }

    @Override
    public ResultInterface getTableTypes() {
        return executeQuery(GET_TABLE_TYPES);
    }

    @Override
    public ResultInterface getColumns(String catalog, String schemaPattern, String tableNamePattern,
            String columnNamePattern) {
        return executeQuery(GET_COLUMNS_4, getString(catalog), getString(schemaPattern), getString(tableNamePattern),
                getString(columnNamePattern));
    }

    @Override
    public ResultInterface getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) {
        return executeQuery(GET_COLUMN_PRIVILEGES_4, getString(catalog), getString(schema), getString(table),
                getString(columnNamePattern));
    }

    @Override
    public ResultInterface getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) {
        return executeQuery(GET_TABLE_PRIVILEGES_3, getString(catalog), getString(schemaPattern), //
                getString(tableNamePattern));
    }

    @Override
    public ResultInterface getBestRowIdentifier(String catalog, String schema, String table, int scope,
            boolean nullable) {
        return executeQuery(GET_BEST_ROW_IDENTIFIER_5, getString(catalog), getString(schema), getString(table),
                ValueInteger.get(scope), ValueBoolean.get(nullable));
    }

    @Override
    public ResultInterface getVersionColumns(String catalog, String schema, String table) {
        return executeQuery(GET_VERSION_COLUMNS_3, getString(catalog), getString(schema), getString(table));
    }

    @Override
    public ResultInterface getPrimaryKeys(String catalog, String schema, String table) {
        return executeQuery(GET_PRIMARY_KEYS_3, getString(catalog), getString(schema), getString(table));
    }

    @Override
    public ResultInterface getImportedKeys(String catalog, String schema, String table) {
        return executeQuery(GET_IMPORTED_KEYS_3, getString(catalog), getString(schema), getString(table));
    }

    @Override
    public ResultInterface getExportedKeys(String catalog, String schema, String table) {
        return executeQuery(GET_EXPORTED_KEYS_3, getString(catalog), getString(schema), getString(table));
    }

    @Override
    public ResultInterface getCrossReference(String primaryCatalog, String primarySchema, String primaryTable,
            String foreignCatalog, String foreignSchema, String foreignTable) {
        return executeQuery(GET_CROSS_REFERENCE_6, getString(primaryCatalog), getString(primarySchema),
                getString(primaryTable), getString(foreignCatalog), getString(foreignSchema), getString(foreignTable));
    }

    @Override
    public ResultInterface getTypeInfo() {
        return executeQuery(GET_TYPE_INFO);
    }

    @Override
    public ResultInterface getIndexInfo(String catalog, String schema, String table, boolean unique,
            boolean approximate) {
        return executeQuery(GET_INDEX_INFO_5, getString(catalog), getString(schema), //
                getString(table), ValueBoolean.get(unique), ValueBoolean.get(approximate));
    }

    @Override
    public ResultInterface getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) {
        return executeQuery(GET_UDTS_4, getString(catalog), getString(schemaPattern), getString(typeNamePattern),
                getIntArray(types));
    }

    @Override
    public ResultInterface getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) {
        return executeQuery(GET_SUPER_TYPES_3, getString(catalog), getString(schemaPattern),
                getString(typeNamePattern));
    }

    @Override
    public ResultInterface getSuperTables(String catalog, String schemaPattern, String tableNamePattern) {
        return executeQuery(GET_SUPER_TABLES_3, getString(catalog), getString(schemaPattern),
                getString(tableNamePattern));
    }

    @Override
    public ResultInterface getAttributes(String catalog, String schemaPattern, String typeNamePattern,
            String attributeNamePattern) {
        return executeQuery(GET_ATTRIBUTES_4, getString(catalog), getString(schemaPattern), getString(typeNamePattern),
                getString(attributeNamePattern));
    }

    @Override
    public int getDatabaseMajorVersion() {
        ResultInterface result = executeQuery(GET_DATABASE_MAJOR_VERSION);
        result.next();
        return result.currentRow()[0].getInt();
    }

    @Override
    public int getDatabaseMinorVersion() {
        ResultInterface result = executeQuery(GET_DATABASE_MINOR_VERSION);
        result.next();
        return result.currentRow()[0].getInt();
    }

    @Override
    public ResultInterface getSchemas(String catalog, String schemaPattern) {
        return executeQuery(GET_SCHEMAS_2, getString(catalog), getString(schemaPattern));
    }

    @Override
    public ResultInterface getFunctions(String catalog, String schemaPattern, String functionNamePattern) {
        return executeQuery(GET_FUNCTIONS_3, getString(catalog), getString(schemaPattern),
                getString(functionNamePattern));
    }

    @Override
    public ResultInterface getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
            String columnNamePattern) {
        return executeQuery(GET_FUNCTION_COLUMNS_4, getString(catalog), getString(schemaPattern),
                getString(functionNamePattern), getString(columnNamePattern));
    }

    @Override
    public ResultInterface getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
            String columnNamePattern) {
        return executeQuery(GET_PSEUDO_COLUMNS_4, getString(catalog), getString(schemaPattern),
                getString(tableNamePattern), getString(columnNamePattern));
    }

    private ResultInterface executeQuery(int code, Value... args) {
        if (session.isClosed()) {
            throw DbException.get(ErrorCode.DATABASE_CALLED_AT_SHUTDOWN);
        }
        session.lock();
        try {
            int objectId = session.getNextId();
            for (int i = 0, count = 0; i < transferList.size(); i++) {
                Transfer transfer = transferList.get(i);
                try {
                    session.traceOperation("GET_META", objectId);
                    int len = args.length;
                    transfer.writeInt(SessionRemote.GET_JDBC_META).writeInt(code).writeInt(len);
                    for (int j = 0; j < len; j++) {
                        transfer.writeValue(args[j]);
                    }
                    session.done(transfer);
                    int columnCount = transfer.readInt();
                    return new ResultRemote(session, transfer, objectId, columnCount, Integer.MAX_VALUE);
                } catch (IOException e) {
                    session.removeServer(e, i--, ++count);
                }
            }
            return null;
        } finally {
            session.unlock();
        }
    }

    private Value getIntArray(int[] array) {
        if (array == null) {
            return ValueNull.INSTANCE;
        }
        int cardinality = array.length;
        Value[] values = new Value[cardinality];
        for (int i = 0; i < cardinality; i++) {
            values[i] = ValueInteger.get(array[i]);
        }
        return ValueArray.get(TypeInfo.TYPE_INTEGER, values, session);
    }

    private Value getStringArray(String[] array) {
        if (array == null) {
            return ValueNull.INSTANCE;
        }
        int cardinality = array.length;
        Value[] values = new Value[cardinality];
        for (int i = 0; i < cardinality; i++) {
            values[i] = getString(array[i]);
        }
        return ValueArray.get(TypeInfo.TYPE_VARCHAR, values, session);
    }

    private Value getString(String string) {
        return string != null ? ValueVarchar.get(string, session) : ValueNull.INSTANCE;
    }

}
