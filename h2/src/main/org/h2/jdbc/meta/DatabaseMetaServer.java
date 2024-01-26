/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc.meta;

import static org.h2.jdbc.meta.DatabaseMetaRemote.DEFAULT_NULL_ORDERING;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_ATTRIBUTES_4;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_BEST_ROW_IDENTIFIER_5;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_CATALOGS;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_COLUMNS_4;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_COLUMN_PRIVILEGES_4;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_CROSS_REFERENCE_6;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_DATABASE_MAJOR_VERSION;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_DATABASE_MINOR_VERSION;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_DATABASE_PRODUCT_VERSION;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_EXPORTED_KEYS_3;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_FUNCTIONS_3;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_FUNCTION_COLUMNS_4;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_IMPORTED_KEYS_3;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_INDEX_INFO_5;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_NUMERIC_FUNCTIONS;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_PRIMARY_KEYS_3;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_PROCEDURES_3;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_PROCEDURE_COLUMNS_4;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_PSEUDO_COLUMNS_4;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_SCHEMAS;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_SCHEMAS_2;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_SEARCH_STRING_ESCAPE;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_SQL_KEYWORDS;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_STRING_FUNCTIONS;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_SUPER_TABLES_3;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_SUPER_TYPES_3;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_SYSTEM_FUNCTIONS;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_TABLES_4;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_TABLE_PRIVILEGES_3;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_TABLE_TYPES;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_TIME_DATE_FUNCTIONS;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_TYPE_INFO;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_UDTS_4;
import static org.h2.jdbc.meta.DatabaseMetaRemote.GET_VERSION_COLUMNS_3;

import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.SimpleResult;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;
import org.h2.value.ValueVarchar;

/**
 * Server side support of database meta information.
 */
public final class DatabaseMetaServer {

    /**
     * Process a database meta data request.
     *
     * @param session the session
     * @param code the operation code
     * @param args the arguments
     * @return the result
     */
    public static ResultInterface process(SessionLocal session, int code, Value[] args) {
        DatabaseMeta meta = session.getDatabaseMeta();
        switch (code) {
        case DEFAULT_NULL_ORDERING:
            return result(meta.defaultNullOrdering().ordinal());
        case GET_DATABASE_PRODUCT_VERSION:
            return result(session, meta.getDatabaseProductVersion());
        case GET_SQL_KEYWORDS:
            return result(session, meta.getSQLKeywords());
        case GET_NUMERIC_FUNCTIONS:
            return result(session, meta.getNumericFunctions());
        case GET_STRING_FUNCTIONS:
            return result(session, meta.getStringFunctions());
        case GET_SYSTEM_FUNCTIONS:
            return result(session, meta.getSystemFunctions());
        case GET_TIME_DATE_FUNCTIONS:
            return result(session, meta.getTimeDateFunctions());
        case GET_SEARCH_STRING_ESCAPE:
            return result(session, meta.getSearchStringEscape());
        case GET_PROCEDURES_3:
            return meta.getProcedures(args[0].getString(), args[1].getString(), args[2].getString());
        case GET_PROCEDURE_COLUMNS_4:
            return meta.getProcedureColumns(args[0].getString(), args[1].getString(), args[2].getString(),
                    args[3].getString());
        case GET_TABLES_4:
            return meta.getTables(args[0].getString(), args[1].getString(), args[2].getString(),
                    toStringArray(args[3]));
        case GET_SCHEMAS:
            return meta.getSchemas();
        case GET_CATALOGS:
            return meta.getCatalogs();
        case GET_TABLE_TYPES:
            return meta.getTableTypes();
        case GET_COLUMNS_4:
            return meta.getColumns(args[0].getString(), args[1].getString(), args[2].getString(), args[3].getString());
        case GET_COLUMN_PRIVILEGES_4:
            return meta.getColumnPrivileges(args[0].getString(), args[1].getString(), args[2].getString(),
                    args[3].getString());
        case GET_TABLE_PRIVILEGES_3:
            return meta.getTablePrivileges(args[0].getString(), args[1].getString(), args[2].getString());
        case GET_BEST_ROW_IDENTIFIER_5:
            return meta.getBestRowIdentifier(args[0].getString(), args[1].getString(), args[2].getString(),
                    args[3].getInt(), args[4].getBoolean());
        case GET_VERSION_COLUMNS_3:
            return meta.getVersionColumns(args[0].getString(), args[1].getString(), args[2].getString());
        case GET_PRIMARY_KEYS_3:
            return meta.getPrimaryKeys(args[0].getString(), args[1].getString(), args[2].getString());
        case GET_IMPORTED_KEYS_3:
            return meta.getImportedKeys(args[0].getString(), args[1].getString(), args[2].getString());
        case GET_EXPORTED_KEYS_3:
            return meta.getExportedKeys(args[0].getString(), args[1].getString(), args[2].getString());
        case GET_CROSS_REFERENCE_6:
            return meta.getCrossReference(args[0].getString(), args[1].getString(), args[2].getString(),
                    args[3].getString(), args[4].getString(), args[5].getString());
        case GET_TYPE_INFO:
            return meta.getTypeInfo();
        case GET_INDEX_INFO_5:
            return meta.getIndexInfo(args[0].getString(), args[1].getString(), args[2].getString(),
                    args[3].getBoolean(), args[4].getBoolean());
        case GET_UDTS_4:
            return meta.getUDTs(args[0].getString(), args[1].getString(), args[2].getString(), toIntArray(args[3]));
        case GET_SUPER_TYPES_3:
            return meta.getSuperTypes(args[0].getString(), args[1].getString(), args[2].getString());
        case GET_SUPER_TABLES_3:
            return meta.getSuperTables(args[0].getString(), args[1].getString(), args[2].getString());
        case GET_ATTRIBUTES_4:
            return meta.getAttributes(args[0].getString(), args[1].getString(), args[2].getString(),
                    args[3].getString());
        case GET_DATABASE_MAJOR_VERSION:
            return result(meta.getDatabaseMajorVersion());
        case GET_DATABASE_MINOR_VERSION:
            return result(meta.getDatabaseMinorVersion());
        case GET_SCHEMAS_2:
            return meta.getSchemas(args[0].getString(), args[1].getString());
        case GET_FUNCTIONS_3:
            return meta.getFunctions(args[0].getString(), args[1].getString(), args[2].getString());
        case GET_FUNCTION_COLUMNS_4:
            return meta.getFunctionColumns(args[0].getString(), args[1].getString(), args[2].getString(),
                    args[3].getString());
        case GET_PSEUDO_COLUMNS_4:
            return meta.getPseudoColumns(args[0].getString(), args[1].getString(), args[2].getString(),
                    args[3].getString());
        default:
            throw DbException.getUnsupportedException("META " + code);
        }
    }

    private static String[] toStringArray(Value value) {
        if (value == ValueNull.INSTANCE) {
            return null;
        }
        Value[] list = ((ValueArray) value).getList();
        int l = list.length;
        String[] result = new String[l];
        for (int i = 0; i < l; i++) {
            result[i] = list[i].getString();
        }
        return result;
    }

    private static int[] toIntArray(Value value) {
        if (value == ValueNull.INSTANCE) {
            return null;
        }
        Value[] list = ((ValueArray) value).getList();
        int l = list.length;
        int[] result = new int[l];
        for (int i = 0; i < l; i++) {
            result[i] = list[i].getInt();
        }
        return result;
    }

    private static ResultInterface result(int value) {
        return result(ValueInteger.get(value));
    }

    private static ResultInterface result(SessionLocal session, String value) {
        return result(ValueVarchar.get(value, session));
    }

    private static ResultInterface result(Value v) {
        SimpleResult result = new SimpleResult();
        result.addColumn("RESULT", v.getType());
        result.addRow(v);
        return result;
    }

    private DatabaseMetaServer() {
    }

}
