/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc.meta;

import org.h2.engine.Constants;
import org.h2.result.ResultInterface;
import org.h2.result.SimpleResult;
import org.h2.value.TypeInfo;

/**
 * Base implementation of database meta information.
 */
abstract class DatabaseMetaLocalBase extends DatabaseMeta {

    @Override
    public final String getDatabaseProductVersion() {
        return Constants.FULL_VERSION;
    }

    @Override
    public final ResultInterface getVersionColumns(String catalog, String schema, String table) {
        checkClosed();
        SimpleResult result = new SimpleResult();
        result.addColumn("SCOPE", TypeInfo.TYPE_SMALLINT);
        result.addColumn("COLUMN_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("DATA_TYPE", TypeInfo.TYPE_INTEGER);
        result.addColumn("TYPE_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("COLUMN_SIZE", TypeInfo.TYPE_INTEGER);
        result.addColumn("BUFFER_LENGTH", TypeInfo.TYPE_INTEGER);
        result.addColumn("DECIMAL_DIGITS", TypeInfo.TYPE_SMALLINT);
        result.addColumn("PSEUDO_COLUMN", TypeInfo.TYPE_SMALLINT);
        return result;
    }

    @Override
    public final ResultInterface getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) {
        checkClosed();
        SimpleResult result = new SimpleResult();
        result.addColumn("TYPE_CAT", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TYPE_SCHEM", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TYPE_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("CLASS_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("DATA_TYPE", TypeInfo.TYPE_INTEGER);
        result.addColumn("REMARKS", TypeInfo.TYPE_VARCHAR);
        result.addColumn("BASE_TYPE", TypeInfo.TYPE_SMALLINT);
        return result;
    }

    @Override
    public final ResultInterface getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) {
        checkClosed();
        SimpleResult result = new SimpleResult();
        result.addColumn("TYPE_CAT", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TYPE_SCHEM", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TYPE_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("SUPERTYPE_CAT", TypeInfo.TYPE_VARCHAR);
        result.addColumn("SUPERTYPE_SCHEM", TypeInfo.TYPE_VARCHAR);
        result.addColumn("SUPERTYPE_NAME", TypeInfo.TYPE_VARCHAR);
        return result;
    }

    @Override
    public final ResultInterface getSuperTables(String catalog, String schemaPattern, String tableNamePattern) {
        checkClosed();
        SimpleResult result = new SimpleResult();
        result.addColumn("TABLE_CAT", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TABLE_SCHEM", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TABLE_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("SUPERTABLE_NAME", TypeInfo.TYPE_VARCHAR);
        return result;
    }

    @Override
    public final ResultInterface getAttributes(String catalog, String schemaPattern, String typeNamePattern,
            String attributeNamePattern) {
        checkClosed();
        SimpleResult result = new SimpleResult();
        result.addColumn("TYPE_CAT", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TYPE_SCHEM", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TYPE_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("ATTR_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("DATA_TYPE", TypeInfo.TYPE_INTEGER);
        result.addColumn("ATTR_TYPE_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("ATTR_SIZE", TypeInfo.TYPE_INTEGER);
        result.addColumn("DECIMAL_DIGITS", TypeInfo.TYPE_INTEGER);
        result.addColumn("NUM_PREC_RADIX", TypeInfo.TYPE_INTEGER);
        result.addColumn("NULLABLE", TypeInfo.TYPE_INTEGER);
        result.addColumn("REMARKS", TypeInfo.TYPE_VARCHAR);
        result.addColumn("ATTR_DEF", TypeInfo.TYPE_VARCHAR);
        result.addColumn("SQL_DATA_TYPE", TypeInfo.TYPE_INTEGER);
        result.addColumn("SQL_DATETIME_SUB", TypeInfo.TYPE_INTEGER);
        result.addColumn("CHAR_OCTET_LENGTH", TypeInfo.TYPE_INTEGER);
        result.addColumn("ORDINAL_POSITION", TypeInfo.TYPE_INTEGER);
        result.addColumn("IS_NULLABLE", TypeInfo.TYPE_VARCHAR);
        result.addColumn("SCOPE_CATALOG", TypeInfo.TYPE_VARCHAR);
        result.addColumn("SCOPE_SCHEMA", TypeInfo.TYPE_VARCHAR);
        result.addColumn("SCOPE_TABLE", TypeInfo.TYPE_VARCHAR);
        result.addColumn("SOURCE_DATA_TYPE", TypeInfo.TYPE_SMALLINT);
        return result;
    }

    @Override
    public final int getDatabaseMajorVersion() {
        return Constants.VERSION_MAJOR;
    }

    @Override
    public final int getDatabaseMinorVersion() {
        return Constants.VERSION_MINOR;
    }

    @Override
    public final ResultInterface getFunctions(String catalog, String schemaPattern, String functionNamePattern) {
        checkClosed();
        SimpleResult result = new SimpleResult();
        result.addColumn("FUNCTION_CAT", TypeInfo.TYPE_VARCHAR);
        result.addColumn("FUNCTION_SCHEM", TypeInfo.TYPE_VARCHAR);
        result.addColumn("FUNCTION_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("REMARKS", TypeInfo.TYPE_VARCHAR);
        result.addColumn("FUNCTION_TYPE", TypeInfo.TYPE_SMALLINT);
        result.addColumn("SPECIFIC_NAME", TypeInfo.TYPE_VARCHAR);
        return result;
    }

    @Override
    public final ResultInterface getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
            String columnNamePattern) {
        checkClosed();
        SimpleResult result = new SimpleResult();
        result.addColumn("FUNCTION_CAT", TypeInfo.TYPE_VARCHAR);
        result.addColumn("FUNCTION_SCHEM", TypeInfo.TYPE_VARCHAR);
        result.addColumn("FUNCTION_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("COLUMN_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("COLUMN_TYPE", TypeInfo.TYPE_SMALLINT);
        result.addColumn("DATA_TYPE", TypeInfo.TYPE_INTEGER);
        result.addColumn("TYPE_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("PRECISION", TypeInfo.TYPE_INTEGER);
        result.addColumn("LENGTH", TypeInfo.TYPE_INTEGER);
        result.addColumn("SCALE", TypeInfo.TYPE_SMALLINT);
        result.addColumn("RADIX", TypeInfo.TYPE_SMALLINT);
        result.addColumn("NULLABLE", TypeInfo.TYPE_SMALLINT);
        result.addColumn("REMARKS", TypeInfo.TYPE_VARCHAR);
        result.addColumn("CHAR_OCTET_LENGTH", TypeInfo.TYPE_INTEGER);
        result.addColumn("ORDINAL_POSITION", TypeInfo.TYPE_INTEGER);
        result.addColumn("IS_NULLABLE", TypeInfo.TYPE_VARCHAR);
        result.addColumn("SPECIFIC_NAME", TypeInfo.TYPE_VARCHAR);
        return result;
    }

    final SimpleResult getPseudoColumnsResult() {
        checkClosed();
        SimpleResult result = new SimpleResult();
        result.addColumn("TABLE_CAT", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TABLE_SCHEM", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TABLE_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("COLUMN_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("DATA_TYPE", TypeInfo.TYPE_INTEGER);
        result.addColumn("COLUMN_SIZE", TypeInfo.TYPE_INTEGER);
        result.addColumn("DECIMAL_DIGITS", TypeInfo.TYPE_INTEGER);
        result.addColumn("NUM_PREC_RADIX", TypeInfo.TYPE_INTEGER);
        result.addColumn("COLUMN_USAGE", TypeInfo.TYPE_VARCHAR);
        result.addColumn("REMARKS", TypeInfo.TYPE_VARCHAR);
        result.addColumn("CHAR_OCTET_LENGTH", TypeInfo.TYPE_INTEGER);
        result.addColumn("IS_NULLABLE", TypeInfo.TYPE_VARCHAR);
        return result;
    }

    abstract void checkClosed();

}
