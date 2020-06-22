/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc.meta;

import org.h2.result.ResultInterface;

/**
 * Database meta information.
 */
public abstract class DatabaseMeta {

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#nullsAreSortedHigh()
     */
    public abstract boolean nullsAreSortedHigh();

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getDatabaseProductVersion()
     */
    public abstract String getDatabaseProductVersion();

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getSQLKeywords()
     */
    public abstract String getSQLKeywords();

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getNumericFunctions()
     */
    public abstract String getNumericFunctions();

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getStringFunctions()
     */
    public abstract String getStringFunctions();

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getSystemFunctions()
     */
    public abstract String getSystemFunctions();

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getTimeDateFunctions()
     */
    public abstract String getTimeDateFunctions();

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getSearchStringEscape()
     */
    public abstract String getSearchStringEscape();

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getProcedures(String, String, String)
     */
    public abstract ResultInterface getProcedures(String catalog, String schemaPattern, String procedureNamePattern);

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getProcedureColumns(String, String,
     *      String, String)
     */
    public abstract ResultInterface getProcedureColumns(String catalog, String schemaPattern,
            String procedureNamePattern, String columnNamePattern);

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getTables(String, String, String,
     *      String[])
     */
    public abstract ResultInterface getTables(String catalog, String schemaPattern, String tableNamePattern,
            String[] types);

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getSchemas()
     */
    public abstract ResultInterface getSchemas();

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getCatalogs()
     */
    public abstract ResultInterface getCatalogs();

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getTableTypes()
     */
    public abstract ResultInterface getTableTypes();

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getColumns(String, String, String, String)
     */
    public abstract ResultInterface getColumns(String catalog, String schemaPattern, String tableNamePattern,
            String columnNamePattern);

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getColumnPrivileges(String, String,
     *      String, String)
     */
    public abstract ResultInterface getColumnPrivileges(String catalog, String schema, String table,
            String columnNamePattern);

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getTablePrivileges(String, String, String)
     */
    public abstract ResultInterface getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern);

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getBestRowIdentifier(String, String,
     *      String, int, boolean)
     */
    public abstract ResultInterface getBestRowIdentifier(String catalogPattern, String schemaPattern, String tableName,
            int scope, boolean nullable);

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getVersionColumns(String, String, String)
     */
    public abstract ResultInterface getVersionColumns(String catalog, String schema, String table);

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getPrimaryKeys(String, String, String)
     */
    public abstract ResultInterface getPrimaryKeys(String catalog, String schema, String table);

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getImportedKeys(String, String, String)
     */
    public abstract ResultInterface getImportedKeys(String catalog, String schema, String table);

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getExportedKeys(String, String, String)
     */
    public abstract ResultInterface getExportedKeys(String catalog, String schema, String table);

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getCrossReference(String, String, String,
     *      String, String, String)
     */
    public abstract ResultInterface getCrossReference(String primaryCatalog, String primarySchema, String primaryTable,
            String foreignCatalog, String foreignSchema, String foreignTable);

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getTypeInfo()
     */
    public abstract ResultInterface getTypeInfo();

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getIndexInfo(String, String, String,
     *      boolean, boolean)
     */
    public abstract ResultInterface getIndexInfo(String catalog, String schema, String table, boolean unique,
            boolean approximate);

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getUDTs(String, String, String, int[])
     */
    public abstract ResultInterface getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types);

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getSuperTypes(String, String, String)
     */
    public abstract ResultInterface getSuperTypes(String catalog, String schemaPattern, String typeNamePattern);

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getSuperTables(String, String, String)
     */
    public abstract ResultInterface getSuperTables(String catalog, String schemaPattern, String tableNamePattern);

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getAttributes(String, String, String,
     *      String)
     */
    public abstract ResultInterface getAttributes(String catalog, String schemaPattern, String typeNamePattern,
            String attributeNamePattern);

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getDatabaseMajorVersion()
     */
    public abstract int getDatabaseMajorVersion();

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getDatabaseMinorVersion()
     */
    public abstract int getDatabaseMinorVersion();

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getSchemas(String, String)
     */
    public abstract ResultInterface getSchemas(String catalog, String schemaPattern);

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getFunctions(String, String, String)
     */
    public abstract ResultInterface getFunctions(String catalog, String schemaPattern, String functionNamePattern);

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getFunctionColumns(String, String, String,
     *      String)
     */
    public abstract ResultInterface getFunctionColumns(String catalog, String schemaPattern, //
            String functionNamePattern, String columnNamePattern);

    /**
     * INTERNAL
     *
     * @see java.sql.DatabaseMetaData#getPseudoColumns(String, String, String,
     *      String)
     */
    public abstract ResultInterface getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
            String columnNamePattern);

}
