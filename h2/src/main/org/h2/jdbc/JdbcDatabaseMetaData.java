/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.sql.*;

import org.h2.engine.Constants;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.message.TraceObject;
import org.h2.util.StringUtils;

/**
 * Represents the meta data for a database.
 */
public class JdbcDatabaseMetaData extends TraceObject implements DatabaseMetaData {

    private JdbcConnection conn;

    /**
     * Returns the major version of this driver.
     *
     * @return the major version number
     */
    public int getDriverMajorVersion() {
        debugCodeCall("getDriverMajorVersion");
        return Constants.VERSION_MAJOR;
    }

    /**
     * Returns the minor version of this driver.
     *
     * @return the minor version number
     */
    public int getDriverMinorVersion() {
        debugCodeCall("getDriverMinorVersion");
        return Constants.VERSION_MINOR;
    }

    /**
     * Gets the database product name.
     *
     * @return the product name
     */
    public String getDatabaseProductName() {
        debugCodeCall("getDatabaseProductName");
        return Constants.PRODUCT_NAME;
    }

    /**
     * Gets the product version of the database.
     *
     * @return the product version
     */
    public String getDatabaseProductVersion() {
        debugCodeCall("getDatabaseProductVersion");
        return Constants.getVersion();
    }

    /**
     * Gets the name of the JDBC driver.
     *
     * @return the driver name
     */
    public String getDriverName() {
        debugCodeCall("getDriverName");
        return Constants.DRIVER_NAME;
    }

    /**
     * Gets the version number of the driver in the format
     * [MajorVersion].[MinorVersion].
     *
     * @return the version number
     */
    public String getDriverVersion() {
        debugCodeCall("getDriverVersion");
        return Constants.getVersion();
    }

    /**
     * Gets the list of tables in the database.
     * The result set is sorted by TABLE_TYPE, TABLE_SCHEM, and TABLE_NAME.
     *
     * <ul>
     * <li>1 TABLE_CAT (String) table catalog
     * <li>2 TABLE_SCHEM (String) table schema
     * <li>3 TABLE_NAME (String) table name
     * <li>4 TABLE_TYPE (String) table type
     * <li>5 REMARKS (String) comment
     *
     * <li>6 SQL (String) the create table statement or NULL for systems tables
     * </ul>
     *
     * @param catalog null (to get all objects) or the catalog name
     * @param schemaPattern null (to get all objects) or a schema name (uppercase for unquoted names)
     * @param tableNamePattern null (to get all objects) or a table name (uppercase for unquoted names)
     * @param types null or a list of table types
     * @return the list of columns
     * @throws SQLException if the connection is closed
     */
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        try {
            if(debug()) {
                debugCode("getTables("
                        +quote(catalog)+", "
                        +quote(schemaPattern)+", "
                        +quote(tableNamePattern)+", "
                        +quoteArray(types)+");");
            }
            checkClosed();
            String tableType;
            if (types != null && types.length>0) {
                tableType = "TABLE_TYPE IN(";
                for (int i = 0; i < types.length; i++) {
                    if (i>0) {
                        tableType += ", ";
                    }
                    tableType += "?";
                }
                tableType += ")";
            } else {
                tableType = "TRUE";
            }
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "TABLE_CATALOG TABLE_CAT, "
                    + "TABLE_SCHEMA TABLE_SCHEM, "
                    + "TABLE_NAME, "
                    + "TABLE_TYPE, "
                    + "REMARKS, "
                    + "SQL "
                    + "FROM INFORMATION_SCHEMA.TABLES "
                    + "WHERE TABLE_CATALOG LIKE ? "
                    + "AND TABLE_SCHEMA LIKE ? "
                    + "AND TABLE_NAME LIKE ? "
                    + "AND (" + tableType + ") "
                    + "ORDER BY TABLE_TYPE, TABLE_SCHEMA, TABLE_NAME");
            prep.setString(1, getCatalogPattern(catalog));
            prep.setString(2, getSchemaPattern(schemaPattern));
            prep.setString(3, getPattern(tableNamePattern));
            for (int i = 0; types != null && i < types.length; i++) {
                prep.setString(4 + i, types[i]);
            }
            return prep.executeQuery();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of columns.
     * The result set is sorted by TABLE_SCHEM, TABLE_NAME, and ORDINAL_POSITION.
     *
     * <ul>
     * <li>1 TABLE_CAT (String) table catalog
     * <li>2 TABLE_SCHEM (String) table schema
     * <li>3 TABLE_NAME (String) table name
     * <li>4 COLUMN_NAME (String) column name
     * <li>5 DATA_TYPE (short) data type (see java.sql.Types)
     * <li>6 TYPE_NAME (String) data type name ("INTEGER", "VARCHAR",...)
     * <li>7 COLUMN_SIZE (int) precision
     * <li>8 BUFFER_LENGTH (int) unused
     * <li>9 DECIMAL_DIGITS (int) scale (0 for INTEGER and VARCHAR)
     * <li>10 NUM_PREC_RADIX (int) radix (always 10)
     * <li>11 NULLABLE (int) nullable or not. columnNoNulls or columnNullable
     * <li>12 REMARKS (String) comment (always empty)
     * <li>13 COLUMN_DEF (String) default value
     * <li>14 SQL_DATA_TYPE (int) unused
     * <li>15 SQL_DATETIME_SUB (int) unused
     * <li>16 CHAR_OCTET_LENGTH (int) unused
     * <li>17 ORDINAL_POSITION (int) the column index (1,2,...)
     * <li>18 IS_NULLABLE (String) "NO" or "YES"
     * </ul>
     *
     * @param catalog null (to get all objects) or the catalog name
     * @param schemaPattern null (to get all objects) or a schema name (uppercase for unquoted names)
     * @param tableNamePattern null (to get all objects) or a table name (uppercase for unquoted names)
     * @param columnNamePattern null (to get all objects) or a column name (uppercase for unquoted names)
     * @return the list of columns
     * @throws SQLException if the connection is closed
     */
    public ResultSet getColumns(String catalog, String schemaPattern,
            String tableNamePattern, String columnNamePattern)
            throws SQLException {
        try {
            if(debug()) {
                debugCode("getColumns("
                        +quote(catalog)+", "
                        +quote(schemaPattern)+", "
                        +quote(tableNamePattern)+", "
                        +quote(columnNamePattern)+");");
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "TABLE_CATALOG TABLE_CAT, "
                    + "TABLE_SCHEMA TABLE_SCHEM, "
                    + "TABLE_NAME, "
                    + "COLUMN_NAME, "
                    + "DATA_TYPE, "
                    + "TYPE_NAME, "
                    + "CHARACTER_MAXIMUM_LENGTH COLUMN_SIZE, "
                    + "CHARACTER_MAXIMUM_LENGTH BUFFER_LENGTH, "
                    + "NUMERIC_SCALE DECIMAL_DIGITS, "
                    + "NUMERIC_PRECISION_RADIX NUM_PREC_RADIX, "
                    + "NULLABLE, "
                    + "REMARKS, "
                    + "COLUMN_DEFAULT COLUMN_DEF, "
                    + "DATA_TYPE SQL_DATA_TYPE, "
                    + "ZERO() SQL_DATETIME_SUB, "
                    + "CHARACTER_OCTET_LENGTH CHAR_OCTET_LENGTH, "
                    + "ORDINAL_POSITION, "
                    + "IS_NULLABLE IS_NULLABLE "
                    + "FROM INFORMATION_SCHEMA.COLUMNS "
                    + "WHERE TABLE_CATALOG LIKE ? "
                    + "AND TABLE_SCHEMA LIKE ? "
                    + "AND TABLE_NAME LIKE ? "
                    + "AND COLUMN_NAME LIKE ? "
                    + "ORDER BY TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION");
            prep.setString(1, getCatalogPattern(catalog));
            prep.setString(2, getSchemaPattern(schemaPattern));
            prep.setString(3, getPattern(tableNamePattern));
            prep.setString(4, getPattern(columnNamePattern));
            return prep.executeQuery();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of indexes for this database.
     * The primary key index (if there is one) is also listed, with the name PRIMARY_KEY.
     * The result set is sorted by NON_UNIQUE ('false' first), TYPE, TABLE_SCHEM, INDEX_NAME, and ORDINAL_POSITION.
     *
     * <ul>
     * <li>1 TABLE_CAT (String) table catalog
     * <li>2 TABLE_SCHEM (String) table schema
     * <li>3 TABLE_NAME (String) table name
     * <li>4 NON_UNIQUE (boolean) 'false' for unique, 'true' for non-unique
     * <li>5 INDEX_QUALIFIER (String) index catalog
     * <li>6 INDEX_NAME (String) index name
     * <li>7 TYPE (short) the index type (always tableIndexOther)
     * <li>8 ORDINAL_POSITION (short) column index (1, 2, ...)
     * <li>9 COLUMN_NAME (String) column name
     * <li>10 ASC_OR_DESC (String) ascending or descending (always 'A')
     * <li>11 CARDINALITY (int) numbers of unique values
     * <li>12 PAGES (int) number of pages use (always 0)
     * <li>13 FILTER_CONDITION (String) filter condition (always empty)
     * </ul>
     *
     * @param catalog null (to get all objects) or the catalog name
     * @param schema schema name (must be specified)
     * @param tableName table name (must be specified)
     * @param unique only unique indexes
     * @param approximate is ignored
     * @return the list of indexes and columns
     * @throws SQLException if the connection is closed
     */
    public ResultSet getIndexInfo(String catalog, String schema, String tableName, boolean unique, boolean approximate)
            throws SQLException {
        try {
            if(debug()) {
                debugCode("getIndexInfo("
                        +quote(catalog)+", "
                        +quote(schema)+", "
                        +quote(tableName)+", "
                        +unique+", "
                        +approximate+");");
            }
            String uniqueCondition;
            if(unique) {
                uniqueCondition = "NON_UNIQUE=FALSE";
            } else {
                uniqueCondition = "TRUE";
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "TABLE_CATALOG TABLE_CAT, "
                    + "TABLE_SCHEMA TABLE_SCHEM, "
                    + "TABLE_NAME, "
                    + "NON_UNIQUE, "
                    + "TABLE_CATALOG INDEX_QUALIFIER, "
                    + "INDEX_NAME, "
                    + "INDEX_TYPE TYPE, "
                    + "ORDINAL_POSITION, "
                    + "COLUMN_NAME, "
                    + "ASC_OR_DESC, "
                    + "CARDINALITY, " // TODO meta data for number of unique values in an index
                    + "PAGES, "
                    + "FILTER_CONDITION "
                    + "FROM INFORMATION_SCHEMA.INDEXES "
                    + "WHERE TABLE_CATALOG LIKE ? "
                    + "AND TABLE_SCHEMA LIKE ? "
                    + "AND (" + uniqueCondition + ") "
                    + "AND TABLE_NAME = ? "
                    + "ORDER BY NON_UNIQUE, TYPE, TABLE_SCHEM, INDEX_NAME, ORDINAL_POSITION");
            prep.setString(1, getCatalogPattern(catalog));
            prep.setString(2, getSchemaPattern(schema));
            prep.setString(3, tableName);
            return prep.executeQuery();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the primary key columns for a table.
     * The result set is sorted by TABLE_SCHEM, and COLUMN_NAME (and not by KEY_SEQ).
     *
     * <ul>
     * <li>1 TABLE_CAT (String) table catalog
     * <li>2 TABLE_SCHEM (String) table schema
     * <li>3 TABLE_NAME (String) table name
     * <li>4 COLUMN_NAME (String) column name
     * <li>5 KEY_SEQ (short) the column index of this column (1,2,...)
     * <li>6 PK_NAME (String) always 'PRIMARY_KEY'
     * </ul>
     *
     * @param catalog null (to get all objects) or the catalog name
     * @param schema schema name (must be specified)
     * @param tableName table name (must be specified)
     * @return the list of primary key columns
     * @throws SQLException if the connection is closed
     */
    public ResultSet getPrimaryKeys(String catalog, String schema, String tableName) throws SQLException {
        try {
            if(debug()) {
                debugCode("getPrimaryKeys("
                        +quote(catalog)+", "
                        +quote(schema)+", "
                        +quote(tableName)+");");
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "TABLE_CATALOG TABLE_CAT, "
                    + "TABLE_SCHEMA TABLE_SCHEM, "
                    + "TABLE_NAME, "
                    + "COLUMN_NAME, "
                    + "ORDINAL_POSITION KEY_SEQ, "
                    + "INDEX_NAME PK_NAME "
                    + "FROM INFORMATION_SCHEMA.INDEXES "
                    + "WHERE TABLE_CATALOG LIKE ? "
                    + "AND TABLE_SCHEMA LIKE ? "
                    + "AND TABLE_NAME = ? "
                    + "AND PRIMARY_KEY = TRUE "
                    + "ORDER BY COLUMN_NAME");
            prep.setString(1, getCatalogPattern(catalog));
            prep.setString(2, getSchemaPattern(schema));
            prep.setString(3, tableName);
            return prep.executeQuery();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if all procedures callable.
     *
     * @return true
     */
    public boolean allProceduresAreCallable() {
        debugCodeCall("allProceduresAreCallable");
        return true;
    }

    /**
     * Checks if it possible to query all tables returned by getTables.
     *
     * @return true
     */
    public boolean allTablesAreSelectable() {
        debugCodeCall("allTablesAreSelectable");
        return true;
    }

    /**
     * Returns the database URL for this connection.
     *
     * @return the url
     */
    public String getURL() throws SQLException {
        try {
            debugCodeCall("getURL");
            return conn.getURL();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the user name as passed to DriverManager.getConnection(url, user, password).
     *
     * @return the user name
     */
    public String getUserName() throws SQLException {
        try {
            debugCodeCall("getUserName");
            return conn.getUser();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the same as Connection.isReadOnly().
     *
     * @return if read only optimization is switched on
     */
    public boolean isReadOnly() throws SQLException {
        try {
            debugCodeCall("isReadOnly");
            return conn.isReadOnly();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks is NULL values are sorted high (bigger than any non-null values).
     *
     * @return true or false
     */
    public boolean nullsAreSortedHigh() {
        debugCodeCall("nullsAreSortedHigh");
        return Constants.NULL_SORT_DEFAULT == Constants.NULL_SORT_HIGH;
    }

    /**
     * Checks is NULL values are sorted low (smaller than any non-null values).
     *
     * @return true or false
     */
    public boolean nullsAreSortedLow() {
        debugCodeCall("nullsAreSortedLow");
        return Constants.NULL_SORT_DEFAULT == Constants.NULL_SORT_LOW;
    }

    /**
     * Checks is NULL values are sorted at the beginning (no matter if ASC or DESC is used).
     *
     * @return true or false
     */
    public boolean nullsAreSortedAtStart() {
        debugCodeCall("nullsAreSortedAtStart");
        return Constants.NULL_SORT_DEFAULT == Constants.NULL_SORT_START;
    }

    /**
     * Checks is NULL values are sorted at the end (no matter if ASC or DESC is used).
     *
     * @return true or false
     */
    public boolean nullsAreSortedAtEnd() {
        debugCodeCall("nullsAreSortedAtEnd");
        return Constants.NULL_SORT_DEFAULT == Constants.NULL_SORT_END;
    }

    /**
     * Returns the connection that created this object.
     *
     * @return the connection
     */
    public Connection getConnection() {
        debugCodeCall("getConnection");
        return conn;
    }

    /**
     * Gets the list of procedures.
     * The result set is sorted by PROCEDURE_SCHEM, and PROCEDURE_NAME.
     *
     * <ul>
     * <li>1 PROCEDURE_CAT (String) catalog
     * <li>2 PROCEDURE_SCHEM (String) schema
     * <li>3 PROCEDURE_NAME (String) name
     * <li>4 NUM_INPUT_PARAMS (int) for future use, always 0
     * <li>5 NUM_OUTPUT_PARAMS (int) for future use, always 0
     * <li>6 NUM_RESULT_SETS (int) for future use, always 0
     * <li>7 REMARKS (String) description
     * <li>8 PROCEDURE_TYPE (short) if this procedure returns a result
     *              (procedureNoResult or procedureReturnsResult)
     * </ul>
     *
     * @return an empty result set
     * @throws SQLException if the connection is closed
     */
    public ResultSet getProcedures(String catalog, String schemaPattern,
            String procedureNamePattern) throws SQLException {
        try {
            if(debug()) {
                debugCode("getProcedures("
                        +quote(catalog)+", "
                        +quote(schemaPattern)+", "
                        +quote(procedureNamePattern)+");");
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "ALIAS_CATALOG PROCEDURE_CAT, "
                    + "ALIAS_SCHEMA PROCEDURE_SCHEM, "
                    + "ALIAS_NAME PROCEDURE_NAME, "
                    + "ZERO() NUM_INPUT_PARAMS, "
                    + "ZERO() NUM_OUTPUT_PARAMS, "
                    + "ZERO() NUM_RESULT_SETS, "
                    + "REMARKS, "
                    + "RETURNS_RESULT PROCEDURE_TYPE "
                    + "FROM INFORMATION_SCHEMA.FUNCTION_ALIASES "
                    + "WHERE ALIAS_CATALOG LIKE ? "
                    + "AND ALIAS_SCHEMA LIKE ? "
                    + "AND ALIAS_NAME LIKE ? "
                    + "ORDER BY PROCEDURE_SCHEM, PROCEDURE_NAME");
            prep.setString(1, getCatalogPattern(catalog));
            prep.setString(2, getSchemaPattern(schemaPattern));
            prep.setString(3, getPattern(procedureNamePattern));
            return prep.executeQuery();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of procedure columns.
     *
     * <ul>
     * <li>1 PROCEDURE_CAT (String) catalog
     * <li>2 PROCEDURE_SCHEM (String) schema
     * <li>3 PROCEDURE_NAME (String) name
     * <li>4 COLUMN_NAME (String) column name
     * <li>5 COLUMN_TYPE (short) column type
     * <li>6 DATA_TYPE (short) sql type
     * <li>7 TYPE_NAME (String) type name
     * <li>8 PRECISION (int) precision
     * <li>9 LENGTH (int) length
     * <li>10 SCALE (short) scale
     * <li>11 RADIX (int) always 10
     * <li>12 NULLABLE (short) nullable
     * <li>13 REMARKS (String) description
     * </ul>
     *
     * @throws SQLException if the connection is closed
     */
    public ResultSet getProcedureColumns(String catalog, String schemaPattern,
            String procedureNamePattern, String columnNamePattern)
            throws SQLException {
        try {
            if(debug()) {
                debugCode("getProcedureColumns("
                        +quote(catalog)+", "
                        +quote(schemaPattern)+", "
                        +quote(procedureNamePattern)+", "
                        +quote(columnNamePattern)+");");
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "ALIAS_CATALOG PROCEDURE_CAT, "
                    + "ALIAS_SCHEMA PROCEDURE_SCHEM, "
                    + "ALIAS_NAME PROCEDURE_NAME, "
                    + "COLUMN_NAME, "
                    + "COLUMN_TYPE, "
                    + "DATA_TYPE, "
                    + "TYPE_NAME, "
                    + "PRECISION, "
                    + "PRECISION LENGTH, "
                    + "SCALE, "
                    + "RADIX, "
                    + "NULLABLE, "
                    + "REMARKS "
                    + "FROM INFORMATION_SCHEMA.FUNCTION_COLUMNS "
                    + "WHERE ALIAS_CATALOG LIKE ? "
                    + "AND ALIAS_SCHEMA LIKE ? "
                    + "AND ALIAS_NAME LIKE ? "
                    + "AND COLUMN_NAME LIKE ?");
            prep.setString(1, getCatalogPattern(catalog));
            prep.setString(2, getSchemaPattern(schemaPattern));
            prep.setString(3, getPattern(procedureNamePattern));
            prep.setString(4, getPattern(columnNamePattern));
            return prep.executeQuery();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of schemas.
     * The result set is sorted by TABLE_SCHEM.
     *
     * <ul>
     * <li>1 TABLE_SCHEM (String) schema name
     * <li>2 TABLE_CATALOG (String) catalog name
     * <li>3 IS_DEFAULT (boolean) if this is the default schema
     * </ul>
     *
     * @return the schema list
     * @throws SQLException if the connection is closed
     */
    public ResultSet getSchemas() throws SQLException {
        try {
            debugCodeCall("getSchemas");
            checkClosed();
            PreparedStatement prep = conn
                    .prepareAutoCloseStatement("SELECT "
                            + "SCHEMA_NAME TABLE_SCHEM, "
                            + "CATALOG_NAME TABLE_CATALOG, "
                            +" IS_DEFAULT "
                            + "FROM INFORMATION_SCHEMA.SCHEMATA "
                            + "ORDER BY SCHEMA_NAME");
            return prep.executeQuery();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of catalogs.
     * The result set is sorted by TABLE_CAT.
     *
     * <ul>
     * <li>1 TABLE_CAT (String) catalog name
     * </ul>
     *
     * @return the catalog list
     * @throws SQLException if the connection is closed
     */
    public ResultSet getCatalogs() throws SQLException {
        try {
            debugCodeCall("getCatalogs");
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement(
                    "SELECT CATALOG_NAME TABLE_CAT "
                    + "FROM INFORMATION_SCHEMA.CATALOGS");
            return prep.executeQuery();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of table types. This call returns a result set with three
     * records: "SYSTEM TABLE", "TABLE", "and "VIEW".
     * The result set is sorted by TABLE_TYPE.
     *
     * <ul>
     * <li>1 TABLE_TYPE (String) table type
     * </ul>
     *
     * @return the table types
     * @throws SQLException if the connection is closed
     */
    public ResultSet getTableTypes() throws SQLException {
        try {
            debugCodeCall("getTableTypes");
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "TYPE TABLE_TYPE "
                    + "FROM INFORMATION_SCHEMA.TABLE_TYPES "
                    + "ORDER BY TABLE_TYPE");
            return prep.executeQuery();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of column privileges.
     * The result set is sorted by COLUMN_NAME and PRIVILEGE
     *
     * <ul>
     * <li>1 TABLE_CAT (String) table catalog
     * <li>2 TABLE_SCHEM (String) table schema
     * <li>3 TABLE_NAME (String) table name
     * <li>4 COLUMN_NAME (String) column name
     * <li>5 GRANTOR (String) grantor of access
     * <li>6 GRANTEE (String) grantee of access
     * <li>7 PRIVILEGE (String) SELECT, INSERT, UPDATE, DELETE or REFERENCES (only one per row)
     * <li>8 IS_GRANTABLE (String) YES means the grantee can grant access to others
     * </ul>
     *
     * @param catalog null (to get all objects) or the catalog name
     * @param schema null (to get all objects) or a schema name (uppercase for unquoted names)
     * @param table a table name (uppercase for unquoted names)
     * @param columnNamePattern null (to get all objects) or a column name (uppercase for unquoted names)
     * @return the list of privileges
     * @throws SQLException if the connection is closed
     */
    public ResultSet getColumnPrivileges(String catalog, String schema,
            String table, String columnNamePattern) throws SQLException {
        try {
            if(debug()) {
                debugCode("getColumnPrivileges("
                        +quote(catalog)+", "
                        +quote(schema)+", "
                        +quote(table)+", "
                        +quote(columnNamePattern)+");");
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "TABLE_CATALOG TABLE_CAT, "
                    + "TABLE_SCHEMA TABLE_SCHEM, "
                    + "TABLE_NAME, "
                    + "COLUMN_NAME, "
                    + "GRANTOR, "
                    + "GRANTEE, "
                    + "PRIVILEGE_TYPE PRIVILEGE, "
                    + "IS_GRANTABLE "
                    + "FROM INFORMATION_SCHEMA.COLUMN_PRIVILEGES "
                    + "WHERE TABLE_CATALOG LIKE ? "
                    + "AND TABLE_SCHEMA LIKE ? "
                    + "AND TABLE_NAME = ? "
                    + "AND COLUMN_NAME LIKE ? "
                    + "ORDER BY COLUMN_NAME, PRIVILEGE");
            prep.setString(1, getCatalogPattern(catalog));
            prep.setString(2, getSchemaPattern(schema));
            prep.setString(3, table);
            prep.setString(4, getPattern(columnNamePattern));
            return prep.executeQuery();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of table privileges.
     * The result set is sorted by TABLE_SCHEM, TABLE_NAME, and PRIVILEGE.
     *
     * <ul>
     * <li>1 TABLE_CAT (String) table catalog
     * <li>2 TABLE_SCHEM (String) table schema
     * <li>3 TABLE_NAME (String) table name
     * <li>4 GRANTOR (String) grantor of access
     * <li>5 GRANTEE (String) grantee of access
     * <li>6 PRIVILEGE (String) SELECT, INSERT, UPDATE, DELETE or REFERENCES (only one per row)
     * <li>7 IS_GRANTABLE (String) YES means the grantee can grant access to others
     * </ul>
     *
     * @param catalog null (to get all objects) or the catalog name
     * @param schemaPattern null (to get all objects) or a schema name (uppercase for unquoted names)
     * @param tableNamePattern null (to get all objects) or a table name (uppercase for unquoted names)
     * @return the list of privileges
     * @throws SQLException if the connection is closed
     */
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        try {
            if(debug()) {
                debugCode("getTablePrivileges("
                        +quote(catalog)+", "
                        +quote(schemaPattern)+", "
                        +quote(tableNamePattern)+");");
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "TABLE_CATALOG TABLE_CAT, "
                    + "TABLE_SCHEMA TABLE_SCHEM, "
                    + "TABLE_NAME, "
                    + "GRANTOR, "
                    + "GRANTEE, "
                    + "PRIVILEGE_TYPE PRIVILEGE, "
                    + "IS_GRANTABLE "
                    + "FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES "
                    + "WHERE TABLE_CATALOG LIKE ? "
                    + "AND TABLE_SCHEMA LIKE ? "
                    + "AND TABLE_NAME LIKE ? "
                    + "ORDER BY TABLE_SCHEM, TABLE_NAME, PRIVILEGE");
            prep.setString(1, getCatalogPattern(catalog));
            prep.setString(2, getSchemaPattern(schemaPattern));
            prep.setString(3, getPattern(tableNamePattern));
            return prep.executeQuery();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of columns that best identifier a row in a table.
     * The list is ordered by SCOPE.
     *
     * <ul>
     * <li>1 SCOPE (short) scope of result (always bestRowSession)
     * <li>2 COLUMN_NAME (String) column name
     * <li>3 DATA_TYPE (short) SQL data type, see also java.sql.Types
     * <li>4 TYPE_NAME (String) type name
     * <li>5 COLUMN_SIZE (int) precision
     * <li>6 BUFFER_LENGTH (int) unused
     * <li>7 DECIMAL_DIGITS (short) scale
     * <li>8 PSEUDO_COLUMN (short) (always bestRowNotPseudo)
     * </ul>
     *
     * @param catalog null (to get all objects) or the catalog name
     * @param schema schema name (must be specified)
     * @param tableName table name (must be specified)
     * @param scope ignored
     * @param nullable ignored
     * @return the primary key index
     * @throws SQLException if the connection is closed
     */
    public ResultSet getBestRowIdentifier(String catalog, String schema,
            String tableName, int scope, boolean nullable) throws SQLException {
        try {
            if(debug()) {
                debugCode("getBestRowIdentifier("
                        +quote(catalog)+", "
                        +quote(schema)+", "
                        +quote(tableName)+", "
                        +scope+", "+nullable+");");
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "CAST(? AS SMALLINT) SCOPE, "
                    + "C.COLUMN_NAME, "
                    + "C.DATA_TYPE, "
                    + "C.TYPE_NAME, "
                    + "C.CHARACTER_MAXIMUM_LENGTH COLUMN_SIZE, "
                    + "C.CHARACTER_MAXIMUM_LENGTH BUFFER_LENGTH, "
                    + "CAST(C.NUMERIC_SCALE AS SMALLINT) DECIMAL_DIGITS, "
                    + "CAST(? AS SMALLINT) PSEUDO_COLUMN "
                    + "FROM INFORMATION_SCHEMA.INDEXES I, "
                    +" INFORMATION_SCHEMA.COLUMNS C "
                    + "WHERE C.TABLE_NAME = I.TABLE_NAME "
                    + "AND C.COLUMN_NAME = I.COLUMN_NAME "
                    + "AND C.TABLE_CATALOG LIKE ? "
                    + "AND C.TABLE_SCHEMA LIKE ? "
                    + "AND C.TABLE_NAME = ? "
                    + "AND I.PRIMARY_KEY = TRUE "
                    + "ORDER BY SCOPE");
            prep.setInt(1, DatabaseMetaData.bestRowSession); // SCOPE
            prep.setInt(2, DatabaseMetaData.bestRowNotPseudo); // PSEUDO_COLUMN
            prep.setString(3, getCatalogPattern(catalog));
            prep.setString(4, getSchemaPattern(schema));
            prep.setString(5, tableName);
            return prep.executeQuery();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Get the list of columns that are update when any value is updated.
     *
     * <ul>
     * <li>1 SCOPE (int) not used
     * <li>2 COLUMN_NAME (String) column name
     * <li>3 DATA_TYPE (int) SQL data type - see also java.sql.Types
     * <li>4 TYPE_NAME (String) data type name
     * <li>5 COLUMN_SIZE (int) precision
     * <li>6 BUFFER_LENGTH (int) length (bytes)
     * <li>7 DECIMAL_DIGITS (int) scale
     * <li>8 PSEUDO_COLUMN (int) is this column a pseudo column
     * </ul>
     *
     * @param catalog null (to get all objects) or the catalog name
     * @param schema schema name (must be specified)
     * @param tableName table name (must be specified)
     * @return an empty result set
     * @throws SQLException if the connection is closed
     */
    public ResultSet getVersionColumns(String catalog, String schema,
            String tableName) throws SQLException {
        try {
            if(debug()) {
                debugCode("getVersionColumns("
                        +quote(catalog)+", "
                        +quote(schema)+", "
                        +quote(tableName)+");");
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "ZERO() SCOPE, "
                    + "COLUMN_NAME, "
                    + "CAST(DATA_TYPE AS INT) DATA_TYPE, "
                    + "TYPE_NAME, "
                    + "NUMERIC_PRECISION COLUMN_SIZE, "
                    + "NUMERIC_PRECISION BUFFER_LENGTH, "
                    + "NUMERIC_PRECISION DECIMAL_DIGITS, "
                    + "ZERO() PSEUDO_COLUMN "
                    + "FROM INFORMATION_SCHEMA.COLUMNS "
                    + "WHERE FALSE");
            return prep.executeQuery();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of primary key columns that are referenced by a table.
     * The result set is sorted by PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, FK_NAME, KEY_SEQ.
     *
     * <ul>
     * <li>1 PKTABLE_CAT (String) primary catalog
     * <li>2 PKTABLE_SCHEM (String) primary schema
     * <li>3 PKTABLE_NAME (String) primary table
     * <li>4 PKCOLUMN_NAME (String) primary column
     * <li>5 FKTABLE_CAT (String) foreign catalog
     * <li>6 FKTABLE_SCHEM (String) foreign schema
     * <li>7 FKTABLE_NAME (String) foreign table
     * <li>8 FKCOLUMN_NAME (String) foreign column
     * <li>9 KEY_SEQ (short) sequence number (1, 2, ...)
     * <li>10 UPDATE_RULE (short) action on update (see DatabaseMetaData.importedKey...)
     * <li>11 DELETE_RULE (short) action on delete (see DatabaseMetaData.importedKey...)
     * <li>12 FK_NAME (String) foreign key name
     * <li>13 PK_NAME (String) primary key name
     * <li>14 DEFERRABILITY (short) deferrable or not (always importedKeyNotDeferrable)
     * </ul>
     *
     * @param catalog null (to get all objects) or the catalog name
     * @param schema the schema name of the foreign table
     * @param tableName the name of the foreign table
     * @return the result set
     * @throws SQLException if the connection is closed
     */
    public ResultSet getImportedKeys(String catalog, String schema, String tableName) throws SQLException {
        try {
            if(debug()) {
                debugCode("getImportedKeys("
                        +quote(catalog)+", "
                        +quote(schema)+", "
                        +quote(tableName)+");");
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "PKTABLE_CATALOG PKTABLE_CAT, "
                    + "PKTABLE_SCHEMA PKTABLE_SCHEM, "
                    + "PKTABLE_NAME PKTABLE_NAME, "
                    + "PKCOLUMN_NAME, "
                    + "FKTABLE_CATALOG FKTABLE_CAT, "
                    + "FKTABLE_SCHEMA FKTABLE_SCHEM, "
                    + "FKTABLE_NAME, "
                    + "FKCOLUMN_NAME, "
                    + "ORDINAL_POSITION KEY_SEQ, "
                    + "UPDATE_RULE, "
                    + "DELETE_RULE, "
                    + "FK_NAME, "
                    + "PK_NAME, "
                    + "DEFERRABILITY "
                    + "FROM INFORMATION_SCHEMA.CROSS_REFERENCES "
                    + "WHERE FKTABLE_CATALOG LIKE ? "
                    + "AND FKTABLE_SCHEMA LIKE ? "
                    + "AND FKTABLE_NAME = ? "
                    + "ORDER BY PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, FK_NAME, KEY_SEQ");
            prep.setString(1, getCatalogPattern(catalog));
            prep.setString(2, getSchemaPattern(schema));
            prep.setString(3, tableName);
            return prep.executeQuery();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of foreign key columns that reference a table.
     * The result set is sorted by FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, FK_NAME, KEY_SEQ.
     *
     * <ul>
     * <li>1 PKTABLE_CAT (String) primary catalog
     * <li>2 PKTABLE_SCHEM (String) primary schema
     * <li>3 PKTABLE_NAME (String) primary table
     * <li>4 PKCOLUMN_NAME (String) primary column
     * <li>5 FKTABLE_CAT (String) foreign catalog
     * <li>6 FKTABLE_SCHEM (String) foreign schema
     * <li>7 FKTABLE_NAME (String) foreign table
     * <li>8 FKCOLUMN_NAME (String) foreign column
     * <li>9 KEY_SEQ (short) sequence number (1,2,...)
     * <li>10 UPDATE_RULE (short) action on update (see DatabaseMetaData.importedKey...)
     * <li>11 DELETE_RULE (short) action on delete (see DatabaseMetaData.importedKey...)
     * <li>12 FK_NAME (String) foreign key name
     * <li>13 PK_NAME (String) primary key name
     * <li>14 DEFERRABILITY (short) deferrable or not (always importedKeyNotDeferrable)
     * </ul>
     *
     * @param catalog null (to get all objects) or the catalog name
     * @param schema the schema name of the primary table
     * @param tableName the name of the primary table
     * @return the result set
     * @throws SQLException if the connection is closed
     */
    public ResultSet getExportedKeys(String catalog, String schema, String tableName)
            throws SQLException {
        try {
            if(debug()) {
                debugCode("getExportedKeys("
                        +quote(catalog)+", "
                        +quote(schema)+", "
                        +quote(tableName)+");");
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "PKTABLE_CATALOG PKTABLE_CAT, "
                    + "PKTABLE_SCHEMA PKTABLE_SCHEM, "
                    + "PKTABLE_NAME PKTABLE_NAME, "
                    + "PKCOLUMN_NAME, "
                    + "FKTABLE_CATALOG FKTABLE_CAT, "
                    + "FKTABLE_SCHEMA FKTABLE_SCHEM, "
                    + "FKTABLE_NAME, "
                    + "FKCOLUMN_NAME, "
                    + "ORDINAL_POSITION KEY_SEQ, "
                    + "UPDATE_RULE, "
                    + "DELETE_RULE, "
                    + "FK_NAME, "
                    + "PK_NAME, "
                    + "DEFERRABILITY "
                    + "FROM INFORMATION_SCHEMA.CROSS_REFERENCES "
                    + "WHERE PKTABLE_CATALOG LIKE ? "
                    + "AND PKTABLE_SCHEMA LIKE ? "
                    + "AND PKTABLE_NAME = ? "
                    + "ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, FK_NAME, KEY_SEQ");
            prep.setString(1, getCatalogPattern(catalog));
            prep.setString(2, getSchemaPattern(schema));
            prep.setString(3, tableName);
            return prep.executeQuery();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of foreign key columns that references a table, as well as
     * the list of primary key columns that are references by a table.
     * The result set is sorted by FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, FK_NAME, KEY_SEQ.
     *
     * <ul>
     * <li>1 PKTABLE_CAT (String) primary catalog
     * <li>2 PKTABLE_SCHEM (String) primary schema
     * <li>3 PKTABLE_NAME (String) primary table
     * <li>4 PKCOLUMN_NAME (String) primary column
     * <li>5 FKTABLE_CAT (String) foreign catalog
     * <li>6 FKTABLE_SCHEM (String) foreign schema
     * <li>7 FKTABLE_NAME (String) foreign table
     * <li>8 FKCOLUMN_NAME (String) foreign column
     * <li>9 KEY_SEQ (short) sequence number (1,2,...)
     * <li>10 UPDATE_RULE (short) action on update (see DatabaseMetaData.importedKey...)
     * <li>11 DELETE_RULE (short) action on delete (see DatabaseMetaData.importedKey...)
     * <li>12 FK_NAME (String) foreign key name
     * <li>13 PK_NAME (String) primary key name
     * <li>14 DEFERRABILITY (short) deferrable or not (always importedKeyNotDeferrable)
     * </ul>
     *
     * @param primaryCatalog ignored
     * @param primarySchema the schema name of the primary table (must be specified)
     * @param primaryTable the name of the primary table (must be specified)
     * @param foreignCatalog ignored
     * @param foreignSchema the schema name of the foreign table (must be specified)
     * @param foreignTable the name of the foreign table (must be specified)
     * @return the result set
     * @throws SQLException if the connection is closed
     */
    public ResultSet getCrossReference(String primaryCatalog,
            String primarySchema, String primaryTable, String foreignCatalog,
            String foreignSchema, String foreignTable) throws SQLException {
        try {
            if(debug()) {
                debugCode("getCrossReference("
                        +quote(primaryCatalog)+", "
                        +quote(primarySchema)+", "
                        +quote(primaryTable)+", "
                        +quote(foreignCatalog)+", "
                        +quote(foreignSchema)+", "
                        +quote(foreignTable)+");");
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "PKTABLE_CATALOG PKTABLE_CAT, "
                    + "PKTABLE_SCHEMA PKTABLE_SCHEM, "
                    + "PKTABLE_NAME PKTABLE_NAME, "
                    + "PKCOLUMN_NAME, "
                    + "FKTABLE_CATALOG FKTABLE_CAT, "
                    + "FKTABLE_SCHEMA FKTABLE_SCHEM, "
                    + "FKTABLE_NAME, "
                    + "FKCOLUMN_NAME, "
                    + "ORDINAL_POSITION KEY_SEQ, "
                    + "UPDATE_RULE, "
                    + "DELETE_RULE, "
                    + "FK_NAME, "
                    + "PK_NAME, "
                    + "DEFERRABILITY "
                    + "FROM INFORMATION_SCHEMA.CROSS_REFERENCES "
                    + "WHERE PKTABLE_CATALOG LIKE ? "
                    + "AND PKTABLE_SCHEMA LIKE ? "
                    + "AND PKTABLE_NAME = ? "
                    + "AND FKTABLE_CATALOG LIKE ? "
                    + "AND FKTABLE_SCHEMA LIKE ? "
                    + "AND FKTABLE_NAME = ? "
                    + "ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, FK_NAME, KEY_SEQ");
            prep.setString(1, getCatalogPattern(primaryCatalog));
            prep.setString(2, getSchemaPattern(primarySchema));
            prep.setString(3, primaryTable);
            prep.setString(4, getCatalogPattern(foreignCatalog));
            prep.setString(5, getSchemaPattern(foreignSchema));
            prep.setString(6, foreignTable);
            return prep.executeQuery();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of user defined data types.
     * This call returns an empty result set.
     *
     * <ul>
     * <li>1 TYPE_CAT (String) catalog
     * <li>2 TYPE_SCHEM (String) schema
     * <li>3 TYPE_NAME (String) type name
     * <li>4 CLASS_NAME (String) Java class
     * <li>5 DATA_TYPE (short) SQL Type - see also java.sql.Types
     * <li>6 REMARKS (String) description
     * <li>7 BASE_TYPE (short) base type - see also java.sql.Types
     * </ul>
     *
     * @param catalog ignored
     * @param schemaPattern ignored
     * @param typeNamePattern ignored
     * @param types ignored
     * @return an empty result set
     * @throws SQLException if the connection is closed
     */
    public ResultSet getUDTs(String catalog, String schemaPattern,
            String typeNamePattern, int[] types) throws SQLException {
        try {
            if(debug()) {
                debugCode("getUDTs("
                        +quote(catalog)+", "
                        +quote(schemaPattern)+", "
                        +quote(typeNamePattern)+", "
                        +quoteIntArray(types)+");");
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "CATALOG_NAME  TYPE_CAT, "
                    + "CATALOG_NAME  TYPE_SCHEM, "
                    + "CATALOG_NAME  TYPE_NAME, "
                    + "CATALOG_NAME  CLASS_NAME, "
                    + "CAST(ZERO() AS SMALLINT) DATA_TYPE, "
                    + "CATALOG_NAME  REMARKS, "
                    + "CAST(ZERO() AS SMALLINT) BASE_TYPE "
                    + "FROM INFORMATION_SCHEMA.CATALOGS "
                    + "WHERE FALSE");
            return prep.executeQuery();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of data types.
     * The result set is sorted by DATA_TYPE and
     * afterwards by how closely the data type maps to the corresponding JDBC SQL type
     * (best match first).
     *
     * <ul>
     * <li>1 TYPE_NAME (String) type name
     * <li>2 DATA_TYPE (short) SQL data type - see also java.sql.Types
     * <li>3 PRECISION (int) maximum precision
     * <li>4 LITERAL_PREFIX (String) prefix used to quote a literal
     * <li>5 LITERAL_SUFFIX (String) suffix used to quote a literal
     * <li>6 CREATE_PARAMS (String) parameters used (may be null)
     * <li>7 NULLABLE (short) typeNoNulls (NULL not allowed) or typeNullable
     * <li>8 CASE_SENSITIVE (boolean) case sensitive
     * <li>9 SEARCHABLE (short) typeSearchable
     * <li>10 UNSIGNED_ATTRIBUTE (boolean) unsigned
     * <li>11 FIXED_PREC_SCALE (boolean) fixed precision
     * <li>12 AUTO_INCREMENT (boolean) auto increment
     * <li>13 LOCAL_TYPE_NAME (String) localized version of the data type
     * <li>14 MINIMUM_SCALE (short) minimum scale
     * <li>15 MAXIMUM_SCALE (short) maximum scale
     * <li>16 SQL_DATA_TYPE (int) unused
     * <li>17 SQL_DATETIME_SUB (int) unused
     * <li>18 NUM_PREC_RADIX (int) 2 for binary, 10 for decimal
     * </ul>
     *
     * @return the list of data types
     * @throws SQLException if the connection is closed
     */
    public ResultSet getTypeInfo() throws SQLException {
        try {
            debugCodeCall("getTypeInfo");
            checkClosed();
            return conn.createStatement().executeQuery("SELECT "
                    + "TYPE_NAME, "
                    + "DATA_TYPE, "
                    + "PRECISION, "
                    + "PREFIX LITERAL_PREFIX, "
                    + "SUFFIX LITERAL_SUFFIX, "
                    + "PARAMS CREATE_PARAMS, "
                    + "NULLABLE, "
                    + "CASE_SENSITIVE, "
                    + "SEARCHABLE, "
                    + "FALSE UNSIGNED_ATTRIBUTE, "
                    + "FALSE FIXED_PREC_SCALE, "
                    + "AUTO_INCREMENT, "
                    + "TYPE_NAME LOCAL_TYPE_NAME, "
                    + "MINIMUM_SCALE, "
                    + "MAXIMUM_SCALE, "
                    + "DATA_TYPE SQL_DATA_TYPE, "
                    + "ZERO() SQL_DATETIME_SUB, "
                    + "RADIX NUM_PREC_RADIX "
                    + "FROM INFORMATION_SCHEMA.TYPE_INFO "
                    + "ORDER BY DATA_TYPE, POS");
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if this database store data in local files.
     *
     * @return true
     */
    public boolean usesLocalFiles() {
        debugCodeCall("usesLocalFiles");
        return true;
    }

    /**
     * Checks if this database use one file per table.
     *
     * @return false
     */
    public boolean usesLocalFilePerTable() {
        debugCodeCall("usesLocalFilePerTable");
        return false;
    }

    /**
     * Returns the string used to quote identifiers.
     *
     * @return a double quote
     */
    public String getIdentifierQuoteString() {
        debugCodeCall("getIdentifierQuoteString");
        return "\"";
    }

    /**
     * Gets the comma-separated list of all SQL keywords that are not supported
     * as table/column/index name, in addition to the SQL-92 keywords.
     *
     * @return a list with the keywords
     */
    public String getSQLKeywords() {
        debugCodeCall("getSQLKeywords");
        return "";
    }

    /**
     * Returns the list of numeric functions supported by this database.
     *
     * @return the list
     */
    public String getNumericFunctions() throws SQLException {
        debugCodeCall("getNumericFunctions");
        return getFunctions("Functions (Numeric)");
    }

    /**
     * Returns the list of string functions supported by this database.
     *
     * @return the list
     */
    public String getStringFunctions() throws SQLException {
        debugCodeCall("getStringFunctions");
        return getFunctions("Functions (String)");
    }

    /**
     * Returns the list of system functions supported by this database.
     *
     * @return the list
     */
    public String getSystemFunctions() throws SQLException {
        debugCodeCall("getSystemFunctions");
        return getFunctions("Functions (System)");
    }

    /**
     * Returns the list of date and time functions supported by this database.
     *
     * @return the list
     */
    public String getTimeDateFunctions() throws SQLException {
        debugCodeCall("getTimeDateFunctions");
        return getFunctions("Functions (Time and Date)");
    }

    private String getFunctions(String section) throws SQLException {
        try {
            StringBuffer buff = new StringBuffer();
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT TOPIC "
                    + "FROM INFORMATION_SCHEMA.HELP WHERE SECTION = ?");
            prep.setString(1, section);
            ResultSet rs = prep.executeQuery();
            while(rs.next()) {
                String s = rs.getString(1).trim();
                String[] array = StringUtils.arraySplit(s, ',', true);
                for(int i=0; i<array.length; i++) {
                    if(buff.length()>0) {
                        buff.append(",");
                    }
                    String f = array[i].trim();
                    if(f.indexOf(' ') >= 0) {
                        // remove 'Function' from 'INSERT Function'
                        f = f.substring(0, f.indexOf(' ')).trim();
                    }
                    buff.append(f);
                }
            }
            rs.close();
            prep.close();
            return buff.toString();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the default escape character for LIKE.
     *
     * @return the character '\'
     */
    public String getSearchStringEscape() {
        debugCodeCall("getSearchStringEscape");
        return "" + Constants.DEFAULT_ESCAPE_CHAR;
    }

    /**
     * Returns the characters that are allowed for identifiers in addiction to
     * A-Z, a-z, 0-9 and '_'.
     *
     * @return an empty String ("")
     */
    public String getExtraNameCharacters() {
        debugCodeCall("getExtraNameCharacters");
        return "";
    }

    /**
     * Returns whether alter table with add column is supported.
     * @return true
     */
    public boolean supportsAlterTableWithAddColumn() {
        debugCodeCall("supportsAlterTableWithAddColumn");
        return true;
    }

    /**
     * Returns whether alter table with drop column is supported.
     *
     * @return true
     */
    public boolean supportsAlterTableWithDropColumn() {
        debugCodeCall("supportsAlterTableWithDropColumn");
        return true;
    }

    /**
     * Returns whether column aliasing is supported.
     *
     * @return true
     */
    public boolean supportsColumnAliasing() {
        debugCodeCall("supportsColumnAliasing");
        return true;
    }

    /**
     * Returns whether NULL+1 is NULL or not.
     *
     * @return true
     */
    public boolean nullPlusNonNullIsNull() {
        debugCodeCall("nullPlusNonNullIsNull");
        return true;
    }

    /**
     * Returns whether CONVERT is supported.
     *
     * @return true
     */
    public boolean supportsConvert() {
        debugCodeCall("supportsConvert");
        return true;
    }

    /**
     * Returns whether CONVERT is supported for one datatype to another.
     *
     * @return true
     */
    public boolean supportsConvert(int fromType, int toType) {
        if(debug()) {
            debugCode("supportsConvert("+fromType+", "+fromType+");");
        }
        return true;
    }

    /**
     * Returns whether table correlation names (table alias) are supported.
     *
     * @return true
     */
    public boolean supportsTableCorrelationNames() {
        debugCodeCall("supportsTableCorrelationNames");
        return true;
    }

    /**
     * Returns whether table correlation names (table alias) are restricted to
     * be different than table names.
     *
     * @return false
     */
    public boolean supportsDifferentTableCorrelationNames() {
        debugCodeCall("supportsDifferentTableCorrelationNames");
        return false;
    }

    /**
     * Returns whether expression in ORDER BY are supported.
     *
     * @return true
     */
    public boolean supportsExpressionsInOrderBy() {
        debugCodeCall("supportsExpressionsInOrderBy");
        return true;
    }

    /**
     * Returns whether ORDER BY is supported if the column is not in the SELECT
     * list.
     *
     * @return true
     */
    public boolean supportsOrderByUnrelated() {
        debugCodeCall("supportsOrderByUnrelated");
        return true;
    }

    /**
     * Returns whether GROUP BY is supported.
     *
     * @return true
     */
    public boolean supportsGroupBy() {
        debugCodeCall("supportsGroupBy");
        return true;
    }

    /**
     * Returns whether GROUP BY is supported if the column is not in the SELECT
     * list.
     *
     * @return true
     */
    public boolean supportsGroupByUnrelated() {
        debugCodeCall("supportsGroupByUnrelated");
        return true;
    }

    /**
     * Checks whether a GROUP BY clause can use columns that are not in the
     * SELECT clause, provided that it specifies all the columns in the SELECT
     * clause.
     *
     * @return true
     */
    public boolean supportsGroupByBeyondSelect() {
        debugCodeCall("supportsGroupByBeyondSelect");
        return true;
    }

    /**
     * Returns whether LIKE... ESCAPE is supported.
     *
     * @return true
     */
    public boolean supportsLikeEscapeClause() {
        debugCodeCall("supportsLikeEscapeClause");
        return true;
    }

    /**
     * Returns whether multiple result sets are supported.
     *
     * @return false
     */
    public boolean supportsMultipleResultSets() {
        debugCodeCall("supportsMultipleResultSets");
        return false;
    }

    /**
     * Returns whether multiple transactions (on different connections) are
     * supported.
     *
     * @return true
     */
    public boolean supportsMultipleTransactions() {
        debugCodeCall("supportsMultipleTransactions");
        return true;
    }

    /**
     * Returns whether columns with NOT NULL are supported.
     *
     * @return true
     */
    public boolean supportsNonNullableColumns() {
        debugCodeCall("supportsNonNullableColumns");
        return true;
    }

    /**
     * Returns whether ODBC Minimum SQL grammar is supported.
     *
     * @return true
     */
    public boolean supportsMinimumSQLGrammar() {
        debugCodeCall("supportsMinimumSQLGrammar");
        return true;
    }

    /**
     * Returns whether ODBC Core SQL grammar is supported.
     *
     * @return true
     */
    public boolean supportsCoreSQLGrammar() {
        debugCodeCall("supportsCoreSQLGrammar");
        return true;
    }

    /**
     * Returns whether ODBC Extended SQL grammar is supported.
     *
     * @return false
     */
    public boolean supportsExtendedSQLGrammar() {
        debugCodeCall("supportsExtendedSQLGrammar");
        return false;
    }

    /**
     * Returns whether SQL-92 entry level grammar is supported.
     *
     * @return true
     */
    public boolean supportsANSI92EntryLevelSQL() {
        debugCodeCall("supportsANSI92EntryLevelSQL");
        return true;
    }

    /**
     * Returns whether SQL-92 intermediate level grammar is supported.
     *
     * @return false
     */
    public boolean supportsANSI92IntermediateSQL() {
        debugCodeCall("supportsANSI92IntermediateSQL");
        return false;
    }

    /**
     * Returns whether SQL-92 full level grammar is supported.
     *
     * @return false
     */
    public boolean supportsANSI92FullSQL() {
        debugCodeCall("supportsANSI92FullSQL");
        return false;
    }

    /**
     * Returns whether refererential integrity is supported.
     *
     * @return true
     */
    public boolean supportsIntegrityEnhancementFacility() {
        debugCodeCall("supportsIntegrityEnhancementFacility");
        return true;
    }

    /**
     * Returns whether outer joins are supported.
     *
     * @return true
     */
    public boolean supportsOuterJoins() {
        debugCodeCall("supportsOuterJoins");
        return true;
    }

    /**
     * Returns whether full outer joins are supported.
     *
     * @return false
     */
    public boolean supportsFullOuterJoins() {
        debugCodeCall("supportsFullOuterJoins");
        return false;
    }

    /**
     * Returns whether limited outer joins are supported.
     *
     * @return true
     */
    public boolean supportsLimitedOuterJoins() {
        debugCodeCall("supportsLimitedOuterJoins");
        return true;
    }

    /**
     * Returns the term for "schema".
     *
     * @return "schema"
     */
    public String getSchemaTerm() {
        debugCodeCall("getSchemaTerm");
        return "schema";
    }

    /**
     * Returns the term for "procedure".
     *
     * @return "procedure"
     */
    public String getProcedureTerm() {
        debugCodeCall("getProcedureTerm");
        return "procedure";
    }

    /**
     * Returns the term for "catalog".
     *
     * @return "catalog"
     */
    public String getCatalogTerm() {
        debugCodeCall("getCatalogTerm");
        return "catalog";
    }

    /**
     * Returns whether the catalog is at the beginning.
     *
     * @return true
     */
    public boolean isCatalogAtStart() {
        debugCodeCall("isCatalogAtStart");
        return true;
    }

    /**
     * Returns the catalog separator.
     *
     * @return "."
     */
    public String getCatalogSeparator() {
        debugCodeCall("getCatalogSeparator");
        return ".";
    }

    /**
     * Returns whether the schema name in INSERT, UPDATE, DELETE is supported.
     *
     * @return true
     */
    public boolean supportsSchemasInDataManipulation() {
        debugCodeCall("supportsSchemasInDataManipulation");
        return true;
    }

    /**
     * Returns whether the schema name in procedure calls is supported.
     *
     * @return true
     */
    public boolean supportsSchemasInProcedureCalls() {
        debugCodeCall("supportsSchemasInProcedureCalls");
        return true;
    }

    /**
     * Returns whether the schema name in CREATE TABLE is supported.
     *
     * @return true
     */
    public boolean supportsSchemasInTableDefinitions() {
        debugCodeCall("supportsSchemasInTableDefinitions");
        return true;
    }

    /**
     * Returns whether the schema name in CREATE INDEX is supported.
     *
     * @return true
     */
    public boolean supportsSchemasInIndexDefinitions() {
        debugCodeCall("supportsSchemasInIndexDefinitions");
        return true;
    }

    /**
     * Returns whether the schema name in GRANT is supported.
     *
     * @return true
     */
    public boolean supportsSchemasInPrivilegeDefinitions() {
        debugCodeCall("supportsSchemasInPrivilegeDefinitions");
        return true;
    }

    /**
     * Returns whether the catalog name in INSERT, UPDATE, DELETE is supported.
     *
     * @return false
     */
    public boolean supportsCatalogsInDataManipulation() {
        debugCodeCall("supportsCatalogsInDataManipulation");
        return false;
    }

    /**
     * Returns whether the catalog name in procedure calls is supported.
     *
     * @return false
     */
    public boolean supportsCatalogsInProcedureCalls() {
        debugCodeCall("supportsCatalogsInProcedureCalls");
        return false;
    }

    /**
     * Returns whether the catalog name in CREATE TABLE is supported.
     *
     * @return false
      */
    public boolean supportsCatalogsInTableDefinitions() {
        debugCodeCall("supportsCatalogsInTableDefinitions");
        return false;
    }

    /**
     * Returns whether the catalog name in CREATE INDEX is supported.
     *
     * @return false
     */
    public boolean supportsCatalogsInIndexDefinitions() {
        debugCodeCall("supportsCatalogsInIndexDefinitions");
        return false;
    }

    /**
     * Returns whether the catalog name in GRANT is supported.
     *
     * @return false
     */
    public boolean supportsCatalogsInPrivilegeDefinitions() {
        debugCodeCall("supportsCatalogsInPrivilegeDefinitions");
        return false;
    }

    /**
     * Returns whether positioned deletes are supported.
     *
     * @return true
     */
    public boolean supportsPositionedDelete() {
        debugCodeCall("supportsPositionedDelete");
        return true;
    }

    /**
     * Returns whether positioned updates are supported.
     *
     * @return true
     */
    public boolean supportsPositionedUpdate() {
        debugCodeCall("supportsPositionedUpdate");
        return true;
    }

    /**
     * Returns whether SELECT ... FOR UPDATE is supported.
     *
     * @return true
     */
    public boolean supportsSelectForUpdate() {
        debugCodeCall("supportsSelectForUpdate");
        return true;
    }

    /**
     * Returns whether stored procedures are supported.
     *
     * @return false
     */
    public boolean supportsStoredProcedures() {
        debugCodeCall("supportsStoredProcedures");
        return false;
    }

    /**
     * Returns whether subqueries (SELECT) in comparisons are supported.
     *
     * @return true
     */
    public boolean supportsSubqueriesInComparisons() {
        debugCodeCall("supportsSubqueriesInComparisons");
        return true;
    }

    /**
     * Returns whether SELECT in EXISTS is supported.
     *
     * @return true
     */
    public boolean supportsSubqueriesInExists() {
        debugCodeCall("supportsSubqueriesInExists");
        return true;
    }

    /**
     * Returns whether IN(SELECT...) is supported.
     *
     * @return true
     */
    public boolean supportsSubqueriesInIns() {
        debugCodeCall("supportsSubqueriesInIns");
        return true;
    }

    /**
     * Returns whether subqueries in quantified expression are supported.
     *
     * @return true
     */
    public boolean supportsSubqueriesInQuantifieds() {
        debugCodeCall("supportsSubqueriesInQuantifieds");
        return true;
    }

    /**
     * Returns whether correlated subqueries are supported.
     *
     * @return true
     */
    public boolean supportsCorrelatedSubqueries() {
        debugCodeCall("supportsCorrelatedSubqueries");
        return true;
    }

    /**
     * Returns whether UNION SELECT is supported.
     *
     * @return true
     */
    public boolean supportsUnion() {
        debugCodeCall("supportsUnion");
        return true;
    }

    /**
     * Returns whether UNION ALL SELECT is supported.
     *
     * @return true
     */
    public boolean supportsUnionAll() {
        debugCodeCall("supportsUnionAll");
        return true;
    }

    /**
     * Returns whether open result sets accross commits are supported.
     *
     * @return false
     */
    public boolean supportsOpenCursorsAcrossCommit() {
        debugCodeCall("supportsOpenCursorsAcrossCommit");
        return false;
    }

    /**
     * Returns whether open result sets accross rollback are supported.
     *
     * @return false
     */
    public boolean supportsOpenCursorsAcrossRollback() {
        debugCodeCall("supportsOpenCursorsAcrossRollback");
        return false;
    }

    /**
     * Returns whether open statements accross commit are supported.
     *
     * @return true
     */
    public boolean supportsOpenStatementsAcrossCommit() {
        debugCodeCall("supportsOpenStatementsAcrossCommit");
        return true;
    }

    /**
     * Returns whether open statements accross rollback are supported.
     *
     * @return true
     */
    public boolean supportsOpenStatementsAcrossRollback() {
        debugCodeCall("supportsOpenStatementsAcrossRollback");
        return true;
    }

    /**
     * Returns whether transactions are supported.
     *
     * @return true
     */
    public boolean supportsTransactions() {
        debugCodeCall("supportsTransactions");
        return true;
    }

    /**
     * Returns whether a specific transaction isolation level is supported.
     *
     * @return true
     */
    public boolean supportsTransactionIsolationLevel(int level) {
        debugCodeCall("supportsTransactionIsolationLevel");
        return true;
    }

    /**
     * Returns whether data manipulation and CREATE/DROP is supported in
     * transactions.
     *
     * @return false
     */
    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        debugCodeCall("supportsDataDefinitionAndDataManipulationTransactions");
        return false;
    }

    /**
     * Returns whether only data manipulations are supported in transactions.
     *
     * @return true
     */
    public boolean supportsDataManipulationTransactionsOnly() {
        debugCodeCall("supportsDataManipulationTransactionsOnly");
        return true;
    }

    /**
     * Returns whether CREATE/DROP commit an open transaction.
     *
     * @return true
     */
    public boolean dataDefinitionCausesTransactionCommit() {
        debugCodeCall("dataDefinitionCausesTransactionCommit");
        return true;
    }

    /**
     * Returns whether CREATE/DROP do not affect transactions.
     *
     * @return false
     */
    public boolean dataDefinitionIgnoredInTransactions() {
        debugCodeCall("dataDefinitionIgnoredInTransactions");
        return false;
    }

    /**
     * Returns whether a specific result set type is supported.
     * ResultSet.TYPE_SCROLL_SENSITIVE is notsupported.
     *
     * @return true for all types except ResultSet.TYPE_FORWARD_ONLY
     */
    public boolean supportsResultSetType(int type) {
        debugCodeCall("supportsResultSetType", type);
        return type != ResultSet.TYPE_SCROLL_SENSITIVE;
    }

    /**
     * Returns whether a specific result set concurrency is supported.
     * ResultSet.TYPE_SCROLL_SENSITIVE is notsupported.
     *
     * @return true if the type is not ResultSet.TYPE_SCROLL_SENSITIVE
     */
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        if(debug()) {
            debugCode("supportsResultSetConcurrency("+type+", "+concurrency+");");
        }
        return type != ResultSet.TYPE_SCROLL_SENSITIVE;
    }

    /**
     * Returns whether own updates are visible.
     *
     * @return false
     */
    public boolean ownUpdatesAreVisible(int type) {
        debugCodeCall("ownUpdatesAreVisible", type);
        return false;
    }

    /**
     * Returns whether own deletes are visible.
     *
     * @return false
     */
    public boolean ownDeletesAreVisible(int type) {
        debugCodeCall("ownDeletesAreVisible", type);
        return false;
    }

    /**
     * Returns whether own inserts are visible.
     * @return false
     */
    public boolean ownInsertsAreVisible(int type) {
        debugCodeCall("ownInsertsAreVisible", type);
        return false;
    }

    /**
     * Returns whether other updates are visible.
     * @return false
     */
    public boolean othersUpdatesAreVisible(int type) {
        debugCodeCall("othersUpdatesAreVisible", type);
        return false;
    }

    /**
     * Returns whether other deletes are visible.
     * @return false
     */
    public boolean othersDeletesAreVisible(int type) {
        debugCodeCall("othersDeletesAreVisible", type);
        return false;
    }

    /**
     * Returns whether other inserts are visible.
     * @return false
     */
    public boolean othersInsertsAreVisible(int type) {
        debugCodeCall("othersInsertsAreVisible", type);
        return false;
    }

    /**
     * Returns whether updates are dectected.
     * @return false
     */
    public boolean updatesAreDetected(int type) {
        debugCodeCall("updatesAreDetected", type);
        return false;
    }

    /**
     * Returns whether deletes are detected.
     * @return false
     */
    public boolean deletesAreDetected(int type) {
        debugCodeCall("deletesAreDetected", type);
        return false;
    }

    /**
     * Returns whether inserts are detected.
     * @return false
     */
    public boolean insertsAreDetected(int type) {
        debugCodeCall("insertsAreDetected", type);
        return false;
    }

    /**
     * Returns whether batch updates are supported.
     * @return true
     */
    public boolean supportsBatchUpdates() {
        debugCodeCall("supportsBatchUpdates");
        return true;
    }

    /**
     * Returns whether the maximum row size includes blobs.
     * @return false
     */
    public boolean doesMaxRowSizeIncludeBlobs() {
        debugCodeCall("doesMaxRowSizeIncludeBlobs");
        return false;
    }

    /**
     * Returns the default transaction isolation level.
     * @return Connection.TRANSACTION_READ_COMMITTED
     */
    public int getDefaultTransactionIsolation() {
        debugCodeCall("getDefaultTransactionIsolation");
        return Connection.TRANSACTION_READ_COMMITTED;
    }

    /**
     * Checks if for CREATE TABLE Test(ID INT), getTables returns Test as the
     * table name.
     * @return false
     */
    public boolean supportsMixedCaseIdentifiers() {
        debugCodeCall("supportsMixedCaseIdentifiers");
        return false;
    }

    /**
     * Checks if a table created with CREATE TABLE "Test"(ID INT) is a different
     * table than a table created with CREATE TABLE TEST(ID INT).
     * @return true
     */
    public boolean supportsMixedCaseQuotedIdentifiers() {
        debugCodeCall("supportsMixedCaseQuotedIdentifiers");
        return true;
    }

    /**
     * Checks if for CREATE TABLE Test(ID INT), getTables returns TEST as the
     * table name.
     * @return true
     */
    public boolean storesUpperCaseIdentifiers() {
        debugCodeCall("storesUpperCaseIdentifiers");
        return true;
    }

    /**
     * Checks if for CREATE TABLE Test(ID INT), getTables returns test as the
     * table name.
     * @return false
     */
    public boolean storesLowerCaseIdentifiers() {
        debugCodeCall("storesLowerCaseIdentifiers");
        return false;
    }

    /**
     * Checks if for CREATE TABLE Test(ID INT), getTables returns Test as the
     * table name.
     * @return false
     */
    public boolean storesMixedCaseIdentifiers() {
        debugCodeCall("storesMixedCaseIdentifiers");
        return false;
    }

    /**
     * Checks if for CREATE TABLE "Test"(ID INT), getTables returns TEST as the
     * table name.
     * @return false
     */
    public boolean storesUpperCaseQuotedIdentifiers() {
        debugCodeCall("storesUpperCaseQuotedIdentifiers");
        return false;
    }

    /**
     * Checks if for CREATE TABLE "Test"(ID INT), getTables returns test as the
     * table name.
     * @return false
     */
    public boolean storesLowerCaseQuotedIdentifiers() {
        debugCodeCall("storesLowerCaseQuotedIdentifiers");
        return false;
    }

    /**
     * Checks if for CREATE TABLE "Test"(ID INT), getTables returns Test as the
     * table name.
     * @return true
     */
    public boolean storesMixedCaseQuotedIdentifiers() {
        debugCodeCall("storesMixedCaseQuotedIdentifiers");
        return true;
    }

    /**
     * Returns the maximum length for hex values (characters).
     * @return 0 for limit is unknown
     */
    public int getMaxBinaryLiteralLength() {
        debugCodeCall("getMaxBinaryLiteralLength");
        return 0;
    }

    /**
     * Returns the maximum length for literals.
     * @return 0 for limit is unknown
     */
    public int getMaxCharLiteralLength() {
        debugCodeCall("getMaxCharLiteralLength");
        return 0;
    }

    /**
     * Returns the maximum length for column names.
     * @return 0 for limit is unknown
     */
    public int getMaxColumnNameLength() {
        debugCodeCall("getMaxColumnNameLength");
        return 0;
    }

    /**
     * Returns the maximum number of columns in GROUP BY.
     * @return 0 for limit is unknown
     */
    public int getMaxColumnsInGroupBy() {
        debugCodeCall("getMaxColumnsInGroupBy");
        return 0;
    }

    /**
     * Returns the maximum number of columns in CREATE INDEX.
     * @return 0 for limit is unknown
     */
    public int getMaxColumnsInIndex() {
        debugCodeCall("getMaxColumnsInIndex");
        return 0;
    }

    /**
     * Returns the maximum number of columns in ORDER BY.
     * @return 0 for limit is unknown
     */
    public int getMaxColumnsInOrderBy() {
        debugCodeCall("getMaxColumnsInOrderBy");
        return 0;
    }

    /**
     * Returns the maximum number of columns in SELECT.
     * @return 0 for limit is unknown
     */
    public int getMaxColumnsInSelect() {
        debugCodeCall("getMaxColumnsInSelect");
        return 0;
    }

    /**
     * Returns the maximum number of columns in CREATE TABLE.
     * @return 0 for limit is unknown
     */
    public int getMaxColumnsInTable() {
        debugCodeCall("getMaxColumnsInTable");
        return 0;
    }

    /**
     * Returns the maximum number of open connection.
     * @return 0 for limit is unknown
     */
    public int getMaxConnections() {
        debugCodeCall("getMaxConnections");
        return 0;
    }

    /**
     * Returns the maximum length for a cursor name.
     * @return 0 for limit is unknown
     */
    public int getMaxCursorNameLength() {
        debugCodeCall("getMaxCursorNameLength");
        return 0;
    }

    /**
     * Returns the maximum length for an index (in bytes).
     * @return 0 for limit is unknown
     */
    public int getMaxIndexLength() {
        debugCodeCall("getMaxIndexLength");
        return 0;
    }

    /**
     * Returns the maximum length for a schema name.
     * @return 0 for limit is unknown
     */
    public int getMaxSchemaNameLength() {
        debugCodeCall("getMaxSchemaNameLength");
        return 0;
    }

    /**
     * Returns the maximum length for a procedure name.
     * @return 0 for limit is unknown
     */
    public int getMaxProcedureNameLength() {
        debugCodeCall("getMaxProcedureNameLength");
        return 0;
    }

    /**
     * Returns the maximum length for a catalog name.
     * @return 0 for limit is unknown
     */
    public int getMaxCatalogNameLength() {
        debugCodeCall("getMaxCatalogNameLength");
        return 0;
    }

    /**
     * Returns the maximum size of a row (in bytes).
     * @return 0 for limit is unknown
     */
    public int getMaxRowSize() {
        debugCodeCall("getMaxRowSize");
        return 0;
    }

    /**
     * Returns the maximum length of a statement.
     * @return 0 for limit is unknown
     */
    public int getMaxStatementLength() {
        debugCodeCall("getMaxStatementLength");
        return 0;
    }

    /**
     * Returns the maximum number of open statements.
     * @return 0 for limit is unknown
     */
    public int getMaxStatements() {
        debugCodeCall("getMaxStatements");
        return 0;
    }

    /**
     * Returns the maximum length for a table name.
     * @return 0 for limit is unknown
     */
    public int getMaxTableNameLength() {
        debugCodeCall("getMaxTableNameLength");
        return 0;
    }

    /**
     * Returns the maximum number of tables in a SELECT.
     * @return 0 for limit is unknown
     */
    public int getMaxTablesInSelect() {
        debugCodeCall("getMaxTablesInSelect");
        return 0;
    }

    /**
     * Returns the maximum length for a user name.
     * @return 0 for limit is unknown
     */
    public int getMaxUserNameLength() {
        debugCodeCall("getMaxUserNameLength");
        return 0;
    }

    /**
     * Does the database support savepoints.
     * @return true
     */
    public boolean supportsSavepoints() {
        debugCodeCall("supportsSavepoints");
        return true;
    }

    /**
     * Does the database support named parameters.
     * @return false
     */
    public boolean supportsNamedParameters() {
        debugCodeCall("supportsNamedParameters");
        return false;
    }

    /**
     * Does the database support multiple open result sets.
     * @return true
     */
    public boolean supportsMultipleOpenResults() {
        debugCodeCall("supportsMultipleOpenResults");
        return true;
    }

    /**
     * Does the database support getGeneratedKeys.
     * @return true
     */
    public boolean supportsGetGeneratedKeys() {
        debugCodeCall("supportsGetGeneratedKeys");
        return true;
    }

    /**
     * THIS FEATURE IS NOT SUPPORTED.
     *
     * @throws SQLException
     *                    Unsupported Feature (SQL State 0A000)
     */
    public ResultSet getSuperTypes(String catalog, String schemaPattern,
            String typeNamePattern) throws SQLException {
        try {
            if(debug()) {
                debugCode("getSuperTypes("
                        +quote(catalog)+", "
                        +quote(schemaPattern)+", "
                        +quote(typeNamePattern)+");");
            }
            throw Message.getUnsupportedException();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Get the list of super tables of a table.
     * This method currently returns an empty result set.
     *
     * <ul>
     * <li>1 TABLE_CAT (String) table catalog
     * <li>2 TABLE_SCHEM (String) table schema
     * <li>3 TABLE_NAME (String) table name
     * <li>4 SUPERTABLE_NAME (String) the name of the super table
     * </ul>
     *
     * @return an empty result set
     */
    public ResultSet getSuperTables(String catalog, String schemaPattern,
            String tableNamePattern) throws SQLException {
        try {
            if(debug()) {
                debugCode("getSuperTables("
                        +quote(catalog)+", "
                        +quote(schemaPattern)+", "
                        +quote(tableNamePattern)+");");
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "CATALOG_NAME TABLE_CAT, "
                    + "CATALOG_NAME TABLE_SCHEM, "
                    + "CATALOG_NAME TABLE_NAME, "
                    + "CATALOG_NAME SUPERTABLE_NAME "
                    + "FROM INFORMATION_SCHEMA.CATALOGS "
                    + "WHERE FALSE");
            return prep.executeQuery();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * THIS FEATURE IS NOT SUPPORTED.
     *
     * @throws SQLException
     *                    Unsupported Feature (SQL State 0A000)
     */
    public ResultSet getAttributes(String catalog, String schemaPattern,
            String typeNamePattern, String attributeNamePattern)
            throws SQLException {
        try {
            if(debug()) {
                debugCode("getAttributes("
                        +quote(catalog)+", "
                        +quote(schemaPattern)+", "
                        +quote(typeNamePattern)+", "
                        +quote(attributeNamePattern)+");");
            }
            throw Message.getUnsupportedException();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Does this database supports a result set holdability.
     *
     * @param holdability ResultSet.HOLD_CURSORS_OVER_COMMIT or CLOSE_CURSORS_AT_COMMIT
     * @return true if the holdability is ResultSet.CLOSE_CURSORS_AT_COMMIT
     */
    public boolean supportsResultSetHoldability(int holdability) {
        debugCodeCall("supportsResultSetHoldability", holdability);
        return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    /**
     * Gets the result set holdability.
     * @return ResultSet.CLOSE_CURSORS_AT_COMMIT
     */
    public int getResultSetHoldability() {
        debugCodeCall("getResultSetHoldability");
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    /**
     * Gets the major version of the database.
     * @return the major version
     */
    public int getDatabaseMajorVersion() {
        debugCodeCall("getDatabaseMajorVersion");
        return Constants.VERSION_MAJOR;
    }

    /**
     * Gets the minor version of the database.
     * @return the minor version
     */
    public int getDatabaseMinorVersion() {
        debugCodeCall("getDatabaseMinorVersion");
        return Constants.VERSION_MINOR;
    }

    /**
     * Gets the major version of the supported JDBC API.
     * @return the major version
     */
    public int getJDBCMajorVersion() throws SQLException {
        debugCodeCall("getJDBCMajorVersion");
        return Constants.VERSION_JDBC_MAJOR;
    }

    /**
     * Gets the minor version of the supported JDBC API.
     * @return the minor version
     */
    public int getJDBCMinorVersion() throws SQLException {
        debugCodeCall("getJDBCMinorVersion");
        return Constants.VERSION_JDBC_MINOR;
    }

    /**
     * Gets the SQL State type.
     * @return DatabaseMetaData.sqlStateSQL99
     */
    public int getSQLStateType() {
        debugCodeCall("getSQLStateType");
        return DatabaseMetaData.sqlStateSQL99;
    }

    /**
     * Does the database make a copy before updating.
     * @return false
     */
    public boolean locatorsUpdateCopy() {
        debugCodeCall("locatorsUpdateCopy");
        return false;
    }

    /**
     * Does the database support statement pooling.
     * @return false
     */
    public boolean supportsStatementPooling() {
        debugCodeCall("supportsStatementPooling");
        return false;
    }

    // =============================================================

    JdbcDatabaseMetaData(JdbcConnection conn, Trace trace, int id) {
        setTrace(trace, TraceObject.DATABASE_META_DATA, id);
        this.conn = conn;
    }

    private void checkClosed() throws SQLException {
        conn.checkClosed();
    }

    private String getPattern(String pattern) {
        return pattern == null ? "%" : pattern;
    }

    private String getSchemaPattern(String pattern) {
        return pattern == null ? "%" : pattern.length() == 0 ? Constants.SCHEMA_MAIN : pattern;
    }

    private String getCatalogPattern(String catalogPattern) {
        // Workaround for OpenOffice: getColumns is called with "" as the catalog
        return catalogPattern == null || catalogPattern.length()==0 ? "%" : catalogPattern;
    }

    /**
     * Get the lifetime of a rowid.
     * @return ROWID_UNSUPPORTED
     */
    //#ifdef JDK16
/*
    public RowIdLifetime getRowIdLifetime() {
        debugCodeCall("getRowIdLifetime");
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }
*/
    //#endif

    /**
     * Gets the list of schemas.
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        throw Message.getUnsupportedException();
    }

    /**
     * Returns whether the database supports calling functions using the call syntax.
     * @return true
     */
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        debugCodeCall("supportsStoredFunctionsUsingCallSyntax");
        return true;
    }

    /**
     * Returns whether an exception while autocommit is on closes all result sets.
     * @return false
     */
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        debugCodeCall("autoCommitFailureClosesAllResultSets");
        return false;
    }

    /**
     * Returns the client info properties.
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public ResultSet getClientInfoProperties() throws SQLException {
        throw Message.getUnsupportedException();
    }

    /**
     * Returns whether this database provides its own query generator.
     * @return false
     */
    public boolean providesQueryObjectGenerator() throws SQLException {
        debugCodeCall("providesQueryObjectGenerator");
        return false;
    }

    /**
     * Return an object of this class if possible.
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    //#ifdef JDK16
/*
    public Object unwrap(Class<?> iface) throws SQLException {
        throw Message.getUnsupportedException();
    }
*/
    //#endif

    /**
     * Checks if unwrap can return an object of this class.
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    //#ifdef JDK16
/*
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw Message.getUnsupportedException();
    }
*/
    //#endif

}

