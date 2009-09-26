/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
/*## Java 1.6 begin ##
import java.sql.RowIdLifetime;
## Java 1.6 end ##*/
import java.sql.SQLException;

import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.message.TraceObject;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;

/**
 * Represents the meta data for a database.
 */
public class JdbcDatabaseMetaData extends TraceObject implements DatabaseMetaData {

    private JdbcConnection conn;

    JdbcDatabaseMetaData(JdbcConnection conn, Trace trace, int id) {
        setTrace(trace, TraceObject.DATABASE_META_DATA, id);
        this.conn = conn;
    }

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
        return Constants.getFullVersion();
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
     * Gets the version number of the driver. The format is
     * [MajorVersion].[MinorVersion].
     *
     * @return the version number
     */
    public String getDriverVersion() {
        debugCodeCall("getDriverVersion");
        return Constants.getFullVersion();
    }

    /**
     * Gets the list of tables in the database. The result set is sorted by
     * TABLE_TYPE, TABLE_SCHEM, and TABLE_NAME.
     *
     * <ul>
     * <li>1 TABLE_CAT (String) table catalog </li>
     * <li>2 TABLE_SCHEM (String) table schema </li>
     * <li>3 TABLE_NAME (String) table name </li>
     * <li>4 TABLE_TYPE (String) table type </li>
     * <li>5 REMARKS (String) comment </li>
     * <li>6 TYPE_CAT (String) always null </li>
     * <li>7 TYPE_SCHEM (String) always null </li>
     * <li>8 TYPE_NAME (String) always null </li>
     * <li>9 SELF_REFERENCING_COL_NAME (String) always null </li>
     * <li>10 REF_GENERATION (String) always null </li>
     * <li>11 SQL (String) the create table statement or NULL for systems tables
     * </li>
     * </ul>
     *
     * @param catalogPattern null (to get all objects) or the catalog name
     * @param schemaPattern null (to get all objects) or a schema name
     *            (uppercase for unquoted names)
     * @param tableNamePattern null (to get all objects) or a table name
     *            (uppercase for unquoted names)
     * @param types null or a list of table types
     * @return the list of columns
     * @throws SQLException if the connection is closed
     */
    public ResultSet getTables(String catalogPattern, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getTables(" + quote(catalogPattern) + ", " + quote(schemaPattern) + ", " + quote(tableNamePattern)
                        + ", " + quoteArray(types) + ");");
            }
            checkClosed();
            String tableType;
            if (types != null && types.length > 0) {
                StatementBuilder buff = new StatementBuilder("TABLE_TYPE IN(");
                for (int i = 0; i < types.length; i++) {
                    buff.appendExceptFirst(", ");
                    buff.append('?');
                }
                tableType = buff.append(')').toString();
            } else {
                tableType = "TRUE";
            }
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "TABLE_CATALOG TABLE_CAT, "
                    + "TABLE_SCHEMA TABLE_SCHEM, "
                    + "TABLE_NAME, "
                    + "TABLE_TYPE, "
                    + "REMARKS, "
                    + "TYPE_NAME TYPE_CAT, "
                    + "TYPE_NAME TYPE_SCHEM, "
                    + "TYPE_NAME, "
                    + "TYPE_NAME SELF_REFERENCING_COL_NAME, "
                    + "TYPE_NAME REF_GENERATION, "
                    + "SQL "
                    + "FROM INFORMATION_SCHEMA.TABLES "
                    + "WHERE TABLE_CATALOG LIKE ? ESCAPE '\\' "
                    + "AND TABLE_SCHEMA LIKE ? ESCAPE '\\' "
                    + "AND TABLE_NAME LIKE ? ESCAPE '\\' "
                    + "AND (" + tableType + ") "
                    + "ORDER BY TABLE_TYPE, TABLE_SCHEMA, TABLE_NAME");
            prep.setString(1, getCatalogPattern(catalogPattern));
            prep.setString(2, getSchemaPattern(schemaPattern));
            prep.setString(3, getPattern(tableNamePattern));
            for (int i = 0; types != null && i < types.length; i++) {
                prep.setString(4 + i, types[i]);
            }
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of columns. The result set is sorted by TABLE_SCHEM,
     * TABLE_NAME, and ORDINAL_POSITION.
     *
     * <ul>
     * <li>1 TABLE_CAT (String) table catalog </li>
     * <li>2 TABLE_SCHEM (String) table schema </li>
     * <li>3 TABLE_NAME (String) table name </li>
     * <li>4 COLUMN_NAME (String) column name </li>
     * <li>5 DATA_TYPE (short) data type (see java.sql.Types) </li>
     * <li>6 TYPE_NAME (String) data type name ("INTEGER", "VARCHAR",...) </li>
     * <li>7 COLUMN_SIZE (int) precision </li>
     * <li>8 BUFFER_LENGTH (int) unused </li>
     * <li>9 DECIMAL_DIGITS (int) scale (0 for INTEGER and VARCHAR) </li>
     * <li>10 NUM_PREC_RADIX (int) radix (always 10) </li>
     * <li>11 NULLABLE (int) columnNoNulls or columnNullable</li>
     * <li>12 REMARKS (String) comment (always empty) </li>
     * <li>13 COLUMN_DEF (String) default value </li>
     * <li>14 SQL_DATA_TYPE (int) unused </li>
     * <li>15 SQL_DATETIME_SUB (int) unused </li>
     * <li>16 CHAR_OCTET_LENGTH (int) unused </li>
     * <li>17 ORDINAL_POSITION (int) the column index (1,2,...) </li>
     * <li>18 IS_NULLABLE (String) "NO" or "YES" </li>
     * <li>19 SCOPE_CATALOG (String) always null </li>
     * <li>20 SCOPE_SCHEMA (String) always null </li>
     * <li>21 SCOPE_TABLE (String) always null </li>
     * <li>22 SOURCE_DATA_TYPE (short) null </li>
     * <li>23 IS_AUTOINCREMENT (String) "NO" or "YES" </li>
     * </ul>
     *
     * @param catalogPattern null (to get all objects) or the catalog name
     * @param schemaPattern null (to get all objects) or a schema name
     *            (uppercase for unquoted names)
     * @param tableNamePattern null (to get all objects) or a table name
     *            (uppercase for unquoted names)
     * @param columnNamePattern null (to get all objects) or a column name
     *            (uppercase for unquoted names)
     * @return the list of columns
     * @throws SQLException if the connection is closed
     */
    public ResultSet getColumns(String catalogPattern, String schemaPattern,
            String tableNamePattern, String columnNamePattern)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getColumns(" + quote(catalogPattern)+", "
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
                    + "IS_NULLABLE IS_NULLABLE, "
                    + "CAST(SOURCE_DATA_TYPE AS VARCHAR) SCOPE_CATALOG, "
                    + "CAST(SOURCE_DATA_TYPE AS VARCHAR) SCOPE_SCHEMA, "
                    + "CAST(SOURCE_DATA_TYPE AS VARCHAR) SCOPE_TABLE, "
                    + "SOURCE_DATA_TYPE, "
                    + "CASE WHEN SEQUENCE_NAME IS NULL THEN 'NO' ELSE 'YES' END IS_AUTOINCREMENT "
                    + "FROM INFORMATION_SCHEMA.COLUMNS "
                    + "WHERE TABLE_CATALOG LIKE ? ESCAPE '\\' "
                    + "AND TABLE_SCHEMA LIKE ? ESCAPE '\\' "
                    + "AND TABLE_NAME LIKE ? ESCAPE '\\' "
                    + "AND COLUMN_NAME LIKE ? ESCAPE '\\' "
                    + "ORDER BY TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION");
            prep.setString(1, getCatalogPattern(catalogPattern));
            prep.setString(2, getSchemaPattern(schemaPattern));
            prep.setString(3, getPattern(tableNamePattern));
            prep.setString(4, getPattern(columnNamePattern));
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of indexes for this database. The primary key index (if
     * there is one) is also listed, with the name PRIMARY_KEY. The result set
     * is sorted by NON_UNIQUE ('false' first), TYPE, TABLE_SCHEM, INDEX_NAME,
     * and ORDINAL_POSITION.
     *
     * <ul>
     * <li>1 TABLE_CAT (String) table catalog </li>
     * <li>2 TABLE_SCHEM (String) table schema </li>
     * <li>3 TABLE_NAME (String) table name </li>
     * <li>4 NON_UNIQUE (boolean) 'false' for unique, 'true' for non-unique
     * </li>
     * <li>5 INDEX_QUALIFIER (String) index catalog </li>
     * <li>6 INDEX_NAME (String) index name </li>
     * <li>7 TYPE (short) the index type (always tableIndexOther) </li>
     * <li>8 ORDINAL_POSITION (short) column index (1, 2, ...) </li>
     * <li>9 COLUMN_NAME (String) column name </li>
     * <li>10 ASC_OR_DESC (String) ascending or descending (always 'A') </li>
     * <li>11 CARDINALITY (int) numbers of unique values </li>
     * <li>12 PAGES (int) number of pages use (always 0) </li>
     * <li>13 FILTER_CONDITION (String) filter condition (always empty) </li>
     * <li>14 SORT_TYPE (int) the sort type bit map: 1=DESCENDING,
     * 2=NULLS_FIRST, 4=NULLS_LAST </li>
     * </ul>
     *
     * @param catalogPattern null or the catalog name
     * @param schemaPattern schema name (must be specified)
     * @param tableName table name (must be specified)
     * @param unique only unique indexes
     * @param approximate is ignored
     * @return the list of indexes and columns
     * @throws SQLException if the connection is closed
     */
    public ResultSet getIndexInfo(String catalogPattern, String schemaPattern, String tableName, boolean unique, boolean approximate)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getIndexInfo(" + quote(catalogPattern) + ", " + quote(schemaPattern) + ", " + quote(tableName) + ", "
                        + unique + ", " + approximate + ");");
            }
            String uniqueCondition;
            if (unique) {
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
                    // TODO meta data for number of unique values in an index
                    + "CARDINALITY, "
                    + "PAGES, "
                    + "FILTER_CONDITION, "
                    + "SORT_TYPE "
                    + "FROM INFORMATION_SCHEMA.INDEXES "
                    + "WHERE TABLE_CATALOG LIKE ? ESCAPE '\\' "
                    + "AND TABLE_SCHEMA LIKE ? ESCAPE '\\' "
                    + "AND (" + uniqueCondition + ") "
                    + "AND TABLE_NAME = ? "
                    + "ORDER BY NON_UNIQUE, TYPE, TABLE_SCHEM, INDEX_NAME, ORDINAL_POSITION");
            prep.setString(1, getCatalogPattern(catalogPattern));
            prep.setString(2, getSchemaPattern(schemaPattern));
            prep.setString(3, tableName);
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the primary key columns for a table. The result set is sorted by
     * TABLE_SCHEM, and COLUMN_NAME (and not by KEY_SEQ).
     *
     * <ul>
     * <li>1 TABLE_CAT (String) table catalog</li>
     * <li>2 TABLE_SCHEM (String) table schema</li>
     * <li>3 TABLE_NAME (String) table name</li>
     * <li>4 COLUMN_NAME (String) column name</li>
     * <li>5 KEY_SEQ (short) the column index of this column (1,2,...)</li>
     * <li>6 PK_NAME (String) the name of the primary key index</li>
     * </ul>
     *
     * @param catalogPattern null or the catalog name
     * @param schemaPattern schema name (must be specified)
     * @param tableName table name (must be specified)
     * @return the list of primary key columns
     * @throws SQLException if the connection is closed
     */
    public ResultSet getPrimaryKeys(String catalogPattern, String schemaPattern, String tableName) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getPrimaryKeys("
                        +quote(catalogPattern)+", "
                        +quote(schemaPattern)+", "
                        +quote(tableName)+");");
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "TABLE_CATALOG TABLE_CAT, "
                    + "TABLE_SCHEMA TABLE_SCHEM, "
                    + "TABLE_NAME, "
                    + "COLUMN_NAME, "
                    + "ORDINAL_POSITION KEY_SEQ, "
                    + "IFNULL(CONSTRAINT_NAME, INDEX_NAME) PK_NAME "
                    + "FROM INFORMATION_SCHEMA.INDEXES "
                    + "WHERE TABLE_CATALOG LIKE ? ESCAPE '\\' "
                    + "AND TABLE_SCHEMA LIKE ? ESCAPE '\\' "
                    + "AND TABLE_NAME = ? "
                    + "AND PRIMARY_KEY = TRUE "
                    + "ORDER BY COLUMN_NAME");
            prep.setString(1, getCatalogPattern(catalogPattern));
            prep.setString(2, getSchemaPattern(schemaPattern));
            prep.setString(3, tableName);
            return prep.executeQuery();
        } catch (Exception e) {
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
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the user name as passed to DriverManager.getConnection(url, user,
     * password).
     *
     * @return the user name
     */
    public String getUserName() throws SQLException {
        try {
            debugCodeCall("getUserName");
            return conn.getUser();
        } catch (Exception e) {
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
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks is NULL values are sorted high (bigger than any non-null values).
     *
     * @return false by default; true if the system property h2.sortNullsHigh is
     *         set to true
     */
    public boolean nullsAreSortedHigh() {
        debugCodeCall("nullsAreSortedHigh");
        return SysProperties.SORT_NULLS_HIGH;
    }

    /**
     * Checks is NULL values are sorted low (smaller than any non-null values).
     *
     * @return true by default; false if the system property h2.sortNullsHigh is
     *         set to true
     */
    public boolean nullsAreSortedLow() {
        debugCodeCall("nullsAreSortedLow");
        return !SysProperties.SORT_NULLS_HIGH;
    }

    /**
     * Checks is NULL values are sorted at the beginning (no matter if ASC or
     * DESC is used).
     *
     * @return false
     */
    public boolean nullsAreSortedAtStart() {
        debugCodeCall("nullsAreSortedAtStart");
        return false;
    }

    /**
     * Checks is NULL values are sorted at the end (no matter if ASC or DESC is
     * used).
     *
     * @return false
     */
    public boolean nullsAreSortedAtEnd() {
        debugCodeCall("nullsAreSortedAtEnd");
        return false;
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
     * Gets the list of procedures. The result set is sorted by PROCEDURE_SCHEM,
     * PROCEDURE_NAME, and NUM_INPUT_PARAMS. There are potentially multiple
     * procedures with the same name, each with a different number of input
     * parameters.
     *
     * <ul>
     * <li>1 PROCEDURE_CAT (String) catalog </li>
     * <li>2 PROCEDURE_SCHEM (String) schema </li>
     * <li>3 PROCEDURE_NAME (String) name </li>
     * <li>4 NUM_INPUT_PARAMS (int) the number of arguments </li>
     * <li>5 NUM_OUTPUT_PARAMS (int) for future use, always 0 </li>
     * <li>6 NUM_RESULT_SETS (int) for future use, always 0 </li>
     * <li>7 REMARKS (String) description </li>
     * <li>8 PROCEDURE_TYPE (short) if this procedure returns a result
     * (procedureNoResult or procedureReturnsResult) </li>
     * <li>9 SPECIFIC_NAME (String) name </li>
     * </ul>
     *
     * @param catalogPattern null or the catalog name
     * @param schemaPattern schema name (must be specified)
     * @param procedureNamePattern the procedure name pattern
     * @return the procedures
     * @throws SQLException if the connection is closed
     */
    public ResultSet getProcedures(String catalogPattern, String schemaPattern,
            String procedureNamePattern) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getProcedures("
                        +quote(catalogPattern)+", "
                        +quote(schemaPattern)+", "
                        +quote(procedureNamePattern)+");");
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "ALIAS_CATALOG PROCEDURE_CAT, "
                    + "ALIAS_SCHEMA PROCEDURE_SCHEM, "
                    + "ALIAS_NAME PROCEDURE_NAME, "
                    + "COLUMN_COUNT NUM_INPUT_PARAMS, "
                    + "ZERO() NUM_OUTPUT_PARAMS, "
                    + "ZERO() NUM_RESULT_SETS, "
                    + "REMARKS, "
                    + "RETURNS_RESULT PROCEDURE_TYPE, "
                    + "ALIAS_NAME SPECIFIC_NAME "
                    + "FROM INFORMATION_SCHEMA.FUNCTION_ALIASES "
                    + "WHERE ALIAS_CATALOG LIKE ? ESCAPE '\\' "
                    + "AND ALIAS_SCHEMA LIKE ? ESCAPE '\\' "
                    + "AND ALIAS_NAME LIKE ? ESCAPE '\\' "
                    + "ORDER BY PROCEDURE_SCHEM, PROCEDURE_NAME, NUM_INPUT_PARAMS");
            prep.setString(1, getCatalogPattern(catalogPattern));
            prep.setString(2, getSchemaPattern(schemaPattern));
            prep.setString(3, getPattern(procedureNamePattern));
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of procedure columns. The result set is sorted by
     * PROCEDURE_SCHEM, PROCEDURE_NAME, NUM_INPUT_PARAMS, and POS.
     * There are potentially multiple procedures with the same name, each with a
     * different number of input parameters.
     *
     * <ul>
     * <li>1 PROCEDURE_CAT (String) catalog </li>
     * <li>2 PROCEDURE_SCHEM (String) schema </li>
     * <li>3 PROCEDURE_NAME (String) name </li>
     * <li>4 COLUMN_NAME (String) column name </li>
     * <li>5 COLUMN_TYPE (short) column type </li>
     * <li>6 DATA_TYPE (short) sql type </li>
     * <li>7 TYPE_NAME (String) type name </li>
     * <li>8 PRECISION (int) precision </li>
     * <li>9 LENGTH (int) length </li>
     * <li>10 SCALE (short) scale </li>
     * <li>11 RADIX (int) always 10 </li>
     * <li>12 NULLABLE (short) nullable </li>
     * <li>13 REMARKS (String) description </li>
     * <li>14 COLUMN_DEF (String) always null </li>
     * <li>15 SQL_DATA_TYPE (int) for future use, always 0 </li>
     * <li>16 SQL_DATETIME_SUB (int) for future use, always 0 </li>
     * <li>17 CHAR_OCTET_LENGTH (int) always null </li>
     * <li>18 ORDINAL_POSITION (int) the parameter index
     * starting from 1 (0 is the return value) </li>
     * <li>19 IS_NULLABLE (String) always "YES" </li>
     * <li>20 SPECIFIC_NAME (String) name </li>
     * </ul>
     *
     * @param catalogPattern null or the catalog name
     * @param schemaPattern schema name (must be specified)
     * @param procedureNamePattern the procedure name pattern
     * @param columnNamePattern the procedure name pattern
     * @return the procedure columns
     * @throws SQLException if the connection is closed
     */
    public ResultSet getProcedureColumns(String catalogPattern, String schemaPattern,
            String procedureNamePattern, String columnNamePattern)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getProcedureColumns("
                        +quote(catalogPattern)+", "
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
                    + "REMARKS, "
                    + "COLUMN_DEFAULT COLUMN_DEF, "
                    + "0 SQL_DATA_TYPE, "
                    + "0 SQL_DATETIME_SUB, "
                    + "0 CHAR_OCTET_LENGTH, "
                    + "POS ORDINAL_POSITION, "
                    + "'YES' IS_NULLABLE, "
                    + "ALIAS_NAME SPECIFIC_NAME "
                    + "FROM INFORMATION_SCHEMA.FUNCTION_COLUMNS "
                    + "WHERE ALIAS_CATALOG LIKE ? ESCAPE '\\' "
                    + "AND ALIAS_SCHEMA LIKE ? ESCAPE '\\' "
                    + "AND ALIAS_NAME LIKE ? ESCAPE '\\' "
                    + "AND COLUMN_NAME LIKE ? ESCAPE '\\' "
                    + "ORDER BY PROCEDURE_SCHEM, PROCEDURE_NAME, ORDINAL_POSITION");
            prep.setString(1, getCatalogPattern(catalogPattern));
            prep.setString(2, getSchemaPattern(schemaPattern));
            prep.setString(3, getPattern(procedureNamePattern));
            prep.setString(4, getPattern(columnNamePattern));
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of schemas.
     * The result set is sorted by TABLE_SCHEM.
     *
     * <ul>
     * <li>1 TABLE_SCHEM (String) schema name
     * </li><li>2 TABLE_CATALOG (String) catalog name
     * </li><li>3 IS_DEFAULT (boolean) if this is the default schema
     * </li></ul>
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
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of catalogs.
     * The result set is sorted by TABLE_CAT.
     *
     * <ul>
     * <li>1 TABLE_CAT (String) catalog name
     * </li></ul>
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
        } catch (Exception e) {
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
     * </li></ul>
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
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of column privileges. The result set is sorted by
     * COLUMN_NAME and PRIVILEGE
     *
     * <ul>
     * <li>1 TABLE_CAT (String) table catalog </li>
     * <li>2 TABLE_SCHEM (String) table schema </li>
     * <li>3 TABLE_NAME (String) table name </li>
     * <li>4 COLUMN_NAME (String) column name </li>
     * <li>5 GRANTOR (String) grantor of access </li>
     * <li>6 GRANTEE (String) grantee of access </li>
     * <li>7 PRIVILEGE (String) SELECT, INSERT, UPDATE, DELETE or REFERENCES
     * (only one per row) </li>
     * <li>8 IS_GRANTABLE (String) YES means the grantee can grant access to
     * others </li>
     * </ul>
     *
     * @param catalogPattern null (to get all objects) or the catalog name
     * @param schemaPattern null (to get all objects) or a schema name (uppercase for
     *            unquoted names)
     * @param table a table name (uppercase for unquoted names)
     * @param columnNamePattern null (to get all objects) or a column name
     *            (uppercase for unquoted names)
     * @return the list of privileges
     * @throws SQLException if the connection is closed
     */
    public ResultSet getColumnPrivileges(String catalogPattern, String schemaPattern,
            String table, String columnNamePattern) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getColumnPrivileges("
                        +quote(catalogPattern)+", "
                        +quote(schemaPattern)+", "
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
                    + "WHERE TABLE_CATALOG LIKE ? ESCAPE '\\' "
                    + "AND TABLE_SCHEMA LIKE ? ESCAPE '\\' "
                    + "AND TABLE_NAME = ? "
                    + "AND COLUMN_NAME LIKE ? ESCAPE '\\' "
                    + "ORDER BY COLUMN_NAME, PRIVILEGE");
            prep.setString(1, getCatalogPattern(catalogPattern));
            prep.setString(2, getSchemaPattern(schemaPattern));
            prep.setString(3, table);
            prep.setString(4, getPattern(columnNamePattern));
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of table privileges. The result set is sorted by
     * TABLE_SCHEM, TABLE_NAME, and PRIVILEGE.
     *
     * <ul>
     * <li>1 TABLE_CAT (String) table catalog </li>
     * <li>2 TABLE_SCHEM (String) table schema </li>
     * <li>3 TABLE_NAME (String) table name </li>
     * <li>4 GRANTOR (String) grantor of access </li>
     * <li>5 GRANTEE (String) grantee of access </li>
     * <li>6 PRIVILEGE (String) SELECT, INSERT, UPDATE, DELETE or REFERENCES
     * (only one per row) </li>
     * <li>7 IS_GRANTABLE (String) YES means the grantee can grant access to
     * others </li>
     * </ul>
     *
     * @param catalogPattern null (to get all objects) or the catalog name
     * @param schemaPattern null (to get all objects) or a schema name
     *            (uppercase for unquoted names)
     * @param tableNamePattern null (to get all objects) or a table name
     *            (uppercase for unquoted names)
     * @return the list of privileges
     * @throws SQLException if the connection is closed
     */
    public ResultSet getTablePrivileges(String catalogPattern, String schemaPattern, String tableNamePattern) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getTablePrivileges("
                        +quote(catalogPattern)+", "
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
                    + "WHERE TABLE_CATALOG LIKE ? ESCAPE '\\' "
                    + "AND TABLE_SCHEMA LIKE ? ESCAPE '\\' "
                    + "AND TABLE_NAME LIKE ? ESCAPE '\\' "
                    + "ORDER BY TABLE_SCHEM, TABLE_NAME, PRIVILEGE");
            prep.setString(1, getCatalogPattern(catalogPattern));
            prep.setString(2, getSchemaPattern(schemaPattern));
            prep.setString(3, getPattern(tableNamePattern));
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of columns that best identifier a row in a table.
     * The list is ordered by SCOPE.
     *
     * <ul>
     * <li>1 SCOPE (short) scope of result (always bestRowSession)
     * </li><li>2 COLUMN_NAME (String) column name
     * </li><li>3 DATA_TYPE (short) SQL data type, see also java.sql.Types
     * </li><li>4 TYPE_NAME (String) type name
     * </li><li>5 COLUMN_SIZE (int) precision
     * </li><li>6 BUFFER_LENGTH (int) unused
     * </li><li>7 DECIMAL_DIGITS (short) scale
     * </li><li>8 PSEUDO_COLUMN (short) (always bestRowNotPseudo)
     * </li></ul>
     *
     * @param catalogPattern null (to get all objects) or the catalog name
     * @param schemaPattern schema name (must be specified)
     * @param tableName table name (must be specified)
     * @param scope ignored
     * @param nullable ignored
     * @return the primary key index
     * @throws SQLException if the connection is closed
     */
    public ResultSet getBestRowIdentifier(String catalogPattern, String schemaPattern,
            String tableName, int scope, boolean nullable) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getBestRowIdentifier("
                        +quote(catalogPattern)+", "
                        +quote(schemaPattern)+", "
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
                    + "AND C.TABLE_CATALOG LIKE ? ESCAPE '\\' "
                    + "AND C.TABLE_SCHEMA LIKE ? ESCAPE '\\' "
                    + "AND C.TABLE_NAME = ? "
                    + "AND I.PRIMARY_KEY = TRUE "
                    + "ORDER BY SCOPE");
            // SCOPE
            prep.setInt(1, DatabaseMetaData.bestRowSession);
            // PSEUDO_COLUMN
            prep.setInt(2, DatabaseMetaData.bestRowNotPseudo);
            prep.setString(3, getCatalogPattern(catalogPattern));
            prep.setString(4, getSchemaPattern(schemaPattern));
            prep.setString(5, tableName);
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Get the list of columns that are update when any value is updated.
     * The result set is always empty.
     *
     * <ul>
     * <li>1 SCOPE (int) not used
     * </li><li>2 COLUMN_NAME (String) column name
     * </li><li>3 DATA_TYPE (int) SQL data type - see also java.sql.Types
     * </li><li>4 TYPE_NAME (String) data type name
     * </li><li>5 COLUMN_SIZE (int) precision
     * </li><li>6 BUFFER_LENGTH (int) length (bytes)
     * </li><li>7 DECIMAL_DIGITS (int) scale
     * </li><li>8 PSEUDO_COLUMN (int) is this column a pseudo column
     * </li></ul>
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
            if (isDebugEnabled()) {
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
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of primary key columns that are referenced by a table. The
     * result set is sorted by PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME,
     * FK_NAME, KEY_SEQ.
     *
     * <ul>
     * <li>1 PKTABLE_CAT (String) primary catalog </li>
     * <li>2 PKTABLE_SCHEM (String) primary schema </li>
     * <li>3 PKTABLE_NAME (String) primary table </li>
     * <li>4 PKCOLUMN_NAME (String) primary column </li>
     * <li>5 FKTABLE_CAT (String) foreign catalog </li>
     * <li>6 FKTABLE_SCHEM (String) foreign schema </li>
     * <li>7 FKTABLE_NAME (String) foreign table </li>
     * <li>8 FKCOLUMN_NAME (String) foreign column </li>
     * <li>9 KEY_SEQ (short) sequence number (1, 2, ...) </li>
     * <li>10 UPDATE_RULE (short) action on update (see
     * DatabaseMetaData.importedKey...) </li>
     * <li>11 DELETE_RULE (short) action on delete (see
     * DatabaseMetaData.importedKey...) </li>
     * <li>12 FK_NAME (String) foreign key name </li>
     * <li>13 PK_NAME (String) primary key name </li>
     * <li>14 DEFERRABILITY (short) deferrable or not (always
     * importedKeyNotDeferrable) </li>
     * </ul>
     *
     * @param catalogPattern null (to get all objects) or the catalog name
     * @param schemaPattern the schema name of the foreign table
     * @param tableName the name of the foreign table
     * @return the result set
     * @throws SQLException if the connection is closed
     */
    public ResultSet getImportedKeys(String catalogPattern, String schemaPattern, String tableName) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getImportedKeys("
                        +quote(catalogPattern)+", "
                        +quote(schemaPattern)+", "
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
                    + "WHERE FKTABLE_CATALOG LIKE ? ESCAPE '\\' "
                    + "AND FKTABLE_SCHEMA LIKE ? ESCAPE '\\' "
                    + "AND FKTABLE_NAME = ? "
                    + "ORDER BY PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, FK_NAME, KEY_SEQ");
            prep.setString(1, getCatalogPattern(catalogPattern));
            prep.setString(2, getSchemaPattern(schemaPattern));
            prep.setString(3, tableName);
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of foreign key columns that reference a table. The result
     * set is sorted by FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, FK_NAME,
     * KEY_SEQ.
     *
     * <ul>
     * <li>1 PKTABLE_CAT (String) primary catalog </li>
     * <li>2 PKTABLE_SCHEM (String) primary schema </li>
     * <li>3 PKTABLE_NAME (String) primary table </li>
     * <li>4 PKCOLUMN_NAME (String) primary column </li>
     * <li>5 FKTABLE_CAT (String) foreign catalog </li>
     * <li>6 FKTABLE_SCHEM (String) foreign schema </li>
     * <li>7 FKTABLE_NAME (String) foreign table </li>
     * <li>8 FKCOLUMN_NAME (String) foreign column </li>
     * <li>9 KEY_SEQ (short) sequence number (1,2,...) </li>
     * <li>10 UPDATE_RULE (short) action on update (see
     * DatabaseMetaData.importedKey...) </li>
     * <li>11 DELETE_RULE (short) action on delete (see
     * DatabaseMetaData.importedKey...) </li>
     * <li>12 FK_NAME (String) foreign key name </li>
     * <li>13 PK_NAME (String) primary key name </li>
     * <li>14 DEFERRABILITY (short) deferrable or not (always
     * importedKeyNotDeferrable) </li>
     * </ul>
     *
     * @param catalogPattern null or the catalog name
     * @param schemaPattern the schema name of the primary table
     * @param tableName the name of the primary table
     * @return the result set
     * @throws SQLException if the connection is closed
     */
    public ResultSet getExportedKeys(String catalogPattern, String schemaPattern, String tableName)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getExportedKeys("
                        +quote(catalogPattern)+", "
                        +quote(schemaPattern)+", "
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
                    + "WHERE PKTABLE_CATALOG LIKE ? ESCAPE '\\' "
                    + "AND PKTABLE_SCHEMA LIKE ? ESCAPE '\\' "
                    + "AND PKTABLE_NAME = ? "
                    + "ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, FK_NAME, KEY_SEQ");
            prep.setString(1, getCatalogPattern(catalogPattern));
            prep.setString(2, getSchemaPattern(schemaPattern));
            prep.setString(3, tableName);
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of foreign key columns that references a table, as well as
     * the list of primary key columns that are references by a table. The
     * result set is sorted by FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME,
     * FK_NAME, KEY_SEQ.
     *
     * <ul>
     * <li>1 PKTABLE_CAT (String) primary catalog </li>
     * <li>2 PKTABLE_SCHEM (String) primary schema </li>
     * <li>3 PKTABLE_NAME (String) primary table </li>
     * <li>4 PKCOLUMN_NAME (String) primary column </li>
     * <li>5 FKTABLE_CAT (String) foreign catalog </li>
     * <li>6 FKTABLE_SCHEM (String) foreign schema </li>
     * <li>7 FKTABLE_NAME (String) foreign table </li>
     * <li>8 FKCOLUMN_NAME (String) foreign column </li>
     * <li>9 KEY_SEQ (short) sequence number (1,2,...) </li>
     * <li>10 UPDATE_RULE (short) action on update (see
     * DatabaseMetaData.importedKey...) </li>
     * <li>11 DELETE_RULE (short) action on delete (see
     * DatabaseMetaData.importedKey...) </li>
     * <li>12 FK_NAME (String) foreign key name </li>
     * <li>13 PK_NAME (String) primary key name </li>
     * <li>14 DEFERRABILITY (short) deferrable or not (always
     * importedKeyNotDeferrable) </li>
     * </ul>
     *
     * @param primaryCatalogPattern null or the catalog name
     * @param primarySchemaPattern the schema name of the primary table (must be
     *            specified)
     * @param primaryTable the name of the primary table (must be specified)
     * @param foreignCatalogPattern null or the catalog name
     * @param foreignSchemaPattern the schema name of the foreign table (must be
     *            specified)
     * @param foreignTable the name of the foreign table (must be specified)
     * @return the result set
     * @throws SQLException if the connection is closed
     */
    public ResultSet getCrossReference(String primaryCatalogPattern,
            String primarySchemaPattern, String primaryTable, String foreignCatalogPattern,
            String foreignSchemaPattern, String foreignTable) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getCrossReference("
                        +quote(primaryCatalogPattern)+", "
                        +quote(primarySchemaPattern)+", "
                        +quote(primaryTable)+", "
                        +quote(foreignCatalogPattern)+", "
                        +quote(foreignSchemaPattern)+", "
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
                    + "WHERE PKTABLE_CATALOG LIKE ? ESCAPE '\\' "
                    + "AND PKTABLE_SCHEMA LIKE ? ESCAPE '\\' "
                    + "AND PKTABLE_NAME = ? "
                    + "AND FKTABLE_CATALOG LIKE ? ESCAPE '\\' "
                    + "AND FKTABLE_SCHEMA LIKE ? ESCAPE '\\' "
                    + "AND FKTABLE_NAME = ? "
                    + "ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, FK_NAME, KEY_SEQ");
            prep.setString(1, getCatalogPattern(primaryCatalogPattern));
            prep.setString(2, getSchemaPattern(primarySchemaPattern));
            prep.setString(3, primaryTable);
            prep.setString(4, getCatalogPattern(foreignCatalogPattern));
            prep.setString(5, getSchemaPattern(foreignSchemaPattern));
            prep.setString(6, foreignTable);
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of user-defined data types.
     * This call returns an empty result set.
     *
     * <ul>
     * <li>1 TYPE_CAT (String) catalog
     * </li><li>2 TYPE_SCHEM (String) schema
     * </li><li>3 TYPE_NAME (String) type name
     * </li><li>4 CLASS_NAME (String) Java class
     * </li><li>5 DATA_TYPE (short) SQL Type - see also java.sql.Types
     * </li><li>6 REMARKS (String) description
     * </li><li>7 BASE_TYPE (short) base type - see also java.sql.Types
     * </li></ul>
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
            if (isDebugEnabled()) {
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
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of data types. The result set is sorted by DATA_TYPE and
     * afterwards by how closely the data type maps to the corresponding JDBC
     * SQL type (best match first).
     *
     * <ul>
     * <li>1 TYPE_NAME (String) type name </li>
     * <li>2 DATA_TYPE (short) SQL data type - see also java.sql.Types </li>
     * <li>3 PRECISION (int) maximum precision </li>
     * <li>4 LITERAL_PREFIX (String) prefix used to quote a literal </li>
     * <li>5 LITERAL_SUFFIX (String) suffix used to quote a literal </li>
     * <li>6 CREATE_PARAMS (String) parameters used (may be null) </li>
     * <li>7 NULLABLE (short) typeNoNulls (NULL not allowed) or typeNullable
     * </li>
     * <li>8 CASE_SENSITIVE (boolean) case sensitive </li>
     * <li>9 SEARCHABLE (short) typeSearchable </li>
     * <li>10 UNSIGNED_ATTRIBUTE (boolean) unsigned </li>
     * <li>11 FIXED_PREC_SCALE (boolean) fixed precision </li>
     * <li>12 AUTO_INCREMENT (boolean) auto increment </li>
     * <li>13 LOCAL_TYPE_NAME (String) localized version of the data type </li>
     * <li>14 MINIMUM_SCALE (short) minimum scale </li>
     * <li>15 MAXIMUM_SCALE (short) maximum scale </li>
     * <li>16 SQL_DATA_TYPE (int) unused </li>
     * <li>17 SQL_DATETIME_SUB (int) unused </li>
     * <li>18 NUM_PREC_RADIX (int) 2 for binary, 10 for decimal </li>
     * </ul>
     *
     * @return the list of data types
     * @throws SQLException if the connection is closed
     */
    public ResultSet getTypeInfo() throws SQLException {
        try {
            debugCodeCall("getTypeInfo");
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
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
            ResultSet rs = prep.executeQuery();
            return rs;
        } catch (Exception e) {
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
     * as table/column/index name, in addition to the SQL-92 keywords. The list
     * returned is:
     * <pre>
     * LIMIT,MINUS,ROWNUM,SYSDATE,SYSTIME,SYSTIMESTAMP,TODAY
     * </pre>
     * The complete list of keywords (including SQL-92 keywords) is:
     * <pre>
     * CROSS, CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, DISTINCT,
     * EXCEPT, EXISTS, FALSE, FOR, FROM, FULL, GROUP, HAVING, INNER,
     * INTERSECT, IS, JOIN, LIKE, LIMIT, MINUS, NATURAL, NOT, NULL, ON,
     * ORDER, PRIMARY, ROWNUM, SELECT, SYSDATE, SYSTIME, SYSTIMESTAMP,
     * TODAY, TRUE, UNION, WHERE
     * </pre>
     *
     * @return a list of additional the keywords
     */
    public String getSQLKeywords() {
        debugCodeCall("getSQLKeywords");
        return "LIMIT,MINUS,ROWNUM,SYSDATE,SYSTIME,SYSTIMESTAMP,TODAY";
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
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT TOPIC "
                    + "FROM INFORMATION_SCHEMA.HELP WHERE SECTION = ?");
            prep.setString(1, section);
            ResultSet rs = prep.executeQuery();
            StatementBuilder buff = new StatementBuilder();
            while (rs.next()) {
                String s = rs.getString(1).trim();
                String[] array = StringUtils.arraySplit(s, ',', true);
                for (String a : array) {
                    buff.appendExceptFirst(",");
                    String f = a.trim();
                    if (f.indexOf(' ') >= 0) {
                        // remove 'Function' from 'INSERT Function'
                        f = f.substring(0, f.indexOf(' ')).trim();
                    }
                    buff.append(f);
                }
            }
            rs.close();
            prep.close();
            return buff.toString();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the default escape character for DatabaseMetaData search
     * patterns.
     *
     * @return the default escape character (always '\', independent on the
     *         mode)
     */
    public String getSearchStringEscape() {
        debugCodeCall("getSearchStringEscape");
        return "\\";
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
     * @param fromType the source SQL type
     * @param toType the target SQL type
     * @return true
     */
    public boolean supportsConvert(int fromType, int toType) {
        if (isDebugEnabled()) {
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
     * Returns whether referential integrity is supported.
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
     * @return true
     */
    public boolean supportsCatalogsInDataManipulation() {
        debugCodeCall("supportsCatalogsInDataManipulation");
        return true;
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
     * @return true
     */
    public boolean supportsCatalogsInTableDefinitions() {
        debugCodeCall("supportsCatalogsInTableDefinitions");
        return true;
    }

    /**
     * Returns whether the catalog name in CREATE INDEX is supported.
     *
     * @return true
     */
    public boolean supportsCatalogsInIndexDefinitions() {
        debugCodeCall("supportsCatalogsInIndexDefinitions");
        return true;
    }

    /**
     * Returns whether the catalog name in GRANT is supported.
     *
     * @return true
     */
    public boolean supportsCatalogsInPrivilegeDefinitions() {
        debugCodeCall("supportsCatalogsInPrivilegeDefinitions");
        return true;
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
     * Returns whether open result sets across commits are supported.
     *
     * @return false
     */
    public boolean supportsOpenCursorsAcrossCommit() {
        debugCodeCall("supportsOpenCursorsAcrossCommit");
        return false;
    }

    /**
     * Returns whether open result sets across rollback are supported.
     *
     * @return false
     */
    public boolean supportsOpenCursorsAcrossRollback() {
        debugCodeCall("supportsOpenCursorsAcrossRollback");
        return false;
    }

    /**
     * Returns whether open statements across commit are supported.
     *
     * @return true
     */
    public boolean supportsOpenStatementsAcrossCommit() {
        debugCodeCall("supportsOpenStatementsAcrossCommit");
        return true;
    }

    /**
     * Returns whether open statements across rollback are supported.
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
     * @param level the transaction isolation level (Connection.TRANSACTION_*)
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
     * ResultSet.TYPE_SCROLL_SENSITIVE is not supported.
     *
     * @param type the result set type
     * @return true for all types except ResultSet.TYPE_FORWARD_ONLY
     */
    public boolean supportsResultSetType(int type) {
        debugCodeCall("supportsResultSetType", type);
        return type != ResultSet.TYPE_SCROLL_SENSITIVE;
    }

    /**
     * Returns whether a specific result set concurrency is supported.
     * ResultSet.TYPE_SCROLL_SENSITIVE is not supported.
     *
     * @param type the result set type
     * @param concurrency the result set concurrency
     * @return true if the type is not ResultSet.TYPE_SCROLL_SENSITIVE
     */
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        if (isDebugEnabled()) {
            debugCode("supportsResultSetConcurrency("+type+", "+concurrency+");");
        }
        return type != ResultSet.TYPE_SCROLL_SENSITIVE;
    }

    /**
     * Returns whether own updates are visible.
     *
     * @param type the result set type
     * @return true
     */
    public boolean ownUpdatesAreVisible(int type) {
        debugCodeCall("ownUpdatesAreVisible", type);
        return true;
    }

    /**
     * Returns whether own deletes are visible.
     *
     * @param type the result set type
     * @return false
     */
    public boolean ownDeletesAreVisible(int type) {
        debugCodeCall("ownDeletesAreVisible", type);
        return false;
    }

    /**
     * Returns whether own inserts are visible.
     *
     * @param type the result set type
     * @return false
     */
    public boolean ownInsertsAreVisible(int type) {
        debugCodeCall("ownInsertsAreVisible", type);
        return false;
    }

    /**
     * Returns whether other updates are visible.
     *
     * @param type the result set type
     * @return false
     */
    public boolean othersUpdatesAreVisible(int type) {
        debugCodeCall("othersUpdatesAreVisible", type);
        return false;
    }

    /**
     * Returns whether other deletes are visible.
     *
     * @param type the result set type
     * @return false
     */
    public boolean othersDeletesAreVisible(int type) {
        debugCodeCall("othersDeletesAreVisible", type);
        return false;
    }

    /**
     * Returns whether other inserts are visible.
     *
     * @param type the result set type
     * @return false
     */
    public boolean othersInsertsAreVisible(int type) {
        debugCodeCall("othersInsertsAreVisible", type);
        return false;
    }

    /**
     * Returns whether updates are detected.
     *
     * @param type the result set type
     * @return false
     */
    public boolean updatesAreDetected(int type) {
        debugCodeCall("updatesAreDetected", type);
        return false;
    }

    /**
     * Returns whether deletes are detected.
     *
     * @param type the result set type
     * @return false
     */
    public boolean deletesAreDetected(int type) {
        debugCodeCall("deletesAreDetected", type);
        return false;
    }

    /**
     * Returns whether inserts are detected.
     *
     * @param type the result set type
     * @return false
     */
    public boolean insertsAreDetected(int type) {
        debugCodeCall("insertsAreDetected", type);
        return false;
    }

    /**
     * Returns whether batch updates are supported.
     *
     * @return true
     */
    public boolean supportsBatchUpdates() {
        debugCodeCall("supportsBatchUpdates");
        return true;
    }

    /**
     * Returns whether the maximum row size includes blobs.
     *
     * @return false
     */
    public boolean doesMaxRowSizeIncludeBlobs() {
        debugCodeCall("doesMaxRowSizeIncludeBlobs");
        return false;
    }

    /**
     * Returns the default transaction isolation level.
     *
     * @return Connection.TRANSACTION_READ_COMMITTED
     */
    public int getDefaultTransactionIsolation() {
        debugCodeCall("getDefaultTransactionIsolation");
        return Connection.TRANSACTION_READ_COMMITTED;
    }

    /**
     * Checks if for CREATE TABLE Test(ID INT), getTables returns Test as the
     * table name.
     *
     * @return false
     */
    public boolean supportsMixedCaseIdentifiers() {
        debugCodeCall("supportsMixedCaseIdentifiers");
        return false;
    }

    /**
     * Checks if a table created with CREATE TABLE "Test"(ID INT) is a different
     * table than a table created with CREATE TABLE TEST(ID INT).
     *
     * @return true
     */
    public boolean supportsMixedCaseQuotedIdentifiers() {
        debugCodeCall("supportsMixedCaseQuotedIdentifiers");
        return true;
    }

    /**
     * Checks if for CREATE TABLE Test(ID INT), getTables returns TEST as the
     * table name.
     *
     * @return true
     */
    public boolean storesUpperCaseIdentifiers() {
        debugCodeCall("storesUpperCaseIdentifiers");
        return true;
    }

    /**
     * Checks if for CREATE TABLE Test(ID INT), getTables returns test as the
     * table name.
     *
     * @return false
     */
    public boolean storesLowerCaseIdentifiers() {
        debugCodeCall("storesLowerCaseIdentifiers");
        return false;
    }

    /**
     * Checks if for CREATE TABLE Test(ID INT), getTables returns Test as the
     * table name.
     *
     * @return false
     */
    public boolean storesMixedCaseIdentifiers() {
        debugCodeCall("storesMixedCaseIdentifiers");
        return false;
    }

    /**
     * Checks if for CREATE TABLE "Test"(ID INT), getTables returns TEST as the
     * table name.
     *
     * @return false
     */
    public boolean storesUpperCaseQuotedIdentifiers() {
        debugCodeCall("storesUpperCaseQuotedIdentifiers");
        return false;
    }

    /**
     * Checks if for CREATE TABLE "Test"(ID INT), getTables returns test as the
     * table name.
     *
     * @return false
     */
    public boolean storesLowerCaseQuotedIdentifiers() {
        debugCodeCall("storesLowerCaseQuotedIdentifiers");
        return false;
    }

    /**
     * Checks if for CREATE TABLE "Test"(ID INT), getTables returns Test as the
     * table name.
     *
     * @return true
     */
    public boolean storesMixedCaseQuotedIdentifiers() {
        debugCodeCall("storesMixedCaseQuotedIdentifiers");
        return true;
    }

    /**
     * Returns the maximum length for hex values (characters).
     *
     * @return 0 for limit is unknown
     */
    public int getMaxBinaryLiteralLength() {
        debugCodeCall("getMaxBinaryLiteralLength");
        return 0;
    }

    /**
     * Returns the maximum length for literals.
     *
     * @return 0 for limit is unknown
     */
    public int getMaxCharLiteralLength() {
        debugCodeCall("getMaxCharLiteralLength");
        return 0;
    }

    /**
     * Returns the maximum length for column names.
     *
     * @return 0 for limit is unknown
     */
    public int getMaxColumnNameLength() {
        debugCodeCall("getMaxColumnNameLength");
        return 0;
    }

    /**
     * Returns the maximum number of columns in GROUP BY.
     *
     * @return 0 for limit is unknown
     */
    public int getMaxColumnsInGroupBy() {
        debugCodeCall("getMaxColumnsInGroupBy");
        return 0;
    }

    /**
     * Returns the maximum number of columns in CREATE INDEX.
     *
     * @return 0 for limit is unknown
     */
    public int getMaxColumnsInIndex() {
        debugCodeCall("getMaxColumnsInIndex");
        return 0;
    }

    /**
     * Returns the maximum number of columns in ORDER BY.
     *
     * @return 0 for limit is unknown
     */
    public int getMaxColumnsInOrderBy() {
        debugCodeCall("getMaxColumnsInOrderBy");
        return 0;
    }

    /**
     * Returns the maximum number of columns in SELECT.
     *
     * @return 0 for limit is unknown
     */
    public int getMaxColumnsInSelect() {
        debugCodeCall("getMaxColumnsInSelect");
        return 0;
    }

    /**
     * Returns the maximum number of columns in CREATE TABLE.
     *
     * @return 0 for limit is unknown
     */
    public int getMaxColumnsInTable() {
        debugCodeCall("getMaxColumnsInTable");
        return 0;
    }

    /**
     * Returns the maximum number of open connection.
     *
     * @return 0 for limit is unknown
     */
    public int getMaxConnections() {
        debugCodeCall("getMaxConnections");
        return 0;
    }

    /**
     * Returns the maximum length for a cursor name.
     *
     * @return 0 for limit is unknown
     */
    public int getMaxCursorNameLength() {
        debugCodeCall("getMaxCursorNameLength");
        return 0;
    }

    /**
     * Returns the maximum length for an index (in bytes).
     *
     * @return 0 for limit is unknown
     */
    public int getMaxIndexLength() {
        debugCodeCall("getMaxIndexLength");
        return 0;
    }

    /**
     * Returns the maximum length for a schema name.
     *
     * @return 0 for limit is unknown
     */
    public int getMaxSchemaNameLength() {
        debugCodeCall("getMaxSchemaNameLength");
        return 0;
    }

    /**
     * Returns the maximum length for a procedure name.
     *
     * @return 0 for limit is unknown
     */
    public int getMaxProcedureNameLength() {
        debugCodeCall("getMaxProcedureNameLength");
        return 0;
    }

    /**
     * Returns the maximum length for a catalog name.
     *
     * @return 0 for limit is unknown
     */
    public int getMaxCatalogNameLength() {
        debugCodeCall("getMaxCatalogNameLength");
        return 0;
    }

    /**
     * Returns the maximum size of a row (in bytes).
     *
     * @return 0 for limit is unknown
     */
    public int getMaxRowSize() {
        debugCodeCall("getMaxRowSize");
        return 0;
    }

    /**
     * Returns the maximum length of a statement.
     *
     * @return 0 for limit is unknown
     */
    public int getMaxStatementLength() {
        debugCodeCall("getMaxStatementLength");
        return 0;
    }

    /**
     * Returns the maximum number of open statements.
     *
     * @return 0 for limit is unknown
     */
    public int getMaxStatements() {
        debugCodeCall("getMaxStatements");
        return 0;
    }

    /**
     * Returns the maximum length for a table name.
     *
     * @return 0 for limit is unknown
     */
    public int getMaxTableNameLength() {
        debugCodeCall("getMaxTableNameLength");
        return 0;
    }

    /**
     * Returns the maximum number of tables in a SELECT.
     *
     * @return 0 for limit is unknown
     */
    public int getMaxTablesInSelect() {
        debugCodeCall("getMaxTablesInSelect");
        return 0;
    }

    /**
     * Returns the maximum length for a user name.
     *
     * @return 0 for limit is unknown
     */
    public int getMaxUserNameLength() {
        debugCodeCall("getMaxUserNameLength");
        return 0;
    }

    /**
     * Does the database support savepoints.
     *
     * @return true
     */
    public boolean supportsSavepoints() {
        debugCodeCall("supportsSavepoints");
        return true;
    }

    /**
     * Does the database support named parameters.
     *
     * @return false
     */
    public boolean supportsNamedParameters() {
        debugCodeCall("supportsNamedParameters");
        return false;
    }

    /**
     * Does the database support multiple open result sets.
     *
     * @return true
     */
    public boolean supportsMultipleOpenResults() {
        debugCodeCall("supportsMultipleOpenResults");
        return true;
    }

    /**
     * Does the database support getGeneratedKeys.
     *
     * @return true
     */
    public boolean supportsGetGeneratedKeys() {
        debugCodeCall("supportsGetGeneratedKeys");
        return true;
    }

    /**
     * [Not supported]
     */
    public ResultSet getSuperTypes(String catalog, String schemaPattern,
            String typeNamePattern) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getSuperTypes("
                        +quote(catalog)+", "
                        +quote(schemaPattern)+", "
                        +quote(typeNamePattern)+");");
            }
            throw Message.getUnsupportedException("superTypes");
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Get the list of super tables of a table. This method currently returns an
     * empty result set.
     * <ul>
     * <li>1 TABLE_CAT (String) table catalog</li>
     * <li>2 TABLE_SCHEM (String) table schema</li>
     * <li>3 TABLE_NAME (String) table name</li>
     * <li>4 SUPERTABLE_NAME (String) the name of the super table</li>
     * </ul>
     *
     * @param catalog null (to get all objects) or the catalog name
     * @param schemaPattern null (to get all objects) or a schema name (uppercase for
     *            unquoted names)
     * @param tableNamePattern null (to get all objects) or a table name pattern
     *            (uppercase for unquoted names)
     * @return an empty result set
     */
    public ResultSet getSuperTables(String catalog, String schemaPattern,
            String tableNamePattern) throws SQLException {
        try {
            if (isDebugEnabled()) {
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
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */
    public ResultSet getAttributes(String catalog, String schemaPattern,
            String typeNamePattern, String attributeNamePattern)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getAttributes("
                        +quote(catalog)+", "
                        +quote(schemaPattern)+", "
                        +quote(typeNamePattern)+", "
                        +quote(attributeNamePattern)+");");
            }
            throw Message.getUnsupportedException("attributes");
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Does this database supports a result set holdability.
     *
     * @param holdability ResultSet.HOLD_CURSORS_OVER_COMMIT or
     *            CLOSE_CURSORS_AT_COMMIT
     * @return true if the holdability is ResultSet.CLOSE_CURSORS_AT_COMMIT
     */
    //## Java 1.4 begin ##
    public boolean supportsResultSetHoldability(int holdability) {
        debugCodeCall("supportsResultSetHoldability", holdability);
        return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }
    //## Java 1.4 end ##

    /**
     * Gets the result set holdability.
     *
     * @return ResultSet.CLOSE_CURSORS_AT_COMMIT
     */
    //## Java 1.4 begin ##
    public int getResultSetHoldability() {
        debugCodeCall("getResultSetHoldability");
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }
    //## Java 1.4 end ##

    /**
     * Gets the major version of the database.
     *
     * @return the major version
     */
    public int getDatabaseMajorVersion() {
        debugCodeCall("getDatabaseMajorVersion");
        return Constants.VERSION_MAJOR;
    }

    /**
     * Gets the minor version of the database.
     *
     * @return the minor version
     */
    public int getDatabaseMinorVersion() {
        debugCodeCall("getDatabaseMinorVersion");
        return Constants.VERSION_MINOR;
    }

    /**
     * Gets the major version of the supported JDBC API.
     *
     * @return the major version
     */
    public int getJDBCMajorVersion() {
        debugCodeCall("getJDBCMajorVersion");
        return Constants.VERSION_JDBC_MAJOR;
    }

    /**
     * Gets the minor version of the supported JDBC API.
     *
     * @return the minor version
     */
    public int getJDBCMinorVersion() {
        debugCodeCall("getJDBCMinorVersion");
        return Constants.VERSION_JDBC_MINOR;
    }

    /**
     * Gets the SQL State type.
     *
     * @return DatabaseMetaData.sqlStateSQL99
     */
//## Java 1.4 begin ##
    public int getSQLStateType() {
        debugCodeCall("getSQLStateType");
        return DatabaseMetaData.sqlStateSQL99;
    }
//## Java 1.4 end ##

    /**
     * Does the database make a copy before updating.
     *
     * @return false
     */
    public boolean locatorsUpdateCopy() {
        debugCodeCall("locatorsUpdateCopy");
        return false;
    }

    /**
     * Does the database support statement pooling.
     *
     * @return false
     */
    public boolean supportsStatementPooling() {
        debugCodeCall("supportsStatementPooling");
        return false;
    }

    // =============================================================

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
        return catalogPattern == null || catalogPattern.length() == 0 ? "%" : catalogPattern;
    }

    /**
     * Get the lifetime of a rowid.
     *
     * @return ROWID_UNSUPPORTED
     */
/*## Java 1.6 begin ##
    public RowIdLifetime getRowIdLifetime() {
        debugCodeCall("getRowIdLifetime");
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }
## Java 1.6 end ##*/

    /**
     * [Not supported] Gets the list of schemas.
     */
/*## Java 1.6 begin ##
    public ResultSet getSchemas(String catalog, String schemaPattern)
            throws SQLException {
        debugCodeCall("getSchemas");
        throw Message.getUnsupportedException("getSchemas(., .)");
    }
## Java 1.6 end ##*/

    /**
     * Returns whether the database supports calling functions using the call syntax.
     *
     * @return true
     */
    public boolean supportsStoredFunctionsUsingCallSyntax() {
        debugCodeCall("supportsStoredFunctionsUsingCallSyntax");
        return true;
    }

    /**
     * Returns whether an exception while auto commit is on closes all result sets.
     *
     * @return false
     */
    public boolean autoCommitFailureClosesAllResultSets() {
        debugCodeCall("autoCommitFailureClosesAllResultSets");
        return false;
    }

    /**
     * [Not supported] Returns the client info properties.
     */
    public ResultSet getClientInfoProperties() throws SQLException {
        debugCodeCall("getClientInfoProperties");
        throw Message.getUnsupportedException("clientInfoProperties");
    }

    /**
     * [Not supported] Return an object of this class if possible.
     */
/*## Java 1.6 begin ##
    public <T> T unwrap(Class<T> iface) throws SQLException {
        debugCodeCall("unwrap");
        throw Message.getUnsupportedException("unwrap");
    }
## Java 1.6 end ##*/

    /**
     * [Not supported] Checks if unwrap can return an object of this class.
     */
/*## Java 1.6 begin ##
    public boolean isWrapperFor(Class< ? > iface) throws SQLException {
        debugCodeCall("isWrapperFor");
        throw Message.getUnsupportedException("isWrapperFor");
    }
## Java 1.6 end ##*/

    /**
     * [Not supported] Gets the list of function columns.
     */
/*## Java 1.6 begin ##
    public ResultSet getFunctionColumns(String catalog, String schemaPattern,
            String functionNamePattern, String columnNamePattern)
            throws SQLException {
        debugCodeCall("getFunctionColumns");
        throw Message.getUnsupportedException("getFunctionColumns");
    }
## Java 1.6 end ##*/

    /**
     * [Not supported] Gets the list of functions.
     */
/*## Java 1.6 begin ##
    public ResultSet getFunctions(String catalog, String schemaPattern,
            String functionNamePattern) throws SQLException {
        debugCodeCall("getFunctions");
        throw Message.getUnsupportedException("getFunctions");
    }
## Java 1.6 end ##*/

    /**
     * INTERNAL
     */
    public String toString() {
        return getTraceObjectName() + ": " + conn;
    }

}
