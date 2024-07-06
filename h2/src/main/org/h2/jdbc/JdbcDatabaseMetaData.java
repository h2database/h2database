/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.Map.Entry;
import java.util.Properties;

import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.jdbc.meta.DatabaseMeta;
import org.h2.jdbc.meta.DatabaseMetaLegacy;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.message.TraceObject;
import org.h2.mode.DefaultNullOrdering;
import org.h2.result.ResultInterface;
import org.h2.result.SimpleResult;
import org.h2.value.TypeInfo;
import org.h2.value.ValueInteger;
import org.h2.value.ValueVarchar;

/**
 * Represents the meta data for a database.
 */
public final class JdbcDatabaseMetaData extends TraceObject implements DatabaseMetaData {

    private final JdbcConnection conn;

    private final DatabaseMeta meta;

    JdbcDatabaseMetaData(JdbcConnection conn, Trace trace, int id) {
        setTrace(trace, TraceObject.DATABASE_META_DATA, id);
        this.conn = conn;
        Session session = conn.getSession();
        meta = session.isOldInformationSchema() ? new DatabaseMetaLegacy(session)
                : conn.getSession().getDatabaseMeta();
    }

    /**
     * Returns the major version of this driver.
     *
     * @return the major version number
     */
    @Override
    public int getDriverMajorVersion() {
        debugCodeCall("getDriverMajorVersion");
        return Constants.VERSION_MAJOR;
    }

    /**
     * Returns the minor version of this driver.
     *
     * @return the minor version number
     */
    @Override
    public int getDriverMinorVersion() {
        debugCodeCall("getDriverMinorVersion");
        return Constants.VERSION_MINOR;
    }

    /**
     * Gets the database product name.
     *
     * @return the product name ("H2")
     */
    @Override
    public String getDatabaseProductName() {
        debugCodeCall("getDatabaseProductName");
        // This value must stay like that, see
        // https://hibernate.atlassian.net/browse/HHH-2682
        return "H2";
    }

    /**
     * Gets the product version of the database.
     *
     * @return the product version
     */
    @Override
    public String getDatabaseProductVersion() throws SQLException {
        try {
            debugCodeCall("getDatabaseProductVersion");
            return meta.getDatabaseProductVersion();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the name of the JDBC driver.
     *
     * @return the driver name ("H2 JDBC Driver")
     */
    @Override
    public String getDriverName() {
        debugCodeCall("getDriverName");
        return "H2 JDBC Driver";
    }

    /**
     * Gets the version number of the driver. The format is
     * [MajorVersion].[MinorVersion].
     *
     * @return the version number
     */
    @Override
    public String getDriverVersion() {
        debugCodeCall("getDriverVersion");
        return Constants.FULL_VERSION;
    }

    /**
     * Gets the list of tables in the database. The result set is sorted by
     * TABLE_TYPE, TABLE_SCHEM, and TABLE_NAME.
     *
     * <ol>
     * <li>TABLE_CAT (String) table catalog</li>
     * <li>TABLE_SCHEM (String) table schema</li>
     * <li>TABLE_NAME (String) table name</li>
     * <li>TABLE_TYPE (String) table type</li>
     * <li>REMARKS (String) comment</li>
     * <li>TYPE_CAT (String) always null</li>
     * <li>TYPE_SCHEM (String) always null</li>
     * <li>TYPE_NAME (String) always null</li>
     * <li>SELF_REFERENCING_COL_NAME (String) always null</li>
     * <li>REF_GENERATION (String) always null</li>
     * <li>SQL (String) the create table statement or NULL for systems tables.</li>
     * </ol>
     *
     * @param catalog null (to get all objects) or the catalog name
     * @param schemaPattern null (to get all objects) or a schema name
     *            (uppercase for unquoted names)
     * @param tableNamePattern null (to get all objects) or a table name
     *            (uppercase for unquoted names)
     * @param types null or a list of table types
     * @return the list of columns
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getTables(" + quote(catalog) + ", " + quote(schemaPattern) + ", " + quote(tableNamePattern)
                        + ", " + quoteArray(types) + ')');
            }
            return getResultSet(meta.getTables(catalog, schemaPattern, tableNamePattern, types));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of columns. The result set is sorted by TABLE_SCHEM,
     * TABLE_NAME, and ORDINAL_POSITION.
     *
     * <ol>
     * <li>TABLE_CAT (String) table catalog</li>
     * <li>TABLE_SCHEM (String) table schema</li>
     * <li>TABLE_NAME (String) table name</li>
     * <li>COLUMN_NAME (String) column name</li>
     * <li>DATA_TYPE (int) data type (see java.sql.Types)</li>
     * <li>TYPE_NAME (String) data type name ("INTEGER", "VARCHAR",...)</li>
     * <li>COLUMN_SIZE (int) precision
     *         (values larger than 2 GB are returned as 2 GB)</li>
     * <li>BUFFER_LENGTH (int) unused</li>
     * <li>DECIMAL_DIGITS (int) scale (0 for INTEGER and VARCHAR)</li>
     * <li>NUM_PREC_RADIX (int) radix</li>
     * <li>NULLABLE (int) columnNoNulls or columnNullable</li>
     * <li>REMARKS (String) comment</li>
     * <li>COLUMN_DEF (String) default value</li>
     * <li>SQL_DATA_TYPE (int) unused</li>
     * <li>SQL_DATETIME_SUB (int) unused</li>
     * <li>CHAR_OCTET_LENGTH (int) unused</li>
     * <li>ORDINAL_POSITION (int) the column index (1,2,...)</li>
     * <li>IS_NULLABLE (String) "NO" or "YES"</li>
     * <li>SCOPE_CATALOG (String) always null</li>
     * <li>SCOPE_SCHEMA (String) always null</li>
     * <li>SCOPE_TABLE (String) always null</li>
     * <li>SOURCE_DATA_TYPE (short) null</li>
     * <li>IS_AUTOINCREMENT (String) "NO" or "YES"</li>
     * <li>IS_GENERATEDCOLUMN (String) "NO" or "YES"</li>
     * </ol>
     *
     * @param catalog null (to get all objects) or the catalog name
     * @param schemaPattern null (to get all objects) or a schema name
     *            (uppercase for unquoted names)
     * @param tableNamePattern null (to get all objects) or a table name
     *            (uppercase for unquoted names)
     * @param columnNamePattern null (to get all objects) or a column name
     *            (uppercase for unquoted names)
     * @return the list of columns
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
            String columnNamePattern) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getColumns(" + quote(catalog)+", "
                        +quote(schemaPattern)+", "
                        +quote(tableNamePattern)+", "
                        +quote(columnNamePattern)+')');
            }
            return getResultSet(meta.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern));
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
     * <ol>
     * <li>TABLE_CAT (String) table catalog</li>
     * <li>TABLE_SCHEM (String) table schema</li>
     * <li>TABLE_NAME (String) table name</li>
     * <li>NON_UNIQUE (boolean) 'true' if non-unique</li>
     * <li>INDEX_QUALIFIER (String) index catalog</li>
     * <li>INDEX_NAME (String) index name</li>
     * <li>TYPE (short) the index type (tableIndexOther or tableIndexHash for
     * unique indexes on non-nullable columns, tableIndexStatistics for other
     * indexes)</li>
     * <li>ORDINAL_POSITION (short) column index (1, 2, ...)</li>
     * <li>COLUMN_NAME (String) column name</li>
     * <li>ASC_OR_DESC (String) ascending or descending (always 'A')</li>
     * <li>CARDINALITY (long) number of rows or numbers of unique values for
     * unique indexes on non-nullable columns</li>
     * <li>PAGES (long) number of pages use</li>
     * <li>FILTER_CONDITION (String) filter condition (always empty)</li>
     * </ol>
     *
     * @param catalog null or the catalog name
     * @param schema null (to get all objects) or a schema name
     *            (uppercase for unquoted names)
     * @param table table name (must be specified)
     * @param unique only unique indexes
     * @param approximate if true, return fast, but approximate CARDINALITY and PAGES
     * @return the list of indexes and columns
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getIndexInfo(" + quote(catalog) + ", " + quote(schema) + ", " + quote(table) + ", " + unique
                        + ", " + approximate + ')');
            }
            return getResultSet(meta.getIndexInfo(catalog, schema, table, unique, approximate));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the primary key columns for a table. The result set is sorted by
     * TABLE_SCHEM, and COLUMN_NAME (and not by KEY_SEQ).
     *
     * <ol>
     * <li>TABLE_CAT (String) table catalog</li>
     * <li>TABLE_SCHEM (String) table schema</li>
     * <li>TABLE_NAME (String) table name</li>
     * <li>COLUMN_NAME (String) column name</li>
     * <li>KEY_SEQ (short) the column index of this column (1,2,...)</li>
     * <li>PK_NAME (String) the name of the primary key index</li>
     * </ol>
     *
     * @param catalog null or the catalog name
     * @param schema null (to get all objects) or a schema name
     *            (uppercase for unquoted names)
     * @param table table name (must be specified)
     * @return the list of primary key columns
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getPrimaryKeys(" + quote(catalog) + ", " + quote(schema) + ", " + quote(table) + ')');
            }
            return getResultSet(meta.getPrimaryKeys(catalog, schema, table));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if all procedures callable.
     *
     * @return true
     */
    @Override
    public boolean allProceduresAreCallable() {
        debugCodeCall("allProceduresAreCallable");
        return true;
    }

    /**
     * Checks if it possible to query all tables returned by getTables.
     *
     * @return true
     */
    @Override
    public boolean allTablesAreSelectable() {
        debugCodeCall("allTablesAreSelectable");
        return true;
    }

    /**
     * Returns the database URL for this connection.
     *
     * @return the url
     */
    @Override
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
    @Override
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
    @Override
    public boolean isReadOnly() throws SQLException {
        try {
            debugCodeCall("isReadOnly");
            return conn.isReadOnly();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if NULL values are sorted high (bigger than anything that is not
     * null).
     *
     * @return if NULL values are sorted high
     */
    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        try {
            debugCodeCall("nullsAreSortedHigh");
            return meta.defaultNullOrdering() == DefaultNullOrdering.HIGH;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if NULL values are sorted low (smaller than anything that is not
     * null).
     *
     * @return if NULL values are sorted low
     */
    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        try {
            debugCodeCall("nullsAreSortedLow");
            return meta.defaultNullOrdering() == DefaultNullOrdering.LOW;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if NULL values are sorted at the beginning (no matter if ASC or
     * DESC is used).
     *
     * @return if NULL values are sorted at the beginning
     */
    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        try {
            debugCodeCall("nullsAreSortedAtStart");
            return meta.defaultNullOrdering() == DefaultNullOrdering.FIRST;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if NULL values are sorted at the end (no matter if ASC or DESC is
     * used).
     *
     * @return if NULL values are sorted at the end
     */
    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        try {
            debugCodeCall("nullsAreSortedAtEnd");
            return meta.defaultNullOrdering() == DefaultNullOrdering.LAST;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the connection that created this object.
     *
     * @return the connection
     */
    @Override
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
     * <ol>
     * <li>PROCEDURE_CAT (String) catalog</li>
     * <li>PROCEDURE_SCHEM (String) schema</li>
     * <li>PROCEDURE_NAME (String) name</li>
     * <li>reserved</li>
     * <li>reserved</li>
     * <li>reserved</li>
     * <li>REMARKS (String) description</li>
     * <li>PROCEDURE_TYPE (short) if this procedure returns a result
     * (procedureNoResult or procedureReturnsResult)</li>
     * <li>SPECIFIC_NAME (String) non-ambiguous name to distinguish
     * overloads</li>
     * </ol>
     *
     * @param catalog null or the catalog name
     * @param schemaPattern null (to get all objects) or a schema name
     *            (uppercase for unquoted names)
     * @param procedureNamePattern the procedure name pattern
     * @return the procedures
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern,
            String procedureNamePattern) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getProcedures("
                        +quote(catalog)+", "
                        +quote(schemaPattern)+", "
                        +quote(procedureNamePattern)+')');
            }
            return getResultSet(meta.getProcedures(catalog, schemaPattern, procedureNamePattern));
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
     * <ol>
     * <li>PROCEDURE_CAT (String) catalog</li>
     * <li>PROCEDURE_SCHEM (String) schema</li>
     * <li>PROCEDURE_NAME (String) name</li>
     * <li>COLUMN_NAME (String) column name</li>
     * <li>COLUMN_TYPE (short) column type
     * (always DatabaseMetaData.procedureColumnIn)</li>
     * <li>DATA_TYPE (short) sql type</li>
     * <li>TYPE_NAME (String) type name</li>
     * <li>PRECISION (int) precision</li>
     * <li>LENGTH (int) length</li>
     * <li>SCALE (short) scale</li>
     * <li>RADIX (int)</li>
     * <li>NULLABLE (short) nullable
     * (DatabaseMetaData.columnNoNulls for primitive data types,
     * DatabaseMetaData.columnNullable otherwise)</li>
     * <li>REMARKS (String) description</li>
     * <li>COLUMN_DEF (String) always null</li>
     * <li>SQL_DATA_TYPE (int) for future use</li>
     * <li>SQL_DATETIME_SUB (int) for future use</li>
     * <li>CHAR_OCTET_LENGTH (int)</li>
     * <li>ORDINAL_POSITION (int) the parameter index
     * starting from 1 (0 is the return value)</li>
     * <li>IS_NULLABLE (String) always "YES"</li>
     * <li>SPECIFIC_NAME (String) non-ambiguous procedure name to distinguish
     * overloads</li>
     * </ol>
     *
     * @param catalog null or the catalog name
     * @param schemaPattern null (to get all objects) or a schema name
     *            (uppercase for unquoted names)
     * @param procedureNamePattern the procedure name pattern
     * @param columnNamePattern the procedure name pattern
     * @return the procedure columns
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
            String columnNamePattern) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getProcedureColumns(" + quote(catalog) + ", " + quote(schemaPattern) + ", "
                        + quote(procedureNamePattern) + ", " + quote(columnNamePattern) + ')');
            }
            checkClosed();
            return getResultSet(
                    meta.getProcedureColumns(catalog, schemaPattern, procedureNamePattern, columnNamePattern));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of schemas.
     * The result set is sorted by TABLE_SCHEM.
     *
     * <ol>
     * <li>TABLE_SCHEM (String) schema name</li>
     * <li>TABLE_CATALOG (String) catalog name</li>
     * </ol>
     *
     * @return the schema list
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getSchemas() throws SQLException {
        try {
            debugCodeCall("getSchemas");
            return getResultSet(meta.getSchemas());
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of catalogs.
     * The result set is sorted by TABLE_CAT.
     *
     * <ol>
     * <li>TABLE_CAT (String) catalog name</li>
     * </ol>
     *
     * @return the catalog list
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getCatalogs() throws SQLException {
        try {
            debugCodeCall("getCatalogs");
            return getResultSet(meta.getCatalogs());
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of table types. This call returns a result set with five
     * records: "SYSTEM TABLE", "TABLE", "VIEW", "TABLE LINK" and "EXTERNAL".
     * <ol>
     * <li>TABLE_TYPE (String) table type</li>
     * </ol>
     *
     * @return the table types
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getTableTypes() throws SQLException {
        try {
            debugCodeCall("getTableTypes");
            return getResultSet(meta.getTableTypes());
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of column privileges. The result set is sorted by
     * COLUMN_NAME and PRIVILEGE
     *
     * <ol>
     * <li>TABLE_CAT (String) table catalog</li>
     * <li>TABLE_SCHEM (String) table schema</li>
     * <li>TABLE_NAME (String) table name</li>
     * <li>COLUMN_NAME (String) column name</li>
     * <li>GRANTOR (String) grantor of access</li>
     * <li>GRANTEE (String) grantee of access</li>
     * <li>PRIVILEGE (String) SELECT, INSERT, UPDATE, DELETE or REFERENCES
     * (only one per row)</li>
     * <li>IS_GRANTABLE (String) YES means the grantee can grant access to
     * others</li>
     * </ol>
     *
     * @param catalog null (to get all objects) or the catalog name
     * @param schema null (to get all objects) or a schema name
     *            (uppercase for unquoted names)
     * @param table a table name (uppercase for unquoted names)
     * @param columnNamePattern null (to get all objects) or a column name
     *            (uppercase for unquoted names)
     * @return the list of privileges
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getColumnPrivileges(" + quote(catalog) + ", " + quote(schema) + ", " + quote(table) + ", "
                        + quote(columnNamePattern) + ')');
            }
            return getResultSet(meta.getColumnPrivileges(catalog, schema, table, columnNamePattern));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of table privileges. The result set is sorted by
     * TABLE_SCHEM, TABLE_NAME, and PRIVILEGE.
     *
     * <ol>
     * <li>TABLE_CAT (String) table catalog</li>
     * <li>TABLE_SCHEM (String) table schema</li>
     * <li>TABLE_NAME (String) table name</li>
     * <li>GRANTOR (String) grantor of access</li>
     * <li>GRANTEE (String) grantee of access</li>
     * <li>PRIVILEGE (String) SELECT, INSERT, UPDATE, DELETE or REFERENCES
     * (only one per row)</li>
     * <li>IS_GRANTABLE (String) YES means the grantee can grant access to
     * others</li>
     * </ol>
     *
     * @param catalog null (to get all objects) or the catalog name
     * @param schemaPattern null (to get all objects) or a schema name
     *            (uppercase for unquoted names)
     * @param tableNamePattern null (to get all objects) or a table name
     *            (uppercase for unquoted names)
     * @return the list of privileges
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getTablePrivileges(" + quote(catalog) + ", " + quote(schemaPattern) + ", "
                        + quote(tableNamePattern) + ')');
            }
            checkClosed();
            return getResultSet(meta.getTablePrivileges(catalog, schemaPattern, tableNamePattern));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of columns that best identifier a row in a table.
     * The list is ordered by SCOPE.
     *
     * <ol>
     * <li>SCOPE (short) scope of result (always bestRowSession)</li>
     * <li>COLUMN_NAME (String) column name</li>
     * <li>DATA_TYPE (short) SQL data type, see also java.sql.Types</li>
     * <li>TYPE_NAME (String) type name</li>
     * <li>COLUMN_SIZE (int) precision
     *         (values larger than 2 GB are returned as 2 GB)</li>
     * <li>BUFFER_LENGTH (int) unused</li>
     * <li>DECIMAL_DIGITS (short) scale</li>
     * <li>PSEUDO_COLUMN (short) (always bestRowNotPseudo)</li>
     * </ol>
     *
     * @param catalog null (to get all objects) or the catalog name
     * @param schema null (to get all objects) or a schema name
     *            (uppercase for unquoted names)
     * @param table table name (must be specified)
     * @param scope ignored
     * @param nullable ignored
     * @return the primary key index
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getBestRowIdentifier(" + quote(catalog) + ", " + quote(schema) + ", " + quote(table) + ", "
                        + scope + ", " + nullable + ')');
            }
            return getResultSet(meta.getBestRowIdentifier(catalog, schema, table, scope, nullable));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Get the list of columns that are update when any value is updated.
     * The result set is always empty.
     *
     * <ol>
     * <li>1 SCOPE (int) not used</li>
     * <li>2 COLUMN_NAME (String) column name</li>
     * <li>3 DATA_TYPE (int) SQL data type - see also java.sql.Types</li>
     * <li>4 TYPE_NAME (String) data type name</li>
     * <li>5 COLUMN_SIZE (int) precision
     *         (values larger than 2 GB are returned as 2 GB)</li>
     * <li>6 BUFFER_LENGTH (int) length (bytes)</li>
     * <li>7 DECIMAL_DIGITS (int) scale</li>
     * <li>8 PSEUDO_COLUMN (int) is this column a pseudo column</li>
     * </ol>
     *
     * @param catalog null (to get all objects) or the catalog name
     * @param schema null (to get all objects) or a schema name
     * @param table table name (must be specified)
     * @return an empty result set
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getVersionColumns(" + quote(catalog) + ", " + quote(schema) + ", " + quote(table) + ')');
            }
            return getResultSet(meta.getVersionColumns(catalog, schema, table));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of primary key columns that are referenced by a table. The
     * result set is sorted by PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME,
     * FK_NAME, KEY_SEQ.
     *
     * <ol>
     * <li>PKTABLE_CAT (String) primary catalog</li>
     * <li>PKTABLE_SCHEM (String) primary schema</li>
     * <li>PKTABLE_NAME (String) primary table</li>
     * <li>PKCOLUMN_NAME (String) primary column</li>
     * <li>FKTABLE_CAT (String) foreign catalog</li>
     * <li>FKTABLE_SCHEM (String) foreign schema</li>
     * <li>FKTABLE_NAME (String) foreign table</li>
     * <li>FKCOLUMN_NAME (String) foreign column</li>
     * <li>KEY_SEQ (short) sequence number (1, 2, ...)</li>
     * <li>UPDATE_RULE (short) action on update (see
     * DatabaseMetaData.importedKey...)</li>
     * <li>DELETE_RULE (short) action on delete (see
     * DatabaseMetaData.importedKey...)</li>
     * <li>FK_NAME (String) foreign key name</li>
     * <li>PK_NAME (String) primary key name</li>
     * <li>DEFERRABILITY (short) deferrable or not (always
     * importedKeyNotDeferrable)</li>
     * </ol>
     *
     * @param catalog null (to get all objects) or the catalog name
     * @param schema the schema name of the foreign table
     * @param table the name of the foreign table
     * @return the result set
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getImportedKeys(" + quote(catalog) + ", " + quote(schema) + ", " + quote(table) + ')');
            }
            return getResultSet(meta.getImportedKeys(catalog, schema, table));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of foreign key columns that reference a table. The result
     * set is sorted by FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, FK_NAME,
     * KEY_SEQ.
     *
     * <ol>
     * <li>PKTABLE_CAT (String) primary catalog</li>
     * <li>PKTABLE_SCHEM (String) primary schema</li>
     * <li>PKTABLE_NAME (String) primary table</li>
     * <li>PKCOLUMN_NAME (String) primary column</li>
     * <li>FKTABLE_CAT (String) foreign catalog</li>
     * <li>FKTABLE_SCHEM (String) foreign schema</li>
     * <li>FKTABLE_NAME (String) foreign table</li>
     * <li>FKCOLUMN_NAME (String) foreign column</li>
     * <li>KEY_SEQ (short) sequence number (1,2,...)</li>
     * <li>UPDATE_RULE (short) action on update (see
     * DatabaseMetaData.importedKey...)</li>
     * <li>DELETE_RULE (short) action on delete (see
     * DatabaseMetaData.importedKey...)</li>
     * <li>FK_NAME (String) foreign key name</li>
     * <li>PK_NAME (String) primary key name</li>
     * <li>DEFERRABILITY (short) deferrable or not (always
     * importedKeyNotDeferrable)</li>
     * </ol>
     *
     * @param catalog null or the catalog name
     * @param schema the schema name of the primary table
     * @param table the name of the primary table
     * @return the result set
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getExportedKeys(" + quote(catalog) + ", " + quote(schema) + ", " + quote(table) + ')');
            }
            return getResultSet(meta.getExportedKeys(catalog, schema, table));
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
     * <ol>
     * <li>PKTABLE_CAT (String) primary catalog</li>
     * <li>PKTABLE_SCHEM (String) primary schema</li>
     * <li>PKTABLE_NAME (String) primary table</li>
     * <li>PKCOLUMN_NAME (String) primary column</li>
     * <li>FKTABLE_CAT (String) foreign catalog</li>
     * <li>FKTABLE_SCHEM (String) foreign schema</li>
     * <li>FKTABLE_NAME (String) foreign table</li>
     * <li>FKCOLUMN_NAME (String) foreign column</li>
     * <li>KEY_SEQ (short) sequence number (1,2,...)</li>
     * <li>UPDATE_RULE (short) action on update (see
     * DatabaseMetaData.importedKey...)</li>
     * <li>DELETE_RULE (short) action on delete (see
     * DatabaseMetaData.importedKey...)</li>
     * <li>FK_NAME (String) foreign key name</li>
     * <li>PK_NAME (String) primary key name</li>
     * <li>DEFERRABILITY (short) deferrable or not (always
     * importedKeyNotDeferrable)</li>
     * </ol>
     *
     * @param primaryCatalog null or the catalog name
     * @param primarySchema the schema name of the primary table
     *          (optional)
     * @param primaryTable the name of the primary table (must be specified)
     * @param foreignCatalog null or the catalog name
     * @param foreignSchema the schema name of the foreign table
     *          (optional)
     * @param foreignTable the name of the foreign table (must be specified)
     * @return the result set
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getCrossReference(String primaryCatalog, String primarySchema, String primaryTable,
            String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getCrossReference(" + quote(primaryCatalog) + ", " + quote(primarySchema) + ", "
                        + quote(primaryTable) + ", " + quote(foreignCatalog) + ", " + quote(foreignSchema) + ", "
                        + quote(foreignTable) + ')');
            }
            return getResultSet(meta.getCrossReference(primaryCatalog, primarySchema, primaryTable, foreignCatalog,
                    foreignSchema, foreignTable));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of user-defined data types.
     * This call returns an empty result set.
     *
     * <ol>
     * <li>TYPE_CAT (String) catalog</li>
     * <li>TYPE_SCHEM (String) schema</li>
     * <li>TYPE_NAME (String) type name</li>
     * <li>CLASS_NAME (String) Java class</li>
     * <li>DATA_TYPE (short) SQL Type - see also java.sql.Types</li>
     * <li>REMARKS (String) description</li>
     * <li>BASE_TYPE (short) base type - see also java.sql.Types</li>
     * </ol>
     *
     * @param catalog ignored
     * @param schemaPattern ignored
     * @param typeNamePattern ignored
     * @param types ignored
     * @return an empty result set
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern,
            String typeNamePattern, int[] types) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getUDTs("
                        +quote(catalog)+", "
                        +quote(schemaPattern)+", "
                        +quote(typeNamePattern)+", "
                        +quoteIntArray(types)+')');
            }
            return getResultSet(meta.getUDTs(catalog, schemaPattern, typeNamePattern, types));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of data types. The result set is sorted by DATA_TYPE and
     * afterwards by how closely the data type maps to the corresponding JDBC
     * SQL type (best match first).
     *
     * <ol>
     * <li>TYPE_NAME (String) type name</li>
     * <li>DATA_TYPE (short) SQL data type - see also java.sql.Types</li>
     * <li>PRECISION (int) maximum precision</li>
     * <li>LITERAL_PREFIX (String) prefix used to quote a literal</li>
     * <li>LITERAL_SUFFIX (String) suffix used to quote a literal</li>
     * <li>CREATE_PARAMS (String) parameters used (may be null)</li>
     * <li>NULLABLE (short) typeNoNulls (NULL not allowed) or typeNullable</li>
     * <li>CASE_SENSITIVE (boolean) case sensitive</li>
     * <li>SEARCHABLE (short) typeSearchable</li>
     * <li>UNSIGNED_ATTRIBUTE (boolean) unsigned</li>
     * <li>FIXED_PREC_SCALE (boolean) fixed precision</li>
     * <li>AUTO_INCREMENT (boolean) auto increment</li>
     * <li>LOCAL_TYPE_NAME (String) localized version of the data type</li>
     * <li>MINIMUM_SCALE (short) minimum scale</li>
     * <li>MAXIMUM_SCALE (short) maximum scale</li>
     * <li>SQL_DATA_TYPE (int) unused</li>
     * <li>SQL_DATETIME_SUB (int) unused</li>
     * <li>NUM_PREC_RADIX (int) 2 for binary, 10 for decimal</li>
     * </ol>
     *
     * @return the list of data types
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getTypeInfo() throws SQLException {
        try {
            debugCodeCall("getTypeInfo");
            return getResultSet(meta.getTypeInfo());
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if this database store data in local files.
     *
     * @return true
     */
    @Override
    public boolean usesLocalFiles() {
        debugCodeCall("usesLocalFiles");
        return true;
    }

    /**
     * Checks if this database use one file per table.
     *
     * @return false
     */
    @Override
    public boolean usesLocalFilePerTable() {
        debugCodeCall("usesLocalFilePerTable");
        return false;
    }

    /**
     * Returns the string used to quote identifiers.
     *
     * @return a double quote
     */
    @Override
    public String getIdentifierQuoteString() {
        debugCodeCall("getIdentifierQuoteString");
        return "\"";
    }

    /**
     * Gets the comma-separated list of all SQL keywords that are not supported
     * as unquoted identifiers, in addition to the SQL:2003 reserved words.
     * <p>
     * List of keywords in H2 may depend on compatibility mode and other
     * settings.
     * </p>
     *
     * @return a list of additional keywords
     */
    @Override
    public String getSQLKeywords() throws SQLException {
        try {
            debugCodeCall("getSQLKeywords");
            return meta.getSQLKeywords();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the list of numeric functions supported by this database.
     *
     * @return the list
     */
    @Override
    public String getNumericFunctions() throws SQLException {
        try {
            debugCodeCall("getNumericFunctions");
            return meta.getNumericFunctions();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the list of string functions supported by this database.
     *
     * @return the list
     */
    @Override
    public String getStringFunctions() throws SQLException {
        try {
            debugCodeCall("getStringFunctions");
            return meta.getStringFunctions();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the list of system functions supported by this database.
     *
     * @return the list
     */
    @Override
    public String getSystemFunctions() throws SQLException {
        try {
            debugCodeCall("getSystemFunctions");
            return meta.getSystemFunctions();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the list of date and time functions supported by this database.
     *
     * @return the list
     */
    @Override
    public String getTimeDateFunctions() throws SQLException {
        try {
            debugCodeCall("getTimeDateFunctions");
            return meta.getTimeDateFunctions();
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
    @Override
    public String getSearchStringEscape() throws SQLException {
        try {
            debugCodeCall("getSearchStringEscape");
            return meta.getSearchStringEscape();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the characters that are allowed for identifiers in addiction to
     * A-Z, a-z, 0-9 and '_'.
     *
     * @return an empty String ("")
     */
    @Override
    public String getExtraNameCharacters() {
        debugCodeCall("getExtraNameCharacters");
        return "";
    }

    /**
     * Returns whether alter table with add column is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsAlterTableWithAddColumn() {
        debugCodeCall("supportsAlterTableWithAddColumn");
        return true;
    }

    /**
     * Returns whether alter table with drop column is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsAlterTableWithDropColumn() {
        debugCodeCall("supportsAlterTableWithDropColumn");
        return true;
    }

    /**
     * Returns whether column aliasing is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsColumnAliasing() {
        debugCodeCall("supportsColumnAliasing");
        return true;
    }

    /**
     * Returns whether NULL+1 is NULL or not.
     *
     * @return true
     */
    @Override
    public boolean nullPlusNonNullIsNull() {
        debugCodeCall("nullPlusNonNullIsNull");
        return true;
    }

    /**
     * Returns whether CONVERT is supported.
     *
     * @return true
     */
    @Override
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
    @Override
    public boolean supportsConvert(int fromType, int toType) {
        if (isDebugEnabled()) {
            debugCode("supportsConvert(" + fromType + ", " + toType + ')');
        }
        return true;
    }

    /**
     * Returns whether table correlation names (table alias) are supported.
     *
     * @return true
     */
    @Override
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
    @Override
    public boolean supportsDifferentTableCorrelationNames() {
        debugCodeCall("supportsDifferentTableCorrelationNames");
        return false;
    }

    /**
     * Returns whether expression in ORDER BY are supported.
     *
     * @return true
     */
    @Override
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
    @Override
    public boolean supportsOrderByUnrelated() {
        debugCodeCall("supportsOrderByUnrelated");
        return true;
    }

    /**
     * Returns whether GROUP BY is supported.
     *
     * @return true
     */
    @Override
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
    @Override
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
    @Override
    public boolean supportsGroupByBeyondSelect() {
        debugCodeCall("supportsGroupByBeyondSelect");
        return true;
    }

    /**
     * Returns whether LIKE... ESCAPE is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsLikeEscapeClause() {
        debugCodeCall("supportsLikeEscapeClause");
        return true;
    }

    /**
     * Returns whether multiple result sets are supported.
     *
     * @return false
     */
    @Override
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
    @Override
    public boolean supportsMultipleTransactions() {
        debugCodeCall("supportsMultipleTransactions");
        return true;
    }

    /**
     * Returns whether columns with NOT NULL are supported.
     *
     * @return true
     */
    @Override
    public boolean supportsNonNullableColumns() {
        debugCodeCall("supportsNonNullableColumns");
        return true;
    }

    /**
     * Returns whether ODBC Minimum SQL grammar is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsMinimumSQLGrammar() {
        debugCodeCall("supportsMinimumSQLGrammar");
        return true;
    }

    /**
     * Returns whether ODBC Core SQL grammar is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsCoreSQLGrammar() {
        debugCodeCall("supportsCoreSQLGrammar");
        return true;
    }

    /**
     * Returns whether ODBC Extended SQL grammar is supported.
     *
     * @return false
     */
    @Override
    public boolean supportsExtendedSQLGrammar() {
        debugCodeCall("supportsExtendedSQLGrammar");
        return false;
    }

    /**
     * Returns whether SQL-92 entry level grammar is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsANSI92EntryLevelSQL() {
        debugCodeCall("supportsANSI92EntryLevelSQL");
        return true;
    }

    /**
     * Returns whether SQL-92 intermediate level grammar is supported.
     *
     * @return false
     */
    @Override
    public boolean supportsANSI92IntermediateSQL() {
        debugCodeCall("supportsANSI92IntermediateSQL");
        return false;
    }

    /**
     * Returns whether SQL-92 full level grammar is supported.
     *
     * @return false
     */
    @Override
    public boolean supportsANSI92FullSQL() {
        debugCodeCall("supportsANSI92FullSQL");
        return false;
    }

    /**
     * Returns whether referential integrity is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsIntegrityEnhancementFacility() {
        debugCodeCall("supportsIntegrityEnhancementFacility");
        return true;
    }

    /**
     * Returns whether outer joins are supported.
     *
     * @return true
     */
    @Override
    public boolean supportsOuterJoins() {
        debugCodeCall("supportsOuterJoins");
        return true;
    }

    /**
     * Returns whether full outer joins are supported.
     *
     * @return false
     */
    @Override
    public boolean supportsFullOuterJoins() {
        debugCodeCall("supportsFullOuterJoins");
        return false;
    }

    /**
     * Returns whether limited outer joins are supported.
     *
     * @return true
     */
    @Override
    public boolean supportsLimitedOuterJoins() {
        debugCodeCall("supportsLimitedOuterJoins");
        return true;
    }

    /**
     * Returns the term for "schema".
     *
     * @return "schema"
     */
    @Override
    public String getSchemaTerm() {
        debugCodeCall("getSchemaTerm");
        return "schema";
    }

    /**
     * Returns the term for "procedure".
     *
     * @return "procedure"
     */
    @Override
    public String getProcedureTerm() {
        debugCodeCall("getProcedureTerm");
        return "procedure";
    }

    /**
     * Returns the term for "catalog".
     *
     * @return "catalog"
     */
    @Override
    public String getCatalogTerm() {
        debugCodeCall("getCatalogTerm");
        return "catalog";
    }

    /**
     * Returns whether the catalog is at the beginning.
     *
     * @return true
     */
    @Override
    public boolean isCatalogAtStart() {
        debugCodeCall("isCatalogAtStart");
        return true;
    }

    /**
     * Returns the catalog separator.
     *
     * @return "."
     */
    @Override
    public String getCatalogSeparator() {
        debugCodeCall("getCatalogSeparator");
        return ".";
    }

    /**
     * Returns whether the schema name in INSERT, UPDATE, DELETE is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsSchemasInDataManipulation() {
        debugCodeCall("supportsSchemasInDataManipulation");
        return true;
    }

    /**
     * Returns whether the schema name in procedure calls is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsSchemasInProcedureCalls() {
        debugCodeCall("supportsSchemasInProcedureCalls");
        return true;
    }

    /**
     * Returns whether the schema name in CREATE TABLE is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsSchemasInTableDefinitions() {
        debugCodeCall("supportsSchemasInTableDefinitions");
        return true;
    }

    /**
     * Returns whether the schema name in CREATE INDEX is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsSchemasInIndexDefinitions() {
        debugCodeCall("supportsSchemasInIndexDefinitions");
        return true;
    }

    /**
     * Returns whether the schema name in GRANT is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() {
        debugCodeCall("supportsSchemasInPrivilegeDefinitions");
        return true;
    }

    /**
     * Returns whether the catalog name in INSERT, UPDATE, DELETE is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsCatalogsInDataManipulation() {
        debugCodeCall("supportsCatalogsInDataManipulation");
        return true;
    }

    /**
     * Returns whether the catalog name in procedure calls is supported.
     *
     * @return false
     */
    @Override
    public boolean supportsCatalogsInProcedureCalls() {
        debugCodeCall("supportsCatalogsInProcedureCalls");
        return false;
    }

    /**
     * Returns whether the catalog name in CREATE TABLE is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsCatalogsInTableDefinitions() {
        debugCodeCall("supportsCatalogsInTableDefinitions");
        return true;
    }

    /**
     * Returns whether the catalog name in CREATE INDEX is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsCatalogsInIndexDefinitions() {
        debugCodeCall("supportsCatalogsInIndexDefinitions");
        return true;
    }

    /**
     * Returns whether the catalog name in GRANT is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() {
        debugCodeCall("supportsCatalogsInPrivilegeDefinitions");
        return true;
    }

    /**
     * Returns whether positioned deletes are supported.
     *
     * @return false
     */
    @Override
    public boolean supportsPositionedDelete() {
        debugCodeCall("supportsPositionedDelete");
        return false;
    }

    /**
     * Returns whether positioned updates are supported.
     *
     * @return false
     */
    @Override
    public boolean supportsPositionedUpdate() {
        debugCodeCall("supportsPositionedUpdate");
        return false;
    }

    /**
     * Returns whether SELECT ... FOR UPDATE is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsSelectForUpdate() {
        debugCodeCall("supportsSelectForUpdate");
        return true;
    }

    /**
     * Returns whether stored procedures are supported.
     *
     * @return false
     */
    @Override
    public boolean supportsStoredProcedures() {
        debugCodeCall("supportsStoredProcedures");
        return false;
    }

    /**
     * Returns whether subqueries (SELECT) in comparisons are supported.
     *
     * @return true
     */
    @Override
    public boolean supportsSubqueriesInComparisons() {
        debugCodeCall("supportsSubqueriesInComparisons");
        return true;
    }

    /**
     * Returns whether SELECT in EXISTS is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsSubqueriesInExists() {
        debugCodeCall("supportsSubqueriesInExists");
        return true;
    }

    /**
     * Returns whether IN(SELECT...) is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsSubqueriesInIns() {
        debugCodeCall("supportsSubqueriesInIns");
        return true;
    }

    /**
     * Returns whether subqueries in quantified expression are supported.
     *
     * @return true
     */
    @Override
    public boolean supportsSubqueriesInQuantifieds() {
        debugCodeCall("supportsSubqueriesInQuantifieds");
        return true;
    }

    /**
     * Returns whether correlated subqueries are supported.
     *
     * @return true
     */
    @Override
    public boolean supportsCorrelatedSubqueries() {
        debugCodeCall("supportsCorrelatedSubqueries");
        return true;
    }

    /**
     * Returns whether UNION SELECT is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsUnion() {
        debugCodeCall("supportsUnion");
        return true;
    }

    /**
     * Returns whether UNION ALL SELECT is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsUnionAll() {
        debugCodeCall("supportsUnionAll");
        return true;
    }

    /**
     * Returns whether open result sets across commits are supported.
     *
     * @return false
     */
    @Override
    public boolean supportsOpenCursorsAcrossCommit() {
        debugCodeCall("supportsOpenCursorsAcrossCommit");
        return false;
    }

    /**
     * Returns whether open result sets across rollback are supported.
     *
     * @return false
     */
    @Override
    public boolean supportsOpenCursorsAcrossRollback() {
        debugCodeCall("supportsOpenCursorsAcrossRollback");
        return false;
    }

    /**
     * Returns whether open statements across commit are supported.
     *
     * @return true
     */
    @Override
    public boolean supportsOpenStatementsAcrossCommit() {
        debugCodeCall("supportsOpenStatementsAcrossCommit");
        return true;
    }

    /**
     * Returns whether open statements across rollback are supported.
     *
     * @return true
     */
    @Override
    public boolean supportsOpenStatementsAcrossRollback() {
        debugCodeCall("supportsOpenStatementsAcrossRollback");
        return true;
    }

    /**
     * Returns whether transactions are supported.
     *
     * @return true
     */
    @Override
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
    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        debugCodeCall("supportsTransactionIsolationLevel");
        switch (level) {
        case Connection.TRANSACTION_READ_UNCOMMITTED:
        case Connection.TRANSACTION_READ_COMMITTED:
        case Connection.TRANSACTION_REPEATABLE_READ:
        case Constants.TRANSACTION_SNAPSHOT:
        case Connection.TRANSACTION_SERIALIZABLE:
            return true;
        default:
            return false;
        }
    }

    /**
     * Returns whether data manipulation and CREATE/DROP is supported in
     * transactions.
     *
     * @return false
     */
    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        debugCodeCall("supportsDataDefinitionAndDataManipulationTransactions");
        return false;
    }

    /**
     * Returns whether only data manipulations are supported in transactions.
     *
     * @return true
     */
    @Override
    public boolean supportsDataManipulationTransactionsOnly() {
        debugCodeCall("supportsDataManipulationTransactionsOnly");
        return true;
    }

    /**
     * Returns whether CREATE/DROP commit an open transaction.
     *
     * @return true
     */
    @Override
    public boolean dataDefinitionCausesTransactionCommit() {
        debugCodeCall("dataDefinitionCausesTransactionCommit");
        return true;
    }

    /**
     * Returns whether CREATE/DROP do not affect transactions.
     *
     * @return false
     */
    @Override
    public boolean dataDefinitionIgnoredInTransactions() {
        debugCodeCall("dataDefinitionIgnoredInTransactions");
        return false;
    }

    /**
     * Returns whether a specific result set type is supported.
     * ResultSet.TYPE_SCROLL_SENSITIVE is not supported.
     *
     * @param type the result set type
     * @return true for all types except ResultSet.TYPE_SCROLL_SENSITIVE
     */
    @Override
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
    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        if (isDebugEnabled()) {
            debugCode("supportsResultSetConcurrency(" + type + ", " + concurrency + ')');
        }
        return type != ResultSet.TYPE_SCROLL_SENSITIVE;
    }

    /**
     * Returns whether own updates are visible.
     *
     * @param type the result set type
     * @return true
     */
    @Override
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
    @Override
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
    @Override
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
    @Override
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
    @Override
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
    @Override
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
    @Override
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
    @Override
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
    @Override
    public boolean insertsAreDetected(int type) {
        debugCodeCall("insertsAreDetected", type);
        return false;
    }

    /**
     * Returns whether batch updates are supported.
     *
     * @return true
     */
    @Override
    public boolean supportsBatchUpdates() {
        debugCodeCall("supportsBatchUpdates");
        return true;
    }

    /**
     * Returns whether the maximum row size includes blobs.
     *
     * @return false
     */
    @Override
    public boolean doesMaxRowSizeIncludeBlobs() {
        debugCodeCall("doesMaxRowSizeIncludeBlobs");
        return false;
    }

    /**
     * Returns the default transaction isolation level.
     *
     * @return Connection.TRANSACTION_READ_COMMITTED
     */
    @Override
    public int getDefaultTransactionIsolation() {
        debugCodeCall("getDefaultTransactionIsolation");
        return Connection.TRANSACTION_READ_COMMITTED;
    }

    /**
     * Checks if for CREATE TABLE Test(ID INT), getTables returns Test as the
     * table name and identifiers are case sensitive.
     *
     * @return true is so, false otherwise
     */
    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        debugCodeCall("supportsMixedCaseIdentifiers");
        Session.StaticSettings settings = conn.getStaticSettings();
        return !settings.databaseToUpper && !settings.databaseToLower && !settings.caseInsensitiveIdentifiers;
    }

    /**
     * Checks if for CREATE TABLE Test(ID INT), getTables returns TEST as the
     * table name.
     *
     * @return true is so, false otherwise
     */
    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        debugCodeCall("storesUpperCaseIdentifiers");
        return conn.getStaticSettings().databaseToUpper;
    }

    /**
     * Checks if for CREATE TABLE Test(ID INT), getTables returns test as the
     * table name.
     *
     * @return true is so, false otherwise
     */
    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        debugCodeCall("storesLowerCaseIdentifiers");
        return conn.getStaticSettings().databaseToLower;
    }

    /**
     * Checks if for CREATE TABLE Test(ID INT), getTables returns Test as the
     * table name and identifiers are not case sensitive.
     *
     * @return true is so, false otherwise
     */
    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        debugCodeCall("storesMixedCaseIdentifiers");
        Session.StaticSettings settings = conn.getStaticSettings();
        return !settings.databaseToUpper && !settings.databaseToLower && settings.caseInsensitiveIdentifiers;
    }

    /**
     * Checks if a table created with CREATE TABLE "Test"(ID INT) is a different
     * table than a table created with CREATE TABLE "TEST"(ID INT).
     *
     * @return true is so, false otherwise
     */
    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        debugCodeCall("supportsMixedCaseQuotedIdentifiers");
        return !conn.getStaticSettings().caseInsensitiveIdentifiers;
    }

    /**
     * Checks if for CREATE TABLE "Test"(ID INT), getTables returns TEST as the
     * table name.
     *
     * @return false
     */
    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        debugCodeCall("storesUpperCaseQuotedIdentifiers");
        return false;
    }

    /**
     * Checks if for CREATE TABLE "Test"(ID INT), getTables returns test as the
     * table name.
     *
     * @return false
     */
    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        debugCodeCall("storesLowerCaseQuotedIdentifiers");
        return false;
    }

    /**
     * Checks if for CREATE TABLE "Test"(ID INT), getTables returns Test as the
     * table name and identifiers are case insensitive.
     *
     * @return true is so, false otherwise
     */
    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        debugCodeCall("storesMixedCaseQuotedIdentifiers");
        return conn.getStaticSettings().caseInsensitiveIdentifiers;
    }

    /**
     * Returns the maximum length for hex values (characters).
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxBinaryLiteralLength() {
        debugCodeCall("getMaxBinaryLiteralLength");
        return 0;
    }

    /**
     * Returns the maximum length for literals.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxCharLiteralLength() {
        debugCodeCall("getMaxCharLiteralLength");
        return 0;
    }

    /**
     * Returns the maximum length for column names.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxColumnNameLength() {
        debugCodeCall("getMaxColumnNameLength");
        return 0;
    }

    /**
     * Returns the maximum number of columns in GROUP BY.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxColumnsInGroupBy() {
        debugCodeCall("getMaxColumnsInGroupBy");
        return 0;
    }

    /**
     * Returns the maximum number of columns in CREATE INDEX.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxColumnsInIndex() {
        debugCodeCall("getMaxColumnsInIndex");
        return 0;
    }

    /**
     * Returns the maximum number of columns in ORDER BY.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxColumnsInOrderBy() {
        debugCodeCall("getMaxColumnsInOrderBy");
        return 0;
    }

    /**
     * Returns the maximum number of columns in SELECT.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxColumnsInSelect() {
        debugCodeCall("getMaxColumnsInSelect");
        return 0;
    }

    /**
     * Returns the maximum number of columns in CREATE TABLE.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxColumnsInTable() {
        debugCodeCall("getMaxColumnsInTable");
        return 0;
    }

    /**
     * Returns the maximum number of open connection.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxConnections() {
        debugCodeCall("getMaxConnections");
        return 0;
    }

    /**
     * Returns the maximum length for a cursor name.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxCursorNameLength() {
        debugCodeCall("getMaxCursorNameLength");
        return 0;
    }

    /**
     * Returns the maximum length for an index (in bytes).
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxIndexLength() {
        debugCodeCall("getMaxIndexLength");
        return 0;
    }

    /**
     * Returns the maximum length for a schema name.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxSchemaNameLength() {
        debugCodeCall("getMaxSchemaNameLength");
        return 0;
    }

    /**
     * Returns the maximum length for a procedure name.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxProcedureNameLength() {
        debugCodeCall("getMaxProcedureNameLength");
        return 0;
    }

    /**
     * Returns the maximum length for a catalog name.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxCatalogNameLength() {
        debugCodeCall("getMaxCatalogNameLength");
        return 0;
    }

    /**
     * Returns the maximum size of a row (in bytes).
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxRowSize() {
        debugCodeCall("getMaxRowSize");
        return 0;
    }

    /**
     * Returns the maximum length of a statement.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxStatementLength() {
        debugCodeCall("getMaxStatementLength");
        return 0;
    }

    /**
     * Returns the maximum number of open statements.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxStatements() {
        debugCodeCall("getMaxStatements");
        return 0;
    }

    /**
     * Returns the maximum length for a table name.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxTableNameLength() {
        debugCodeCall("getMaxTableNameLength");
        return 0;
    }

    /**
     * Returns the maximum number of tables in a SELECT.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxTablesInSelect() {
        debugCodeCall("getMaxTablesInSelect");
        return 0;
    }

    /**
     * Returns the maximum length for a user name.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxUserNameLength() {
        debugCodeCall("getMaxUserNameLength");
        return 0;
    }

    /**
     * Does the database support savepoints.
     *
     * @return true
     */
    @Override
    public boolean supportsSavepoints() {
        debugCodeCall("supportsSavepoints");
        return true;
    }

    /**
     * Does the database support named parameters.
     *
     * @return false
     */
    @Override
    public boolean supportsNamedParameters() {
        debugCodeCall("supportsNamedParameters");
        return false;
    }

    /**
     * Does the database support multiple open result sets returned from a
     * <code>CallableStatement</code>.
     *
     * @return false
     */
    @Override
    public boolean supportsMultipleOpenResults() {
        debugCodeCall("supportsMultipleOpenResults");
        return false;
    }

    /**
     * Does the database support getGeneratedKeys.
     *
     * @return true
     */
    @Override
    public boolean supportsGetGeneratedKeys() {
        debugCodeCall("supportsGetGeneratedKeys");
        return true;
    }

    /**
     * [Not supported]
     */
    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getSuperTypes(" + quote(catalog) + ", " + quote(schemaPattern) + ", "
                        + quote(typeNamePattern) + ')');
            }
            return getResultSet(meta.getSuperTypes(catalog, schemaPattern, typeNamePattern));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Get the list of super tables of a table. This method currently returns an
     * empty result set.
     * <ol>
     * <li>TABLE_CAT (String) table catalog</li>
     * <li>TABLE_SCHEM (String) table schema</li>
     * <li>TABLE_NAME (String) table name</li>
     * <li>SUPERTABLE_NAME (String) the name of the super table</li>
     * </ol>
     *
     * @param catalog null (to get all objects) or the catalog name
     * @param schemaPattern null (to get all objects) or a schema name
     *            (uppercase for unquoted names)
     * @param tableNamePattern null (to get all objects) or a table name pattern
     *            (uppercase for unquoted names)
     * @return an empty result set
     */
    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) //
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getSuperTables(" + quote(catalog) + ", " + quote(schemaPattern) + ", "
                        + quote(tableNamePattern) + ')');
            }
            return getResultSet(meta.getSuperTables(catalog, schemaPattern, tableNamePattern));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */
    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
            String attributeNamePattern) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getAttributes(" + quote(catalog) + ", " + quote(schemaPattern) + ", "
                        + quote(typeNamePattern) + ", " + quote(attributeNamePattern) + ')');
            }
            return getResultSet(meta.getAttributes(catalog, schemaPattern, typeNamePattern, attributeNamePattern));
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
    @Override
    public boolean supportsResultSetHoldability(int holdability) {
        debugCodeCall("supportsResultSetHoldability", holdability);
        return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    /**
     * Gets the result set holdability.
     *
     * @return ResultSet.CLOSE_CURSORS_AT_COMMIT
     */
    @Override
    public int getResultSetHoldability() {
        debugCodeCall("getResultSetHoldability");
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    /**
     * Gets the major version of the database.
     *
     * @return the major version
     */
    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        try {
            debugCodeCall("getDatabaseMajorVersion");
            return meta.getDatabaseMajorVersion();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the minor version of the database.
     *
     * @return the minor version
     */
    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        try {
            debugCodeCall("getDatabaseMinorVersion");
            return meta.getDatabaseMinorVersion();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the major version of the supported JDBC API.
     *
     * @return the major version (4)
     */
    @Override
    public int getJDBCMajorVersion() {
        debugCodeCall("getJDBCMajorVersion");
        return 4;
    }

    /**
     * Gets the minor version of the supported JDBC API.
     *
     * @return the minor version (2)
     */
    @Override
    public int getJDBCMinorVersion() {
        debugCodeCall("getJDBCMinorVersion");
        return 3;
    }

    /**
     * Gets the SQL State type.
     *
     * @return {@link DatabaseMetaData#sqlStateSQL}
     */
    @Override
    public int getSQLStateType() {
        debugCodeCall("getSQLStateType");
        return DatabaseMetaData.sqlStateSQL;
    }

    /**
     * Does the database make a copy before updating.
     *
     * @return false
     */
    @Override
    public boolean locatorsUpdateCopy() {
        debugCodeCall("locatorsUpdateCopy");
        return false;
    }

    /**
     * Does the database support statement pooling.
     *
     * @return false
     */
    @Override
    public boolean supportsStatementPooling() {
        debugCodeCall("supportsStatementPooling");
        return false;
    }

    // =============================================================

    private void checkClosed() {
        conn.checkClosed();
    }

    /**
     * Get the lifetime of a rowid.
     *
     * @return ROWID_UNSUPPORTED
     */
    @Override
    public RowIdLifetime getRowIdLifetime() {
        debugCodeCall("getRowIdLifetime");
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    /**
     * Gets the list of schemas in the database.
     * The result set is sorted by TABLE_SCHEM.
     *
     * <ol>
     * <li>TABLE_SCHEM (String) schema name</li>
     * <li>TABLE_CATALOG (String) catalog name</li>
     * </ol>
     *
     * @param catalogPattern null (to get all objects) or the catalog name
     * @param schemaPattern null (to get all objects) or a schema name
     *            (uppercase for unquoted names)
     * @return the schema list
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getSchemas(String catalogPattern, String schemaPattern)
            throws SQLException {
        try {
            debugCodeCall("getSchemas(String,String)");
            return getResultSet(meta.getSchemas(catalogPattern, schemaPattern));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns whether the database supports calling functions using the call
     * syntax.
     *
     * @return true
     */
    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() {
        debugCodeCall("supportsStoredFunctionsUsingCallSyntax");
        return true;
    }

    /**
     * Returns whether an exception while auto commit is on closes all result
     * sets.
     *
     * @return false
     */
    @Override
    public boolean autoCommitFailureClosesAllResultSets() {
        debugCodeCall("autoCommitFailureClosesAllResultSets");
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        Properties clientInfo = conn.getClientInfo();
        SimpleResult result = new SimpleResult();
        result.addColumn("NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("MAX_LEN", TypeInfo.TYPE_INTEGER);
        result.addColumn("DEFAULT_VALUE", TypeInfo.TYPE_VARCHAR);
        result.addColumn("DESCRIPTION", TypeInfo.TYPE_VARCHAR);
        // Non-standard column
        result.addColumn("VALUE", TypeInfo.TYPE_VARCHAR);
        for (Entry<Object, Object> entry : clientInfo.entrySet()) {
            result.addRow(ValueVarchar.get((String) entry.getKey()), ValueInteger.get(Integer.MAX_VALUE),
                    ValueVarchar.EMPTY, ValueVarchar.EMPTY, ValueVarchar.get((String) entry.getValue()));
        }
        int id = getNextId(TraceObject.RESULT_SET);
        debugCodeAssign("ResultSet", TraceObject.RESULT_SET, id, "getClientInfoProperties()");
        return new JdbcResultSet(conn, null, null, result, id, true, false, false);
    }

    /**
     * Return an object of this class if possible.
     *
     * @param iface the class
     * @return this
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            if (isWrapperFor(iface)) {
                return (T) this;
            }
            throw DbException.getInvalidValueException("iface", iface);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if unwrap can return an object of this class.
     *
     * @param iface the class
     * @return whether or not the interface is assignable from this class
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(getClass());
    }

    /**
     * [Not supported] Gets the list of function columns.
     */
    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
            String columnNamePattern) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getFunctionColumns(" + quote(catalog) + ", " + quote(schemaPattern) + ", "
                        + quote(functionNamePattern) + ", " + quote(columnNamePattern) + ')');
            }
            return getResultSet(
                    meta.getFunctionColumns(catalog, schemaPattern, functionNamePattern, columnNamePattern));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Gets the list of functions.
     */
    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getFunctions(" + quote(catalog) + ", " + quote(schemaPattern) + ", "
                        + quote(functionNamePattern) + ')');
            }
            return getResultSet(meta.getFunctions(catalog, schemaPattern, functionNamePattern));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns whether database always returns generated keys if valid names or
     * indexes of columns were specified and command was completed successfully.
     *
     * @return true
     */
    @Override
    public boolean generatedKeyAlwaysReturned() {
        return true;
    }

    /**
     * Gets the list of pseudo and invisible columns. The result set is sorted
     * by TABLE_SCHEM, TABLE_NAME, and COLUMN_NAME.
     *
     * <ol>
     * <li>TABLE_CAT (String) table catalog</li>
     * <li>TABLE_SCHEM (String) table schema</li>
     * <li>TABLE_NAME (String) table name</li>
     * <li>COLUMN_NAME (String) column name</li>
     * <li>DATA_TYPE (int) data type (see java.sql.Types)</li>
     * <li>COLUMN_SIZE (int) precision
     *         (values larger than 2 GB are returned as 2 GB)</li>
     * <li>DECIMAL_DIGITS (int) scale (0 for INTEGER and VARCHAR)</li>
     * <li>NUM_PREC_RADIX (int) radix</li>
     * <li>COLUMN_USAGE (String) he allowed usage for the column,
     *         see {@link java.sql.PseudoColumnUsage}</li>
     * <li>REMARKS (String) comment</li>
     * <li>CHAR_OCTET_LENGTH (int) for char types the
     *         maximum number of bytes in the column</li>
     * <li>IS_NULLABLE (String) "NO" or "YES"</li>
     * </ol>
     *
     * @param catalog null (to get all objects) or the catalog name
     * @param schemaPattern null (to get all objects) or a schema name
     *            (uppercase for unquoted names)
     * @param tableNamePattern null (to get all objects) or a table name
     *            (uppercase for unquoted names)
     * @param columnNamePattern null (to get all objects) or a column name
     *            (uppercase for unquoted names)
     * @return the list of pseudo and invisible columns
     */
    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
            String columnNamePattern) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getPseudoColumns(" + quote(catalog) + ", " + quote(schemaPattern) + ", "
                        + quote(tableNamePattern) + ", " + quote(columnNamePattern) + ')');
            }
            return getResultSet(meta.getPseudoColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * INTERNAL
     */
    @Override
    public String toString() {
        return getTraceObjectName() + ": " + conn;
    }

    private JdbcResultSet getResultSet(ResultInterface result) {
        return new JdbcResultSet(conn, null, null, result, getNextId(TraceObject.RESULT_SET), true, false, false);
    }

}
