/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;

import org.h2.engine.Constants;
import org.h2.test.TestBase;
import org.h2.value.DataType;

/**
 * Test for the DatabaseMetaData implementation.
 */
public class TestMetaData extends TestBase {

    Connection conn;
    DatabaseMetaData meta;
    Statement stat;
    String catalog = "METADATA";

    public void test() throws Exception {
        deleteDb("metaData");
        conn = getConnection("metaData");

        testColumnDefault();
        testCrossReferences();
        testProcedureColumns();

        stat = conn.createStatement();
        meta = conn.getMetaData();
        testStatic();
        // TODO test remaining meta data

        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        stat.execute("CREATE INDEX IDXNAME ON TEST(NAME)");

        ResultSet rs;

        rs = meta.getCatalogs();
        rs.next();
        check(rs.getString(1), catalog);
        checkFalse(rs.next());

        rs = meta.getSchemas();
        rs.next();
        check(rs.getString("TABLE_SCHEM"), "INFORMATION_SCHEMA");
        rs.next();
        check(rs.getString("TABLE_SCHEM"), "PUBLIC");
        checkFalse(rs.next());

        rs = meta.getTableTypes();
        rs.next();
        check(rs.getString("TABLE_TYPE"), "SYSTEM TABLE");
        rs.next();
        check(rs.getString("TABLE_TYPE"), "TABLE");
        rs.next();
        check(rs.getString("TABLE_TYPE"), "TABLE LINK");
        rs.next();
        check(rs.getString("TABLE_TYPE"), "VIEW");
        checkFalse(rs.next());

        rs = meta.getTables(null, Constants.SCHEMA_MAIN, null, new String[] { "TABLE" });
        check(rs.getStatement() == null);
        rs.next();
        check(rs.getString("TABLE_NAME"), "TEST");
        checkFalse(rs.next());

        rs = meta.getTables(null, "INFORMATION_SCHEMA", null, new String[] { "TABLE", "SYSTEM TABLE" });
        rs.next();
        check("CATALOGS", rs.getString("TABLE_NAME"));
        rs.next();
        check("COLLATIONS", rs.getString("TABLE_NAME"));
        rs.next();
        check("COLUMNS", rs.getString("TABLE_NAME"));
        rs.next();
        check("COLUMN_PRIVILEGES", rs.getString("TABLE_NAME"));
        rs.next();
        check("CONSTANTS", rs.getString("TABLE_NAME"));
        rs.next();
        check("CONSTRAINTS", rs.getString("TABLE_NAME"));
        rs.next();
        check("CROSS_REFERENCES", rs.getString("TABLE_NAME"));
        rs.next();
        check("DOMAINS", rs.getString("TABLE_NAME"));
        rs.next();
        check("FUNCTION_ALIASES", rs.getString("TABLE_NAME"));
        rs.next();
        check("FUNCTION_COLUMNS", rs.getString("TABLE_NAME"));
        rs.next();
        check("HELP", rs.getString("TABLE_NAME"));
        rs.next();
        check("INDEXES", rs.getString("TABLE_NAME"));
        rs.next();
        check("IN_DOUBT", rs.getString("TABLE_NAME"));
        rs.next();
        check("LOCKS", rs.getString("TABLE_NAME"));
        rs.next();
        check("RIGHTS", rs.getString("TABLE_NAME"));
        rs.next();
        check("ROLES", rs.getString("TABLE_NAME"));
        rs.next();
        check("SCHEMATA", rs.getString("TABLE_NAME"));
        rs.next();
        check("SEQUENCES", rs.getString("TABLE_NAME"));
        rs.next();
        check("SESSIONS", rs.getString("TABLE_NAME"));
        rs.next();
        check("SETTINGS", rs.getString("TABLE_NAME"));
        rs.next();
        check("TABLES", rs.getString("TABLE_NAME"));
        rs.next();
        check("TABLE_PRIVILEGES", rs.getString("TABLE_NAME"));
        rs.next();
        check("TABLE_TYPES", rs.getString("TABLE_NAME"));
        rs.next();
        check("TRIGGERS", rs.getString("TABLE_NAME"));
        rs.next();
        check("TYPE_INFO", rs.getString("TABLE_NAME"));
        rs.next();
        check("USERS", rs.getString("TABLE_NAME"));
        rs.next();
        check("VIEWS", rs.getString("TABLE_NAME"));
        checkFalse(rs.next());

        rs = meta.getColumns(null, null, "TEST", null);
        rs.next();
        check(rs.getString("COLUMN_NAME"), "ID");
        rs.next();
        check(rs.getString("COLUMN_NAME"), "NAME");
        checkFalse(rs.next());

        rs = meta.getPrimaryKeys(null, null, "TEST");
        rs.next();
        check(rs.getString("COLUMN_NAME"), "ID");
        checkFalse(rs.next());

        rs = meta.getBestRowIdentifier(null, null, "TEST", DatabaseMetaData.bestRowSession, false);
        rs.next();
        check(rs.getString("COLUMN_NAME"), "ID");
        checkFalse(rs.next());

        rs = meta.getIndexInfo(null, null, "TEST", false, false);
        rs.next();
        String index = rs.getString("INDEX_NAME");
        check(index.startsWith("PRIMARY_KEY"));
        check(rs.getString("COLUMN_NAME"), "ID");
        rs.next();
        check(rs.getString("INDEX_NAME"), "IDXNAME");
        check(rs.getString("COLUMN_NAME"), "NAME");
        checkFalse(rs.next());

        rs = meta.getIndexInfo(null, null, "TEST", true, false);
        rs.next();
        index = rs.getString("INDEX_NAME");
        check(index.startsWith("PRIMARY_KEY"));
        check(rs.getString("COLUMN_NAME"), "ID");
        checkFalse(rs.next());

        rs = meta.getVersionColumns(null, null, "TEST");
        checkFalse(rs.next());

        stat.execute("DROP TABLE TEST");

        rs = stat.executeQuery("SELECT * FROM INFORMATION_SCHEMA.SETTINGS");
        while (rs.next()) {
            String name = rs.getString("NAME");
            String value = rs.getString("VALUE");
            trace(name + "=" + value);
        }

        test(conn);

        // meta.getTablePrivileges()

        // meta.getAttributes()
        // meta.getColumnPrivileges()
        // meta.getSuperTables()
        // meta.getSuperTypes()
        // meta.getTypeInfo()
        // meta.getUDTs()

        conn.close();
        testTempTable();

    }

    private void testColumnDefault() throws Exception {
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet rs;
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(A INT, B INT DEFAULT NULL)");
        rs = meta.getColumns(null, null, "TEST", null);
        rs.next();
        check("A", rs.getString("COLUMN_NAME"));
        check(null, rs.getString("COLUMN_DEF"));
        rs.next();
        check("B", rs.getString("COLUMN_NAME"));
        check("NULL", rs.getString("COLUMN_DEF"));
        checkFalse(rs.next());
        stat.execute("DROP TABLE TEST");
    }

    private void testProcedureColumns() throws Exception {
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet rs;
        Statement stat = conn.createStatement();
        stat.execute("CREATE ALIAS PROP FOR \"java.lang.System.getProperty(java.lang.String)\"");
        stat.execute("CREATE ALIAS EXIT FOR \"java.lang.System.exit\"");
        rs = meta.getProcedures(null, null, "EX%");
        testResultSetMeta(rs, 8, new String[] { "PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME",
                "NUM_INPUT_PARAMS", "NUM_OUTPUT_PARAMS", "NUM_RESULT_SETS", "REMARKS", "PROCEDURE_TYPE" }, new int[] {
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.INTEGER,
                Types.VARCHAR, Types.SMALLINT }, null, null);
        testResultSetOrdered(rs, new String[][] { { catalog, Constants.SCHEMA_MAIN, "EXIT", "0", "0", "0", "",
                "" + DatabaseMetaData.procedureNoResult }, });
        rs = meta.getProcedureColumns(null, null, null, null);
        testResultSetMeta(rs, 13,
                new String[] { "PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "COLUMN_NAME", "COLUMN_TYPE",
                        "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH", "SCALE", "RADIX", "NULLABLE", "REMARKS" },
                new int[] { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT, Types.INTEGER,
                        Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.SMALLINT, Types.SMALLINT, Types.SMALLINT,
                        Types.VARCHAR }, null, null);
        testResultSetOrdered(rs, new String[][] {
                { catalog, Constants.SCHEMA_MAIN, "EXIT", "P1", "" + DatabaseMetaData.procedureColumnIn,
                        "" + Types.INTEGER, "INTEGER", "10", "10", "0", "10", "" + DatabaseMetaData.procedureNoNulls },
                { catalog, Constants.SCHEMA_MAIN, "PROP", "P1", "" + DatabaseMetaData.procedureColumnIn,
                        "" + Types.VARCHAR, "VARCHAR", "" + Integer.MAX_VALUE, "" + Integer.MAX_VALUE, "0", "10",
                        "" + DatabaseMetaData.procedureNullable }, });
        stat.execute("DROP ALIAS EXIT");
        stat.execute("DROP ALIAS PROP");
    }

    private void testCrossReferences() throws Exception {
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet rs;
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE PARENT(A INT, B INT, PRIMARY KEY(A, B))");
        stat
                .execute("CREATE TABLE CHILD(ID INT PRIMARY KEY, PA INT, PB INT, CONSTRAINT AB FOREIGN KEY(PA, PB) REFERENCES PARENT(A, B))");
        rs = meta.getCrossReference(null, "PUBLIC", "PARENT", null, "PUBLIC", "CHILD");
        checkCrossRef(rs);
        rs = meta.getImportedKeys(null, "PUBLIC", "CHILD");
        checkCrossRef(rs);
        rs = meta.getExportedKeys(null, "PUBLIC", "PARENT");
        checkCrossRef(rs);
        stat.execute("DROP TABLE PARENT");
        stat.execute("DROP TABLE CHILD");
    }

    private void checkCrossRef(ResultSet rs) throws Exception {
        testResultSetMeta(rs, 14, new String[] { "PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
                "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE",
                "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY" }, new int[] { Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.SMALLINT, Types.SMALLINT, Types.SMALLINT, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT }, null,
                null);
        testResultSetOrdered(rs, new String[][] {
                { catalog, Constants.SCHEMA_MAIN, "PARENT", "A", catalog, Constants.SCHEMA_MAIN, "CHILD", "PA", "1",
                        "" + DatabaseMetaData.importedKeyRestrict, "" + DatabaseMetaData.importedKeyRestrict, "AB",
                        null, "" + DatabaseMetaData.importedKeyNotDeferrable },
                { catalog, Constants.SCHEMA_MAIN, "PARENT", "B", catalog, Constants.SCHEMA_MAIN, "CHILD", "PB", "2",
                        "" + DatabaseMetaData.importedKeyRestrict, "" + DatabaseMetaData.importedKeyRestrict, "AB",
                        null, "" + DatabaseMetaData.importedKeyNotDeferrable } });
    }

    void testTempTable() throws Exception {
        Connection conn = getConnection("metaData");
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST_TEMP");
        stat.execute("CREATE TEMP TABLE TEST_TEMP(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        stat.execute("CREATE INDEX IDX_NAME ON TEST_TEMP(NAME)");
        stat.execute("ALTER TABLE TEST_TEMP ADD FOREIGN KEY(ID) REFERENCES(ID)");
        conn.close();

        conn = getConnection("metaData");
        stat = conn.createStatement();
        stat.execute("CREATE TEMP TABLE TEST_TEMP(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        ResultSet rs = stat.executeQuery("SELECT STORAGE_TYPE FROM "
                + "INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='TEST_TEMP'");
        rs.next();
        check(rs.getString("STORAGE_TYPE"), "GLOBAL TEMPORARY");
        stat.execute("DROP TABLE IF EXISTS TEST_TEMP");
        conn.close();
    }

    void testStatic() throws Exception {
        Driver dr = (Driver) Class.forName("org.h2.Driver").newInstance();

        check(dr.getMajorVersion(), meta.getDriverMajorVersion());
        check(dr.getMinorVersion(), meta.getDriverMinorVersion());
        check(dr.jdbcCompliant());

        check(dr.getPropertyInfo(null, null).length, 0);
        check(dr.connect("jdbc:test:false", null) == null);

        check(meta.getNumericFunctions().length() > 0);
        check(meta.getStringFunctions().length() > 0);
        check(meta.getSystemFunctions().length() > 0);
        check(meta.getTimeDateFunctions().length() > 0);

        check(meta.allProceduresAreCallable());
        check(meta.allTablesAreSelectable());
        check(meta.dataDefinitionCausesTransactionCommit());
        checkFalse(meta.dataDefinitionIgnoredInTransactions());
        checkFalse(meta.deletesAreDetected(ResultSet.TYPE_FORWARD_ONLY));
        checkFalse(meta.deletesAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE));
        checkFalse(meta.deletesAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE));
        checkFalse(meta.doesMaxRowSizeIncludeBlobs());
        check(meta.getCatalogSeparator(), ".");
        check(meta.getCatalogTerm(), "catalog");
        check(meta.getConnection() == conn);
        if (config.jdk14) {
            String versionStart = meta.getDatabaseMajorVersion() + "." + meta.getDatabaseMinorVersion();
            check(meta.getDatabaseProductVersion().startsWith(versionStart));
            check(meta.getDriverMajorVersion(), meta.getDatabaseMajorVersion());
            check(meta.getDriverMinorVersion(), meta.getDatabaseMinorVersion());
            check(meta.getJDBCMajorVersion(), 3);
            check(meta.getJDBCMinorVersion(), 0);
        }
        check(meta.getDatabaseProductName(), "H2");
        check(meta.getDefaultTransactionIsolation(), Connection.TRANSACTION_READ_COMMITTED);
        check(meta.getDriverName(), "H2 JDBC Driver");

        String versionStart = meta.getDriverMajorVersion() + "." + meta.getDriverMinorVersion();
        check(meta.getDriverVersion().startsWith(versionStart));
        check(meta.getExtraNameCharacters(), "");
        check(meta.getIdentifierQuoteString(), "\"");
        check(meta.getMaxBinaryLiteralLength(), 0);
        check(meta.getMaxCatalogNameLength(), 0);
        check(meta.getMaxCharLiteralLength(), 0);
        check(meta.getMaxColumnNameLength(), 0);
        check(meta.getMaxColumnsInGroupBy(), 0);
        check(meta.getMaxColumnsInIndex(), 0);
        check(meta.getMaxColumnsInOrderBy(), 0);
        check(meta.getMaxColumnsInSelect(), 0);
        check(meta.getMaxColumnsInTable(), 0);
        check(meta.getMaxConnections(), 0);
        check(meta.getMaxCursorNameLength(), 0);
        check(meta.getMaxIndexLength(), 0);
        check(meta.getMaxProcedureNameLength(), 0);
        check(meta.getMaxRowSize(), 0);
        check(meta.getMaxSchemaNameLength(), 0);
        check(meta.getMaxStatementLength(), 0);
        check(meta.getMaxStatements(), 0);
        check(meta.getMaxTableNameLength(), 0);
        check(meta.getMaxTablesInSelect(), 0);
        check(meta.getMaxUserNameLength(), 0);
        check(meta.getProcedureTerm(), "procedure");
        if (config.jdk14) {
            check(meta.getResultSetHoldability(), ResultSet.CLOSE_CURSORS_AT_COMMIT);
            check(meta.getSQLStateType(), DatabaseMetaData.sqlStateSQL99);
            checkFalse(meta.locatorsUpdateCopy());
        }
        check(meta.getSchemaTerm(), "schema");
        check(meta.getSearchStringEscape(), "\\");
        check(meta.getSQLKeywords(), "");

        check(meta.getURL().startsWith("jdbc:h2:"));
        check(meta.getUserName().length() > 1);
        checkFalse(meta.insertsAreDetected(ResultSet.TYPE_FORWARD_ONLY));
        checkFalse(meta.insertsAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE));
        checkFalse(meta.insertsAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE));
        check(meta.isCatalogAtStart());
        checkFalse(meta.isReadOnly());
        check(meta.nullPlusNonNullIsNull());
        checkFalse(meta.nullsAreSortedAtEnd());
        checkFalse(meta.nullsAreSortedAtStart());
        checkFalse(meta.nullsAreSortedHigh());
        check(meta.nullsAreSortedLow());
        checkFalse(meta.othersDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        checkFalse(meta.othersDeletesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
        checkFalse(meta.othersDeletesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
        checkFalse(meta.othersInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        checkFalse(meta.othersInsertsAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
        checkFalse(meta.othersInsertsAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
        checkFalse(meta.othersUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        checkFalse(meta.othersUpdatesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
        checkFalse(meta.othersUpdatesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
        checkFalse(meta.ownDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        checkFalse(meta.ownDeletesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
        checkFalse(meta.ownDeletesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
        checkFalse(meta.ownInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        checkFalse(meta.ownInsertsAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
        checkFalse(meta.ownInsertsAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
        checkFalse(meta.ownUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        checkFalse(meta.ownUpdatesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
        checkFalse(meta.ownUpdatesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
        checkFalse(meta.storesLowerCaseIdentifiers());
        checkFalse(meta.storesLowerCaseQuotedIdentifiers());
        checkFalse(meta.storesMixedCaseIdentifiers());
        check(meta.storesMixedCaseQuotedIdentifiers());
        check(meta.storesUpperCaseIdentifiers());
        checkFalse(meta.storesUpperCaseQuotedIdentifiers());
        check(meta.supportsAlterTableWithAddColumn());
        check(meta.supportsAlterTableWithDropColumn());
        check(meta.supportsANSI92EntryLevelSQL());
        checkFalse(meta.supportsANSI92IntermediateSQL());
        checkFalse(meta.supportsANSI92FullSQL());
        check(meta.supportsBatchUpdates());
        check(meta.supportsCatalogsInDataManipulation());
        check(meta.supportsCatalogsInIndexDefinitions());
        check(meta.supportsCatalogsInPrivilegeDefinitions());
        checkFalse(meta.supportsCatalogsInProcedureCalls());
        check(meta.supportsCatalogsInTableDefinitions());
        check(meta.supportsColumnAliasing());
        check(meta.supportsConvert());
        check(meta.supportsConvert(Types.INTEGER, Types.VARCHAR));
        check(meta.supportsCoreSQLGrammar());
        check(meta.supportsCorrelatedSubqueries());
        checkFalse(meta.supportsDataDefinitionAndDataManipulationTransactions());
        check(meta.supportsDataManipulationTransactionsOnly());
        checkFalse(meta.supportsDifferentTableCorrelationNames());
        check(meta.supportsExpressionsInOrderBy());
        checkFalse(meta.supportsExtendedSQLGrammar());
        checkFalse(meta.supportsFullOuterJoins());
        if (config.jdk14) {
            check(meta.supportsGetGeneratedKeys());
            check(meta.supportsMultipleOpenResults());
            checkFalse(meta.supportsNamedParameters());
        }
        check(meta.supportsGroupBy());
        check(meta.supportsGroupByBeyondSelect());
        check(meta.supportsGroupByUnrelated());
        check(meta.supportsIntegrityEnhancementFacility());
        check(meta.supportsLikeEscapeClause());
        check(meta.supportsLimitedOuterJoins());
        check(meta.supportsMinimumSQLGrammar());
        checkFalse(meta.supportsMixedCaseIdentifiers());
        check(meta.supportsMixedCaseQuotedIdentifiers());
        checkFalse(meta.supportsMultipleResultSets());
        check(meta.supportsMultipleTransactions());
        check(meta.supportsNonNullableColumns());
        checkFalse(meta.supportsOpenCursorsAcrossCommit());
        checkFalse(meta.supportsOpenCursorsAcrossRollback());
        check(meta.supportsOpenStatementsAcrossCommit());
        check(meta.supportsOpenStatementsAcrossRollback());
        check(meta.supportsOrderByUnrelated());
        check(meta.supportsOuterJoins());
        check(meta.supportsPositionedDelete());
        check(meta.supportsPositionedUpdate());
        check(meta.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        check(meta.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));
        check(meta.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));
        check(meta.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE));
        checkFalse(meta.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY));
        checkFalse(meta.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE));
        if (config.jdk14) {
            checkFalse(meta.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
            check(meta.supportsResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT));
            check(meta.supportsSavepoints());
            checkFalse(meta.supportsStatementPooling());
        }
        check(meta.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
        check(meta.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE));
        checkFalse(meta.supportsResultSetType(ResultSet.TYPE_SCROLL_SENSITIVE));
        check(meta.supportsSchemasInDataManipulation());
        check(meta.supportsSchemasInIndexDefinitions());
        check(meta.supportsSchemasInPrivilegeDefinitions());
        check(meta.supportsSchemasInProcedureCalls());
        check(meta.supportsSchemasInTableDefinitions());
        check(meta.supportsSelectForUpdate());
        checkFalse(meta.supportsStoredProcedures());
        check(meta.supportsSubqueriesInComparisons());
        check(meta.supportsSubqueriesInExists());
        check(meta.supportsSubqueriesInIns());
        check(meta.supportsSubqueriesInQuantifieds());
        check(meta.supportsTableCorrelationNames());
        check(meta.supportsTransactions());
        check(meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_NONE));
        check(meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED));
        check(meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED));
        check(meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ));
        check(meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE));
        check(meta.supportsUnion());
        check(meta.supportsUnionAll());
        checkFalse(meta.updatesAreDetected(ResultSet.TYPE_FORWARD_ONLY));
        checkFalse(meta.updatesAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE));
        checkFalse(meta.updatesAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE));
        checkFalse(meta.usesLocalFilePerTable());
        check(meta.usesLocalFiles());
    }

    void test(Connection conn) throws Exception {
        DatabaseMetaData meta = conn.getMetaData();
        Statement stat = conn.createStatement();
        ResultSet rs;

        conn.setReadOnly(true);
        conn.setReadOnly(false);
        checkFalse(conn.isReadOnly());
        check(conn.isReadOnly() == meta.isReadOnly());

        check(conn == meta.getConnection());

        // currently, setCatalog is ignored
        conn.setCatalog("XYZ");
        trace(conn.getCatalog());

        String product = meta.getDatabaseProductName();
        trace("meta.getDatabaseProductName:" + product);

        String version = meta.getDatabaseProductVersion();
        trace("meta.getDatabaseProductVersion:" + version);

        int major = meta.getDriverMajorVersion();
        trace("meta.getDriverMajorVersion:" + major);

        int minor = meta.getDriverMinorVersion();
        trace("meta.getDriverMinorVersion:" + minor);

        String driverName = meta.getDriverName();
        trace("meta.getDriverName:" + driverName);

        String driverVersion = meta.getDriverVersion();
        trace("meta.getDriverVersion:" + driverVersion);

        meta.getSearchStringEscape();

        String url = meta.getURL();
        trace("meta.getURL:" + url);

        String user = meta.getUserName();
        trace("meta.getUserName:" + user);

        trace("meta.nullsAreSortedHigh:" + meta.nullsAreSortedHigh());
        trace("meta.nullsAreSortedLow:" + meta.nullsAreSortedLow());
        trace("meta.nullsAreSortedAtStart:" + meta.nullsAreSortedAtStart());
        trace("meta.nullsAreSortedAtEnd:" + meta.nullsAreSortedAtEnd());
        int count = (meta.nullsAreSortedHigh() ? 1 : 0) + (meta.nullsAreSortedLow() ? 1 : 0)
                + (meta.nullsAreSortedAtStart() ? 1 : 0) + (meta.nullsAreSortedAtEnd() ? 1 : 0);
        check(count == 1);

        trace("meta.allProceduresAreCallable:" + meta.allProceduresAreCallable());
        check(meta.allProceduresAreCallable());

        trace("meta.allTablesAreSelectable:" + meta.allTablesAreSelectable());
        check(meta.allTablesAreSelectable());

        trace("getTables");
        rs = meta.getTables(null, Constants.SCHEMA_MAIN, null, new String[] { "TABLE" });
        testResultSetMeta(rs, 6, new String[] { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS",
                "SQL" }, new int[] { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR }, null, null);
        if (rs.next()) {
            error("Database is not empty after dropping all tables");
        }
        stat.executeUpdate("CREATE TABLE TEST(" + "ID INT PRIMARY KEY," + "TEXT_V VARCHAR(120),"
                + "DEC_V DECIMAL(12,3)," + "DATE_V DATETIME," + "BLOB_V BLOB," + "CLOB_V CLOB" + ")");
        rs = meta.getTables(null, Constants.SCHEMA_MAIN, null, new String[] { "TABLE" });
        testResultSetOrdered(rs, new String[][] { { catalog, Constants.SCHEMA_MAIN, "TEST", "TABLE", "" } });
        trace("getColumns");
        rs = meta.getColumns(null, null, "TEST", null);
        testResultSetMeta(rs, 18, new String[] { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE",
                "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS",
                "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
                "IS_NULLABLE" }, new int[] { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.SMALLINT, Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER,
                Types.SMALLINT, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT, Types.INTEGER, Types.INTEGER,
                Types.INTEGER, Types.VARCHAR }, null, null);
        testResultSetOrdered(rs,
                new String[][] {
                        { catalog, Constants.SCHEMA_MAIN, "TEST", "ID", "" + Types.INTEGER, "INTEGER", "10", "10", "0",
                                "10", "" + DatabaseMetaData.columnNoNulls, "", null, "" + Types.INTEGER, "0", "10", "1",
                                "NO" },
                        { catalog, Constants.SCHEMA_MAIN, "TEST", "TEXT_V", "" + Types.VARCHAR, "VARCHAR", "120",
                                "120", "0", "10", "" + DatabaseMetaData.columnNullable, "", null, "" + Types.VARCHAR,
                                "0", "120", "2", "YES" },
                        { catalog, Constants.SCHEMA_MAIN, "TEST", "DEC_V", "" + Types.DECIMAL, "DECIMAL", "12", "12",
                                "3", "10", "" + DatabaseMetaData.columnNullable, "", null, "" + Types.DECIMAL, "0", "12",
                                "3", "YES" },
                        { catalog, Constants.SCHEMA_MAIN, "TEST", "DATE_V", "" + Types.TIMESTAMP, "TIMESTAMP", "23",
                                "23", "10", "10", "" + DatabaseMetaData.columnNullable, "", null, "" + Types.TIMESTAMP,
                                "0", "23", "4", "YES" },
                        { catalog, Constants.SCHEMA_MAIN, "TEST", "BLOB_V", "" + Types.BLOB, "BLOB",
                                "" + Integer.MAX_VALUE, "" + Integer.MAX_VALUE, "0", "10",
                                "" + DatabaseMetaData.columnNullable, "", null, "" + Types.BLOB, "0",
                                "" + Integer.MAX_VALUE, "5", "YES" },
                        { catalog, Constants.SCHEMA_MAIN, "TEST", "CLOB_V", "" + Types.CLOB, "CLOB",
                                "" + Integer.MAX_VALUE, "" + Integer.MAX_VALUE, "0", "10",
                                "" + DatabaseMetaData.columnNullable, "", null, "" + Types.CLOB, "0",
                                "" + Integer.MAX_VALUE, "6", "YES" } });
        /*
         * rs=meta.getColumns(null,null,"TEST",null); while(rs.next()) { int
         * datatype=rs.getInt(5); }
         */
        trace("getIndexInfo");
        stat.executeUpdate("CREATE INDEX IDX_TEXT_DEC ON TEST(TEXT_V,DEC_V)");
        stat.executeUpdate("CREATE UNIQUE INDEX IDX_DATE ON TEST(DATE_V)");
        rs = meta.getIndexInfo(null, null, "TEST", false, false);
        testResultSetMeta(rs, 14, new String[] { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE",
                "INDEX_QUALIFIER", "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC",
                "CARDINALITY", "PAGES", "FILTER_CONDITION", "SORT_TYPE"}, new int[] { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                DataType.TYPE_BOOLEAN, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT, Types.SMALLINT, Types.VARCHAR,
                Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.VARCHAR, Types.INTEGER}, null, null);
        testResultSetOrdered(rs, new String[][] {
                { catalog, Constants.SCHEMA_MAIN, "TEST", "FALSE", catalog, "IDX_DATE",
                        "" + DatabaseMetaData.tableIndexOther, "1", "DATE_V", "A", "0", "0", "" },
                { catalog, Constants.SCHEMA_MAIN, "TEST", "FALSE", catalog, "PRIMARY_KEY_2",
                        "" + DatabaseMetaData.tableIndexOther, "1", "ID", "A", "0", "0", "" },
                { catalog, Constants.SCHEMA_MAIN, "TEST", "TRUE", catalog, "IDX_TEXT_DEC",
                        "" + DatabaseMetaData.tableIndexOther, "1", "TEXT_V", "A", "0", "0", "" },
                { catalog, Constants.SCHEMA_MAIN, "TEST", "TRUE", catalog, "IDX_TEXT_DEC",
                        "" + DatabaseMetaData.tableIndexOther, "2", "DEC_V", "A", "0", "0", "" }, });
        stat.executeUpdate("DROP INDEX IDX_TEXT_DEC");
        stat.executeUpdate("DROP INDEX IDX_DATE");
        rs = meta.getIndexInfo(null, null, "TEST", false, false);
        testResultSetMeta(rs, 14, new String[] { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE",
                "INDEX_QUALIFIER", "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC",
                "CARDINALITY", "PAGES", "FILTER_CONDITION", "SORT_TYPE" }, new int[] { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                DataType.TYPE_BOOLEAN, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT, Types.SMALLINT, Types.VARCHAR,
                Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.VARCHAR, Types.INTEGER }, null, null);
        testResultSetOrdered(rs, new String[][] { { catalog, Constants.SCHEMA_MAIN, "TEST", "FALSE", catalog,
                "PRIMARY_KEY_2", "" + DatabaseMetaData.tableIndexOther, "1", "ID", "A", "0", "0", "" } });
        trace("getPrimaryKeys");
        rs = meta.getPrimaryKeys(null, null, "TEST");
        testResultSetMeta(rs, 6, new String[] { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ",
                "PK_NAME" }, new int[] { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT,
                Types.VARCHAR }, null, null);
        testResultSetOrdered(rs,
                new String[][] { { catalog, Constants.SCHEMA_MAIN, "TEST", "ID", "1", "PRIMARY_KEY_2" }, });
        trace("getTables - using a wildcard");
        stat.executeUpdate("CREATE TABLE T_2(B INT,A VARCHAR(6),C INT,PRIMARY KEY(C,A,B))");
        stat.executeUpdate("CREATE TABLE TX2(B INT,A VARCHAR(6),C INT,PRIMARY KEY(C,A,B))");
        rs = meta.getTables(null, null, "T_2", null);
        testResultSetOrdered(rs, new String[][] { { catalog, Constants.SCHEMA_MAIN, "TX2", "TABLE", "" },
                { catalog, Constants.SCHEMA_MAIN, "T_2", "TABLE", "" } });
        trace("getTables - using a quoted _ character");
        rs = meta.getTables(null, null, "T\\_2", null);
        testResultSetOrdered(rs, new String[][] { { catalog, Constants.SCHEMA_MAIN, "T_2", "TABLE", "" } });
        trace("getTables - using the % wildcard");
        rs = meta.getTables(null, Constants.SCHEMA_MAIN, "%", new String[] { "TABLE" });
        testResultSetOrdered(rs, new String[][] { { catalog, Constants.SCHEMA_MAIN, "TEST", "TABLE", "" },
                { catalog, Constants.SCHEMA_MAIN, "TX2", "TABLE", "" },
                { catalog, Constants.SCHEMA_MAIN, "T_2", "TABLE", "" } });
        stat.execute("DROP TABLE TEST");

        trace("getColumns - using wildcards");
        rs = meta.getColumns(null, null, "___", "B%");
        testResultSetOrdered(rs, new String[][] {
                { catalog, Constants.SCHEMA_MAIN, "TX2", "B", "" + Types.INTEGER, "INTEGER", "10" /*
                                                                                                     * ,
                                                                                                     * null,
                                                                                                     * "0",
                                                                                                     * "10", "" +
                                                                                                     * DatabaseMetaData.columnNoNulls,
                                                                                                     * null,
                                                                                                     * null,
                                                                                                     * null,
                                                                                                     * null,
                                                                                                     * null,
                                                                                                     * "1",
                                                                                                     * "NO"
                                                                                                     */},
                { catalog, Constants.SCHEMA_MAIN, "T_2", "B", "" + Types.INTEGER, "INTEGER", "10" /*
                                                                                                     * ,
                                                                                                     * null,
                                                                                                     * "0",
                                                                                                     * "10", "" +
                                                                                                     * DatabaseMetaData.columnNoNulls,
                                                                                                     * null,
                                                                                                     * null,
                                                                                                     * null,
                                                                                                     * null,
                                                                                                     * null,
                                                                                                     * "1",
                                                                                                     * "NO"
                                                                                                     */}, });
        trace("getColumns - using wildcards");
        rs = meta.getColumns(null, null, "_\\__", "%");
        testResultSetOrdered(rs, new String[][] {
                { catalog, Constants.SCHEMA_MAIN, "T_2", "B", "" + Types.INTEGER, "INTEGER", "10" /*
                                                                                                     * ,
                                                                                                     * null,
                                                                                                     * "0",
                                                                                                     * "10", "" +
                                                                                                     * DatabaseMetaData.columnNoNulls,
                                                                                                     * null,
                                                                                                     * null,
                                                                                                     * null,
                                                                                                     * null,
                                                                                                     * null,
                                                                                                     * "1",
                                                                                                     * "NO"
                                                                                                     */},
                { catalog, Constants.SCHEMA_MAIN, "T_2", "A", "" + Types.VARCHAR, "VARCHAR", "6" /*
                                                                                                     * ,
                                                                                                     * null,
                                                                                                     * "0",
                                                                                                     * "10", "" +
                                                                                                     * DatabaseMetaData.columnNoNulls,
                                                                                                     * null,
                                                                                                     * null,
                                                                                                     * null,
                                                                                                     * null,
                                                                                                     * null,
                                                                                                     * "2",
                                                                                                     * "NO"
                                                                                                     */},
                { catalog, Constants.SCHEMA_MAIN, "T_2", "C", "" + Types.INTEGER, "INTEGER", "10" /*
                                                                                                     * ,
                                                                                                     * null,
                                                                                                     * "0",
                                                                                                     * "10", "" +
                                                                                                     * DatabaseMetaData.columnNoNulls,
                                                                                                     * null,
                                                                                                     * null,
                                                                                                     * null,
                                                                                                     * null,
                                                                                                     * null,
                                                                                                     * "3",
                                                                                                     * "NO"
                                                                                                     */}, });
        trace("getIndexInfo");
        stat.executeUpdate("CREATE UNIQUE INDEX A_INDEX ON TX2(B,C,A)");
        stat.executeUpdate("CREATE INDEX B_INDEX ON TX2(A,B,C)");
        rs = meta.getIndexInfo(null, null, "TX2", false, false);
        testResultSetOrdered(rs, new String[][] {
                { catalog, Constants.SCHEMA_MAIN, "TX2", "FALSE", catalog, "A_INDEX",
                        "" + DatabaseMetaData.tableIndexOther, "1", "B", "A" /*
                                                                                 * ,
                                                                                 * null,
                                                                                 * null,
                                                                                 * null
                                                                                 */},
                { catalog, Constants.SCHEMA_MAIN, "TX2", "FALSE", catalog, "A_INDEX",
                        "" + DatabaseMetaData.tableIndexOther, "2", "C", "A" /*
                                                                                 * ,
                                                                                 * null,
                                                                                 * null,
                                                                                 * null
                                                                                 */},
                { catalog, Constants.SCHEMA_MAIN, "TX2", "FALSE", catalog, "A_INDEX",
                        "" + DatabaseMetaData.tableIndexOther, "3", "A", "A" /*
                                                                                 * ,
                                                                                 * null,
                                                                                 * null,
                                                                                 * null
                                                                                 */},
                { catalog, Constants.SCHEMA_MAIN, "TX2", "FALSE", catalog, "PRIMARY_KEY_14",
                        "" + DatabaseMetaData.tableIndexOther, "1", "C", "A" /*
                                                                                 * ,
                                                                                 * null,
                                                                                 * null,
                                                                                 * null
                                                                                 */},
                { catalog, Constants.SCHEMA_MAIN, "TX2", "FALSE", catalog, "PRIMARY_KEY_14",
                        "" + DatabaseMetaData.tableIndexOther, "2", "A", "A" /*
                                                                                 * ,
                                                                                 * null,
                                                                                 * null,
                                                                                 * null
                                                                                 */},
                { catalog, Constants.SCHEMA_MAIN, "TX2", "FALSE", catalog, "PRIMARY_KEY_14",
                        "" + DatabaseMetaData.tableIndexOther, "3", "B", "A"/*
                                                                             * ,
                                                                             * null,
                                                                             * null,
                                                                             * null
                                                                             */},
                { catalog, Constants.SCHEMA_MAIN, "TX2", "TRUE", catalog, "B_INDEX",
                        "" + DatabaseMetaData.tableIndexOther, "1", "A", "A" /*
                                                                                 * ,
                                                                                 * null,
                                                                                 * null,
                                                                                 * null
                                                                                 */},
                { catalog, Constants.SCHEMA_MAIN, "TX2", "TRUE", catalog, "B_INDEX",
                        "" + DatabaseMetaData.tableIndexOther, "2", "B", "A" /*
                                                                                 * ,
                                                                                 * null,
                                                                                 * null,
                                                                                 * null
                                                                                 */},
                { catalog, Constants.SCHEMA_MAIN, "TX2", "TRUE", catalog, "B_INDEX",
                        "" + DatabaseMetaData.tableIndexOther, "3", "C", "A" /*
                                                                                 * ,
                                                                                 * null,
                                                                                 * null,
                                                                                 * null
                                                                                 */}, });
        trace("getPrimaryKeys");
        rs = meta.getPrimaryKeys(null, null, "T_2");
        testResultSetOrdered(rs, new String[][] {
                { catalog, Constants.SCHEMA_MAIN, "T_2", "A", "2", "PRIMARY_KEY_1" },
                { catalog, Constants.SCHEMA_MAIN, "T_2", "B", "3", "PRIMARY_KEY_1" },
                { catalog, Constants.SCHEMA_MAIN, "T_2", "C", "1", "PRIMARY_KEY_1" }, });
        stat.executeUpdate("DROP TABLE TX2");
        stat.executeUpdate("DROP TABLE T_2");
        stat.executeUpdate("CREATE TABLE PARENT(ID INT PRIMARY KEY)");
        stat
                .executeUpdate("CREATE TABLE CHILD(P_ID INT,ID INT,PRIMARY KEY(P_ID,ID),FOREIGN KEY(P_ID) REFERENCES PARENT(ID))");

        trace("getImportedKeys");
        rs = meta.getImportedKeys(null, null, "CHILD");
        testResultSetMeta(rs, 14, new String[] { "PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
                "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE",
                "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY" }, new int[] { Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.SMALLINT, Types.SMALLINT, Types.SMALLINT, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT }, null,
                null);
        // TODO test
        // testResultSetOrdered(rs, new String[][] { { null, null, "PARENT",
        // "ID",
        // null, null, "CHILD", "P_ID", "1",
        // "" + DatabaseMetaData.importedKeyNoAction,
        // "" + DatabaseMetaData.importedKeyNoAction, "FK_1", null,
        // "" + DatabaseMetaData.importedKeyNotDeferrable}});

        trace("getExportedKeys");
        rs = meta.getExportedKeys(null, null, "PARENT");
        testResultSetMeta(rs, 14, new String[] { "PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
                "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE",
                "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY" }, new int[] { Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.SMALLINT, Types.SMALLINT, Types.SMALLINT, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT }, null,
                null);
        // TODO test
        /*
         * testResultSetOrdered(rs, new String[][]{ { null,null,"PARENT","ID",
         * null,null,"CHILD","P_ID",
         * "1",""+DatabaseMetaData.importedKeyNoAction,""+DatabaseMetaData.importedKeyNoAction,
         * null,null,""+DatabaseMetaData.importedKeyNotDeferrable } } );
         */
        trace("getCrossReference");
        rs = meta.getCrossReference(null, null, "PARENT", null, null, "CHILD");
        testResultSetMeta(rs, 14, new String[] { "PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
                "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE",
                "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY" }, new int[] { Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.SMALLINT, Types.SMALLINT, Types.SMALLINT, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT }, null,
                null);
        // TODO test
        /*
         * testResultSetOrdered(rs, new String[][]{ { null,null,"PARENT","ID",
         * null,null,"CHILD","P_ID",
         * "1",""+DatabaseMetaData.importedKeyNoAction,""+DatabaseMetaData.importedKeyNoAction,
         * null,null,""+DatabaseMetaData.importedKeyNotDeferrable } } );
         */

        rs = meta.getSchemas();
        testResultSetMeta(rs, 3, new String[] { "TABLE_SCHEM", "TABLE_CATALOG", "IS_DEFAULT" }, new int[] {
                Types.VARCHAR, Types.VARCHAR, DataType.TYPE_BOOLEAN }, null, null);
        check(rs.next());
        check(rs.getString(1), "INFORMATION_SCHEMA");
        check(rs.next());
        check(rs.getString(1), "PUBLIC");
        checkFalse(rs.next());

        rs = meta.getCatalogs();
        testResultSetMeta(rs, 1, new String[] { "TABLE_CAT" }, new int[] { Types.VARCHAR }, null, null);
        testResultSetOrdered(rs, new String[][] { { catalog } });

        rs = meta.getTableTypes();
        testResultSetMeta(rs, 1, new String[] { "TABLE_TYPE" }, new int[] { Types.VARCHAR }, null, null);
        testResultSetOrdered(rs, new String[][] { { "SYSTEM TABLE" }, { "TABLE" }, { "TABLE LINK" }, { "VIEW" } });

        rs = meta.getTypeInfo();
        testResultSetMeta(rs, 18, new String[] { "TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_PREFIX",
                "LITERAL_SUFFIX", "CREATE_PARAMS", "NULLABLE", "CASE_SENSITIVE", "SEARCHABLE", "UNSIGNED_ATTRIBUTE",
                "FIXED_PREC_SCALE", "AUTO_INCREMENT", "LOCAL_TYPE_NAME", "MINIMUM_SCALE", "MAXIMUM_SCALE",
                "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "NUM_PREC_RADIX" }, new int[] { Types.VARCHAR, Types.SMALLINT,
                Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT, DataType.TYPE_BOOLEAN,
                Types.SMALLINT, DataType.TYPE_BOOLEAN, DataType.TYPE_BOOLEAN, DataType.TYPE_BOOLEAN, Types.VARCHAR,
                Types.SMALLINT, Types.SMALLINT, Types.SMALLINT, Types.INTEGER, Types.INTEGER }, null, null);

        rs = meta.getTablePrivileges(null, null, null);
        testResultSetMeta(rs, 7, new String[] { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "GRANTOR", "GRANTEE",
                "PRIVILEGE", "IS_GRANTABLE" }, new int[] { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR }, null, null);

        rs = meta.getColumnPrivileges(null, null, "TEST", null);
        testResultSetMeta(rs, 8, new String[] { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "GRANTOR",
                "GRANTEE", "PRIVILEGE", "IS_GRANTABLE" }, new int[] { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR }, null, null);

        check(conn.getWarnings() == null);
        conn.clearWarnings();
        check(conn.getWarnings() == null);

    }

}
