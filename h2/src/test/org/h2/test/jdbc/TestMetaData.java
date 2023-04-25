/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import static org.h2.engine.Constants.MAX_ARRAY_CARDINALITY;
import static org.h2.engine.Constants.MAX_NUMERIC_PRECISION;
import static org.h2.engine.Constants.MAX_STRING_LENGTH;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.mode.DefaultNullOrdering;
import org.h2.test.TestBase;
import org.h2.test.TestDb;

/**
 * Test for the DatabaseMetaData implementation.
 */
public class TestMetaData extends TestDb {

    private static final String CATALOG = "METADATA";

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws SQLException {
        deleteDb("metaData");
        testUnwrap();
        testUnsupportedOperations();
        testTempTable();
        testColumnLobMeta();
        testColumnMetaData();
        testColumnPrecision();
        testColumnDefault();
        testColumnGenerated();
        testHiddenColumn();
        testCrossReferences();
        testProcedureColumns();
        testTypeInfo();
        testUDTs();
        testStatic();
        testNullsAreSortedAt();
        testGeneral();
        testAllowLiteralsNone();
        testClientInfo();
        testQueryStatistics();
        testQueryStatisticsLimit();
    }

    private void testUnwrap() throws SQLException {
        Connection conn = getConnection("metaData");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select 1 as x from dual");
        ResultSetMetaData meta = rs.getMetaData();
        assertTrue(meta.isWrapperFor(Object.class));
        assertTrue(meta.isWrapperFor(ResultSetMetaData.class));
        assertTrue(meta.isWrapperFor(meta.getClass()));
        assertTrue(meta == meta.unwrap(Object.class));
        assertTrue(meta == meta.unwrap(ResultSetMetaData.class));
        assertTrue(meta == meta.unwrap(meta.getClass()));
        assertFalse(meta.isWrapperFor(Integer.class));
        assertThrows(ErrorCode.INVALID_VALUE_2, meta).
                unwrap(Integer.class);
        conn.close();
    }

    private void testUnsupportedOperations() throws SQLException {
        Connection conn = getConnection("metaData");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select 1 as x from dual");
        ResultSetMetaData meta = rs.getMetaData();
        assertThrows(ErrorCode.INVALID_VALUE_2, meta).getColumnLabel(0);
        assertThrows(ErrorCode.INVALID_VALUE_2, meta).getColumnName(0);
        assertThrows(ErrorCode.INVALID_VALUE_2, meta).getColumnType(0);
        assertThrows(ErrorCode.INVALID_VALUE_2, meta).getColumnTypeName(0);
        assertThrows(ErrorCode.INVALID_VALUE_2, meta).getSchemaName(0);
        assertThrows(ErrorCode.INVALID_VALUE_2, meta).getTableName(0);
        assertThrows(ErrorCode.INVALID_VALUE_2, meta).getCatalogName(0);
        assertThrows(ErrorCode.INVALID_VALUE_2, meta).isAutoIncrement(0);
        assertThrows(ErrorCode.INVALID_VALUE_2, meta).isCaseSensitive(0);
        assertThrows(ErrorCode.INVALID_VALUE_2, meta).isSearchable(0);
        assertThrows(ErrorCode.INVALID_VALUE_2, meta).isCurrency(0);
        assertThrows(ErrorCode.INVALID_VALUE_2, meta).isNullable(0);
        assertThrows(ErrorCode.INVALID_VALUE_2, meta).isSigned(0);
        assertThrows(ErrorCode.INVALID_VALUE_2, meta).isReadOnly(0);
        assertThrows(ErrorCode.INVALID_VALUE_2, meta).isWritable(0);
        assertThrows(ErrorCode.INVALID_VALUE_2, meta).isDefinitelyWritable(0);
        assertThrows(ErrorCode.INVALID_VALUE_2, meta).getColumnClassName(0);
        assertThrows(ErrorCode.INVALID_VALUE_2, meta).getPrecision(0);
        assertThrows(ErrorCode.INVALID_VALUE_2, meta).getScale(0);
        assertThrows(ErrorCode.INVALID_VALUE_2, meta).getColumnDisplaySize(0);
        conn.close();
    }

    private void testColumnLobMeta() throws SQLException {
        Connection conn = getConnection("metaData");
        Statement stat = conn.createStatement();
        stat.executeUpdate("CREATE TABLE t (blob BLOB, clob CLOB)");
        stat.execute("INSERT INTO t VALUES('', '')");
        ResultSet rs = stat.executeQuery("SELECT blob,clob FROM t");
        ResultSetMetaData rsMeta = rs.getMetaData();
        assertEquals("java.sql.Blob", rsMeta.getColumnClassName(1));
        assertEquals("java.sql.Clob", rsMeta.getColumnClassName(2));
        rs.next();
        assertTrue(rs.getObject(1) instanceof java.sql.Blob);
        assertTrue(rs.getObject(2) instanceof java.sql.Clob);
        stat.executeUpdate("DROP TABLE t");
        conn.close();
    }

    private void testColumnMetaData() throws SQLException {
        Connection conn = getConnection("metaData");
        String sql = "select substring('Hello',0,1)";
        ResultSet rs = conn.prepareStatement(sql).executeQuery();
        rs.next();
        int type = rs.getMetaData().getColumnType(1);
        assertEquals(Types.VARCHAR, type);
        rs = conn.createStatement().executeQuery("SELECT COUNT(*) C FROM DUAL");
        assertEquals("C", rs.getMetaData().getColumnName(1));

        Statement stat = conn.createStatement();
        stat.execute("create table a(x int array)");
        stat.execute("insert into a values(ARRAY[1, 2])");
        rs = stat.executeQuery("SELECT x[1] FROM a");
        ResultSetMetaData rsMeta = rs.getMetaData();
        assertEquals(Types.INTEGER, rsMeta.getColumnType(1));
        rs.next();
        assertEquals(Integer.class.getName(),
                rs.getObject(1).getClass().getName());
        stat.execute("drop table a");
        conn.close();
    }

    private void testColumnPrecision() throws SQLException {
        Connection conn = getConnection("metaData");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE ONE(X NUMBER(12,2), Y FLOAT)");
        stat.execute("CREATE TABLE TWO AS SELECT * FROM ONE");
        ResultSet rs;
        ResultSetMetaData rsMeta;
        rs = stat.executeQuery("SELECT * FROM ONE");
        rsMeta = rs.getMetaData();
        assertEquals(12, rsMeta.getPrecision(1));
        assertEquals(53, rsMeta.getPrecision(2));
        assertEquals(Types.NUMERIC, rsMeta.getColumnType(1));
        assertEquals(Types.FLOAT, rsMeta.getColumnType(2));
        rs = stat.executeQuery("SELECT * FROM TWO");
        rsMeta = rs.getMetaData();
        assertEquals(12, rsMeta.getPrecision(1));
        assertEquals(53, rsMeta.getPrecision(2));
        assertEquals(Types.NUMERIC, rsMeta.getColumnType(1));
        assertEquals(Types.FLOAT, rsMeta.getColumnType(2));
        stat.execute("DROP TABLE ONE, TWO");
        conn.close();
    }

    private void testColumnDefault() throws SQLException {
        Connection conn = getConnection("metaData");
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet rs;
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(A INT, B INT DEFAULT NULL)");
        rs = meta.getColumns(null, null, "TEST", null);
        rs.next();
        assertEquals("A", rs.getString("COLUMN_NAME"));
        assertEquals(null, rs.getString("COLUMN_DEF"));
        rs.next();
        assertEquals("B", rs.getString("COLUMN_NAME"));
        assertEquals("NULL", rs.getString("COLUMN_DEF"));
        assertFalse(rs.next());
        stat.execute("DROP TABLE TEST");
        conn.close();
    }

    private void testColumnGenerated() throws SQLException {
        Connection conn = getConnection("metaData");
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet rs;
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(A INT, B INT AS A + 1)");
        rs = meta.getColumns(null, null, "TEST", null);
        rs.next();
        assertEquals("A", rs.getString("COLUMN_NAME"));
        assertEquals("NO", rs.getString("IS_GENERATEDCOLUMN"));
        rs.next();
        assertEquals("B", rs.getString("COLUMN_NAME"));
        assertEquals("YES", rs.getString("IS_GENERATEDCOLUMN"));
        assertFalse(rs.next());
        stat.execute("DROP TABLE TEST");
        conn.close();
    }

    private void testHiddenColumn() throws SQLException {
        Connection conn = getConnection("metaData");
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet rs;
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(A INT, B INT INVISIBLE)");
        rs = meta.getColumns(null, null, "TEST", null);
        assertTrue(rs.next());
        assertEquals("A", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());
        rs = meta.getPseudoColumns(null, null, "TEST", null);
        assertTrue(rs.next());
        assertEquals("B", rs.getString("COLUMN_NAME"));
        assertEquals("YES", rs.getString("IS_NULLABLE"));
        assertTrue(rs.next());
        assertEquals("_ROWID_", rs.getString("COLUMN_NAME"));
        assertEquals("NO", rs.getString("IS_NULLABLE"));
        assertFalse(rs.next());
        stat.execute("DROP TABLE TEST");
        conn.close();
    }

    private void testProcedureColumns() throws SQLException {
        Connection conn = getConnection("metaData");
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet rs;
        Statement stat = conn.createStatement();
        stat.execute("CREATE ALIAS PROP FOR 'java.lang.System.getProperty(java.lang.String)'");
        stat.execute("CREATE ALIAS EXIT FOR 'java.lang.System.exit'");
        rs = meta.getProcedures(null, null, "EX%");
        assertResultSetMeta(rs, 9, new String[] { "PROCEDURE_CAT",
                "PROCEDURE_SCHEM", "PROCEDURE_NAME", "RESERVED1",
                "RESERVED2", "RESERVED3", "REMARKS",
                "PROCEDURE_TYPE", "SPECIFIC_NAME" }, new int[] { Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.NULL, Types.NULL,
                Types.NULL, Types.VARCHAR, Types.SMALLINT, Types.VARCHAR },
                null, null);
        assertResultSetOrdered(rs, new String[][] { { CATALOG,
                Constants.SCHEMA_MAIN, "EXIT", null, null, null, null,
                "" + DatabaseMetaData.procedureNoResult, "EXIT_1" } });
        rs = meta.getProcedureColumns(null, null, null, null);
        assertResultSetMeta(rs, 20, new String[] { "PROCEDURE_CAT",
                "PROCEDURE_SCHEM", "PROCEDURE_NAME", "COLUMN_NAME",
                "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH",
                "SCALE", "RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF",
                "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME" },
                new int[] { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.SMALLINT, Types.INTEGER,
                        Types.VARCHAR, Types.INTEGER, Types.INTEGER,
                        Types.SMALLINT, Types.SMALLINT, Types.SMALLINT,
                        Types.VARCHAR, Types.VARCHAR, Types.INTEGER,
                        Types.INTEGER, Types.INTEGER, Types.INTEGER,
                        Types.VARCHAR, Types.VARCHAR }, null, null);
        assertResultSetOrdered(rs, new String[][] {
                { CATALOG, Constants.SCHEMA_MAIN, "EXIT", "P1",
                        "" + DatabaseMetaData.procedureColumnIn,
                        "" + Types.INTEGER, "INTEGER", "32", "32", null, "2",
                        "" + DatabaseMetaData.procedureNoNulls,
                        null, null, null, null, null, "1", "", "EXIT_1" },
                { CATALOG, Constants.SCHEMA_MAIN, "PROP", "RESULT",
                        "" + DatabaseMetaData.procedureColumnReturn,
                        "" + Types.VARCHAR, "CHARACTER VARYING", "" + MAX_STRING_LENGTH,
                        "" + MAX_STRING_LENGTH, null, null,
                        "" + DatabaseMetaData.procedureNullableUnknown,
                        null, null, null, null, "" + MAX_STRING_LENGTH, "0", "", "PROP_1" },
                { CATALOG, Constants.SCHEMA_MAIN, "PROP", "P1",
                        "" + DatabaseMetaData.procedureColumnIn,
                        "" + Types.VARCHAR, "CHARACTER VARYING", "" + MAX_STRING_LENGTH,
                        "" + MAX_STRING_LENGTH, null, null,
                        "" + DatabaseMetaData.procedureNullableUnknown,
                        null, null, null, null, "" + MAX_STRING_LENGTH, "1", "", "PROP_1" }, });
        stat.execute("DROP ALIAS EXIT");
        stat.execute("DROP ALIAS PROP");
        conn.close();
    }

    private void testTypeInfo() throws SQLException {
        Connection conn = getConnection("metaData");
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet rs;
        rs = meta.getTypeInfo();
        assertResultSetMeta(rs, 18,
                new String[] { "TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_PREFIX", "LITERAL_SUFFIX",
                        "CREATE_PARAMS", "NULLABLE", "CASE_SENSITIVE", "SEARCHABLE", "UNSIGNED_ATTRIBUTE",
                        "FIXED_PREC_SCALE", "AUTO_INCREMENT", "LOCAL_TYPE_NAME", "MINIMUM_SCALE", "MAXIMUM_SCALE",
                        "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "NUM_PREC_RADIX"},
                new int[] { Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.SMALLINT, Types.BOOLEAN, Types.SMALLINT, Types.BOOLEAN, Types.BOOLEAN, Types.BOOLEAN,
                        Types.VARCHAR, Types.SMALLINT, Types.SMALLINT, Types.INTEGER, Types.INTEGER, Types.INTEGER },
                null, null);
        testTypeInfo(rs, "TINYINT", Types.TINYINT, 8, null, null, null, false, false, true, (short) 0, (short) 0, 2);
        testTypeInfo(rs, "BIGINT", Types.BIGINT, 64, null, null, null, false, false, true, (short) 0, (short) 0, 2);
        testTypeInfo(rs, "BINARY VARYING", Types.VARBINARY, MAX_STRING_LENGTH, "X'", "'", "LENGTH", false, false,
                false, (short) 0, (short) 0, 0);
        testTypeInfo(rs, "BINARY", Types.BINARY, MAX_STRING_LENGTH, "X'", "'", "LENGTH", false, false, false,
                (short) 0, (short) 0, 0);
        testTypeInfo(rs, "UUID", Types.BINARY, 16, "'", "'", null, false, false, false, (short) 0, (short) 0, 0);
        testTypeInfo(rs, "CHARACTER", Types.CHAR, MAX_STRING_LENGTH, "'", "'", "LENGTH", true, false, false,
                (short) 0, (short) 0, 0);
        testTypeInfo(rs, "NUMERIC", Types.NUMERIC, MAX_NUMERIC_PRECISION, null, null, "PRECISION,SCALE", false, true,
                true, (short) 0, Short.MAX_VALUE, 10);
        testTypeInfo(rs, "DECFLOAT", Types.NUMERIC, MAX_NUMERIC_PRECISION, null, null, "PRECISION", false, false,
                true, (short) 0, (short) 0, 10);
        testTypeInfo(rs, "INTEGER", Types.INTEGER, 32, null, null, null, false, false, true,
                (short) 0, (short) 0, 2);
        testTypeInfo(rs, "SMALLINT", Types.SMALLINT, 16, null, null, null, false, false, true,
                (short) 0, (short) 0, 2);
        testTypeInfo(rs, "REAL", Types.REAL, 24, null, null, null, false, false, true, (short) 0, (short) 0, 2);
        testTypeInfo(rs, "DOUBLE PRECISION", Types.DOUBLE, 53, null, null, null, false, false, true, (short) 0,
                (short) 0, 2);
        testTypeInfo(rs, "CHARACTER VARYING", Types.VARCHAR, MAX_STRING_LENGTH, "'", "'", "LENGTH", true, false,
                false, (short) 0, (short) 0, 0);
        testTypeInfo(rs, "VARCHAR_IGNORECASE", Types.VARCHAR, MAX_STRING_LENGTH, "'", "'", "LENGTH", false, false,
                false, (short) 0, (short) 0, 0);
        testTypeInfo(rs, "BOOLEAN", Types.BOOLEAN, 1, null, null, null, false, false, false,
                (short) 0, (short) 0, 0);
        testTypeInfo(rs, "DATE", Types.DATE, 10, "DATE '", "'", null, false, false, false, (short) 0, (short) 0, 0);
        testTypeInfo(rs, "TIME", Types.TIME, 18, "TIME '", "'", "SCALE", false, false, false, (short) 0, (short) 9, 0);
        testTypeInfo(rs, "TIMESTAMP", Types.TIMESTAMP, 29, "TIMESTAMP '", "'", "SCALE", false, false, false,
                (short) 0, (short) 9, 0);
        testTypeInfo(rs, "INTERVAL YEAR", Types.OTHER, 18, "INTERVAL '", "' YEAR", "PRECISION", false, false,
                false, (short) 0, (short) 0, 0);
        testTypeInfo(rs, "INTERVAL MONTH", Types.OTHER, 18, "INTERVAL '", "' MONTH", "PRECISION", false, false,
                false, (short) 0, (short) 0, 0);
        testTypeInfo(rs, "INTERVAL DAY", Types.OTHER, 18, "INTERVAL '", "' DAY", "PRECISION", false, false,
                false, (short) 0, (short) 0, 0);
        testTypeInfo(rs, "INTERVAL HOUR", Types.OTHER, 18, "INTERVAL '", "' HOUR", "PRECISION", false, false,
                false, (short) 0, (short) 0, 0);
        testTypeInfo(rs, "INTERVAL MINUTE", Types.OTHER, 18, "INTERVAL '", "' MINUTE", "PRECISION", false, false,
                false, (short) 0, (short) 0, 0);
        testTypeInfo(rs, "INTERVAL SECOND", Types.OTHER, 18, "INTERVAL '", "' SECOND", "PRECISION,SCALE", false, false,
                false, (short) 0, (short) 9, 0);
        testTypeInfo(rs, "INTERVAL YEAR TO MONTH", Types.OTHER, 18, "INTERVAL '", "' YEAR TO MONTH", "PRECISION",
                false, false, false, (short) 0, (short) 0, 0);
        testTypeInfo(rs, "INTERVAL DAY TO HOUR", Types.OTHER, 18, "INTERVAL '", "' DAY TO HOUR", "PRECISION",
                false, false, false, (short) 0, (short) 0, 0);
        testTypeInfo(rs, "INTERVAL DAY TO MINUTE", Types.OTHER, 18, "INTERVAL '", "' DAY TO MINUTE", "PRECISION",
                false, false, false, (short) 0, (short) 0, 0);
        testTypeInfo(rs, "INTERVAL DAY TO SECOND", Types.OTHER, 18, "INTERVAL '", "' DAY TO SECOND", "PRECISION,SCALE",
                false, false, false, (short) 0, (short) 9, 0);
        testTypeInfo(rs, "INTERVAL HOUR TO MINUTE", Types.OTHER, 18, "INTERVAL '", "' HOUR TO MINUTE", "PRECISION",
                false, false, false, (short) 0, (short) 0, 0);
        testTypeInfo(rs, "INTERVAL HOUR TO SECOND", Types.OTHER, 18, "INTERVAL '", "' HOUR TO SECOND",
                "PRECISION,SCALE", false, false, false, (short) 0, (short) 9, 0);
        testTypeInfo(rs, "INTERVAL MINUTE TO SECOND", Types.OTHER, 18, "INTERVAL '", "' MINUTE TO SECOND",
                "PRECISION,SCALE", false, false, false, (short) 0, (short) 9, 0);
        testTypeInfo(rs, "ENUM", Types.OTHER, MAX_STRING_LENGTH, "'", "'", "ELEMENT [,...]", false, false, false,
                (short) 0, (short) 0, 0);
        testTypeInfo(rs, "GEOMETRY", Types.OTHER, Integer.MAX_VALUE, "'", "'", "TYPE,SRID", false, false, false,
                (short) 0, (short) 0, 0);
        testTypeInfo(rs, "JSON", Types.OTHER, MAX_STRING_LENGTH, "JSON '", "'", "LENGTH", true, false, false,
                (short) 0, (short) 0, 0);
        testTypeInfo(rs, "ROW", Types.OTHER, 0, "ROW(", ")", "NAME DATA_TYPE [,...]", false, false, false,
                (short) 0, (short) 0, 0);
        testTypeInfo(rs, "JAVA_OBJECT", Types.JAVA_OBJECT, MAX_STRING_LENGTH, "X'", "'", "LENGTH", false, false,
                false, (short) 0, (short) 0, 0);
        testTypeInfo(rs, "ARRAY", Types.ARRAY, MAX_ARRAY_CARDINALITY, "ARRAY[", "]", "CARDINALITY", false, false,
                false, (short) 0, (short) 0, 0);
        testTypeInfo(rs, "BINARY LARGE OBJECT", Types.BLOB, Integer.MAX_VALUE, "X'", "'", "LENGTH", false, false,
                false, (short) 0, (short) 0, 0);
        testTypeInfo(rs, "CHARACTER LARGE OBJECT", Types.CLOB, Integer.MAX_VALUE, "'", "'", "LENGTH", true, false,
                false, (short) 0, (short) 0, 0);
        testTypeInfo(rs, "TIME WITH TIME ZONE", Types.TIME_WITH_TIMEZONE, 24, "TIME WITH TIME ZONE '", "'", "SCALE",
                false, false, false, (short) 0, (short) 9, 0);
        testTypeInfo(rs, "TIMESTAMP WITH TIME ZONE", Types.TIMESTAMP_WITH_TIMEZONE, 35, "TIMESTAMP WITH TIME ZONE '",
                "'", "SCALE", false, false, false, (short) 0, (short) 9, 0);
        assertFalse(rs.next());
        conn.close();
    }

    private void testTypeInfo(ResultSet rs, String name, int type, long precision, String prefix, String suffix,
            String params, boolean caseSensitive, boolean fixed, boolean autoIncrement, short minScale, short maxScale,
            int radix) throws SQLException {
        assertTrue(rs.next());
        assertEquals(name, rs.getString(1));
        assertEquals(type, rs.getInt(2));
        assertEquals(precision, rs.getLong(3));
        assertEquals(prefix, rs.getString(4));
        assertEquals(suffix, rs.getString(5));
        assertEquals(params, rs.getString(6));
        assertEquals(DatabaseMetaData.typeNullable, rs.getShort(7));
        assertEquals(caseSensitive, rs.getBoolean(8));
        assertEquals(DatabaseMetaData.typeSearchable, rs.getShort(9));
        assertFalse(rs.getBoolean(10));
        assertEquals(fixed, rs.getBoolean(11));
        assertEquals(autoIncrement, rs.getBoolean(12));
        assertEquals(name, rs.getString(13));
        assertEquals(minScale, rs.getShort(14));
        assertEquals(maxScale, rs.getShort(15));
        rs.getInt(16);
        assertTrue(rs.wasNull());
        rs.getInt(17);
        assertTrue(rs.wasNull());
        if (radix != 0) {
            assertEquals(radix, rs.getInt(18));
        } else {
            rs.getInt(18);
            assertTrue(rs.wasNull());
        }
    }

    private void testUDTs() throws SQLException {
        Connection conn = getConnection("metaData");
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet rs;
        rs = meta.getUDTs(null, null, null, null);
        assertResultSetMeta(rs, 7,
                new String[] { "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
                        "CLASS_NAME", "DATA_TYPE", "REMARKS", "BASE_TYPE" },
                new int[] { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.INTEGER, Types.VARCHAR,
                        Types.SMALLINT }, null, null);
        conn.close();
    }

    private void testCrossReferences() throws SQLException {
        Connection conn = getConnection("metaData");
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet rs;
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE PARENT(A INT, B INT, PRIMARY KEY(A, B))");
        stat.execute("CREATE TABLE CHILD(ID INT PRIMARY KEY, PA INT, PB INT, " +
                "CONSTRAINT AB FOREIGN KEY(PA, PB) REFERENCES PARENT(A, B))");
        rs = meta.getCrossReference(null, "PUBLIC", "PARENT", null, "PUBLIC", "CHILD");
        checkCrossRef(rs);
        rs = meta.getImportedKeys(null, "PUBLIC", "CHILD");
        checkCrossRef(rs);
        rs = meta.getExportedKeys(null, "PUBLIC", "PARENT");
        checkCrossRef(rs);
        stat.execute("DROP TABLE PARENT, CHILD");
        conn.close();
    }

    private void checkCrossRef(ResultSet rs) throws SQLException {
        assertResultSetMeta(rs, 14, new String[] { "PKTABLE_CAT",
                "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
                "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME",
                "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE",
                "FK_NAME", "PK_NAME", "DEFERRABILITY" }, new int[] {
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.SMALLINT, Types.SMALLINT, Types.SMALLINT, Types.VARCHAR,
                Types.VARCHAR, Types.SMALLINT }, null, null);
        assertResultSetOrdered(rs, new String[][] {
                { CATALOG, Constants.SCHEMA_MAIN, "PARENT", "A", CATALOG,
                        Constants.SCHEMA_MAIN, "CHILD", "PA", "1",
                        "" + DatabaseMetaData.importedKeyRestrict,
                        "" + DatabaseMetaData.importedKeyRestrict, "AB",
                        "CONSTRAINT_8",
                        "" + DatabaseMetaData.importedKeyNotDeferrable },
                { CATALOG, Constants.SCHEMA_MAIN, "PARENT", "B", CATALOG,
                        Constants.SCHEMA_MAIN, "CHILD", "PB", "2",
                        "" + DatabaseMetaData.importedKeyRestrict,
                        "" + DatabaseMetaData.importedKeyRestrict, "AB",
                        "CONSTRAINT_8",
                        "" + DatabaseMetaData.importedKeyNotDeferrable } });
    }

    private void testTempTable() throws SQLException {
        Connection conn = getConnection("metaData");
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST_TEMP");
        stat.execute("CREATE TEMP TABLE TEST_TEMP" +
                "(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        stat.execute("CREATE INDEX IDX_NAME ON TEST_TEMP(NAME)");
        stat.execute("ALTER TABLE TEST_TEMP ADD FOREIGN KEY(ID) REFERENCES(ID)");
        conn.close();

        conn = getConnection("metaData");
        stat = conn.createStatement();
        stat.execute("CREATE TEMP TABLE TEST_TEMP" +
                "(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        ResultSet rs = stat.executeQuery("SELECT STORAGE_TYPE FROM "
                + "INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='TEST_TEMP'");
        rs.next();
        assertEquals("GLOBAL TEMPORARY", rs.getString("STORAGE_TYPE"));
        stat.execute("DROP TABLE IF EXISTS TEST_TEMP");
        conn.close();
    }

    private void testStatic() throws SQLException {
        Driver dr = org.h2.Driver.load();
        Connection conn = getConnection("metaData");
        DatabaseMetaData meta = conn.getMetaData();

        assertEquals(dr.getMajorVersion(), meta.getDriverMajorVersion());
        assertEquals(dr.getMinorVersion(), meta.getDriverMinorVersion());
        assertTrue(dr.jdbcCompliant());

        assertEquals(0, dr.getPropertyInfo(null, null).length);
        assertNull(dr.connect("jdbc:test:false", null));

        assertTrue(meta.getNumericFunctions().length() > 0);
        assertTrue(meta.getStringFunctions().length() > 0);
        assertTrue(meta.getSystemFunctions().length() > 0);
        assertTrue(meta.getTimeDateFunctions().length() > 0);

        assertTrue(meta.allProceduresAreCallable());
        assertTrue(meta.allTablesAreSelectable());
        assertTrue(meta.dataDefinitionCausesTransactionCommit());
        assertFalse(meta.dataDefinitionIgnoredInTransactions());
        assertFalse(meta.deletesAreDetected(ResultSet.TYPE_FORWARD_ONLY));
        assertFalse(meta.deletesAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertFalse(meta.deletesAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE));
        assertFalse(meta.doesMaxRowSizeIncludeBlobs());
        assertEquals(".", meta.getCatalogSeparator());
        assertEquals("catalog", meta.getCatalogTerm());
        assertTrue(meta.getConnection() == conn);
        String versionStart = meta.getDatabaseMajorVersion() + "." +
                meta.getDatabaseMinorVersion();
        assertTrue(meta.getDatabaseProductVersion().startsWith(versionStart));
        assertEquals(meta.getDatabaseMajorVersion(),
                meta.getDriverMajorVersion());
        assertEquals(meta.getDatabaseMinorVersion(),
                meta.getDriverMinorVersion());
        int majorVersion = 4;
        assertEquals(majorVersion, meta.getJDBCMajorVersion());
        assertEquals(2, meta.getJDBCMinorVersion());
        assertEquals("H2", meta.getDatabaseProductName());
        assertEquals(Connection.TRANSACTION_READ_COMMITTED,
                meta.getDefaultTransactionIsolation());
        assertEquals("H2 JDBC Driver", meta.getDriverName());

        versionStart = meta.getDriverMajorVersion() + "." +
                meta.getDriverMinorVersion();
        assertTrue(meta.getDriverVersion().startsWith(versionStart));
        assertEquals("", meta.getExtraNameCharacters());
        assertEquals("\"", meta.getIdentifierQuoteString());
        assertEquals(0, meta.getMaxBinaryLiteralLength());
        assertEquals(0, meta.getMaxCatalogNameLength());
        assertEquals(0, meta.getMaxCharLiteralLength());
        assertEquals(0, meta.getMaxColumnNameLength());
        assertEquals(0, meta.getMaxColumnsInGroupBy());
        assertEquals(0, meta.getMaxColumnsInIndex());
        assertEquals(0, meta.getMaxColumnsInOrderBy());
        assertEquals(0, meta.getMaxColumnsInSelect());
        assertEquals(0, meta.getMaxColumnsInTable());
        assertEquals(0, meta.getMaxConnections());
        assertEquals(0, meta.getMaxCursorNameLength());
        assertEquals(0, meta.getMaxIndexLength());
        assertEquals(0, meta.getMaxProcedureNameLength());
        assertEquals(0, meta.getMaxRowSize());
        assertEquals(0, meta.getMaxSchemaNameLength());
        assertEquals(0, meta.getMaxStatementLength());
        assertEquals(0, meta.getMaxStatements());
        assertEquals(0, meta.getMaxTableNameLength());
        assertEquals(0, meta.getMaxTablesInSelect());
        assertEquals(0, meta.getMaxUserNameLength());
        assertEquals("procedure", meta.getProcedureTerm());

        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT,
                meta.getResultSetHoldability());
        assertEquals(DatabaseMetaData.sqlStateSQL, meta.getSQLStateType());
        assertFalse(meta.locatorsUpdateCopy());

        assertEquals("schema", meta.getSchemaTerm());
        assertEquals("\\", meta.getSearchStringEscape());

        assertTrue(meta.getURL().startsWith("jdbc:h2:"));
        assertTrue(meta.getUserName().length() > 1);
        assertFalse(meta.insertsAreDetected(
                ResultSet.TYPE_FORWARD_ONLY));
        assertFalse(meta.insertsAreDetected(
                ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertFalse(meta.insertsAreDetected(
                ResultSet.TYPE_SCROLL_SENSITIVE));
        assertTrue(meta.isCatalogAtStart());
        assertFalse(meta.isReadOnly());
        assertTrue(meta.nullPlusNonNullIsNull());
        assertFalse(meta.othersDeletesAreVisible(
                ResultSet.TYPE_FORWARD_ONLY));
        assertFalse(meta.othersDeletesAreVisible(
                ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertFalse(meta.othersDeletesAreVisible(
                ResultSet.TYPE_SCROLL_SENSITIVE));
        assertFalse(meta.othersInsertsAreVisible(
                ResultSet.TYPE_FORWARD_ONLY));
        assertFalse(meta.othersInsertsAreVisible(
                ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertFalse(meta.othersInsertsAreVisible(
                ResultSet.TYPE_SCROLL_SENSITIVE));
        assertFalse(meta.othersUpdatesAreVisible(
                ResultSet.TYPE_FORWARD_ONLY));
        assertFalse(meta.othersUpdatesAreVisible(
                ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertFalse(meta.othersUpdatesAreVisible(
                ResultSet.TYPE_SCROLL_SENSITIVE));
        assertFalse(meta.ownDeletesAreVisible(
                ResultSet.TYPE_FORWARD_ONLY));
        assertFalse(meta.ownDeletesAreVisible(
                ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertFalse(meta.ownDeletesAreVisible(
                ResultSet.TYPE_SCROLL_SENSITIVE));
        assertFalse(meta.ownInsertsAreVisible(
                ResultSet.TYPE_FORWARD_ONLY));
        assertFalse(meta.ownInsertsAreVisible(
                ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertFalse(meta.ownInsertsAreVisible(
                ResultSet.TYPE_SCROLL_SENSITIVE));
        assertTrue(meta.ownUpdatesAreVisible(
                ResultSet.TYPE_FORWARD_ONLY));
        assertTrue(meta.ownUpdatesAreVisible(
                ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertTrue(meta.ownUpdatesAreVisible(
                ResultSet.TYPE_SCROLL_SENSITIVE));
        assertFalse(meta.storesLowerCaseIdentifiers());
        assertFalse(meta.storesLowerCaseQuotedIdentifiers());
        assertFalse(meta.storesMixedCaseIdentifiers());
        assertFalse(meta.storesMixedCaseQuotedIdentifiers());
        assertTrue(meta.storesUpperCaseIdentifiers());
        assertFalse(meta.storesUpperCaseQuotedIdentifiers());
        assertTrue(meta.supportsAlterTableWithAddColumn());
        assertTrue(meta.supportsAlterTableWithDropColumn());
        assertTrue(meta.supportsANSI92EntryLevelSQL());
        assertFalse(meta.supportsANSI92IntermediateSQL());
        assertFalse(meta.supportsANSI92FullSQL());
        assertTrue(meta.supportsBatchUpdates());
        assertTrue(meta.supportsCatalogsInDataManipulation());
        assertTrue(meta.supportsCatalogsInIndexDefinitions());
        assertTrue(meta.supportsCatalogsInPrivilegeDefinitions());
        assertFalse(meta.supportsCatalogsInProcedureCalls());
        assertTrue(meta.supportsCatalogsInTableDefinitions());
        assertTrue(meta.supportsColumnAliasing());
        assertTrue(meta.supportsConvert());
        assertTrue(meta.supportsConvert(Types.INTEGER, Types.VARCHAR));
        assertTrue(meta.supportsCoreSQLGrammar());
        assertTrue(meta.supportsCorrelatedSubqueries());
        assertFalse(meta.supportsDataDefinitionAndDataManipulationTransactions());
        assertTrue(meta.supportsDataManipulationTransactionsOnly());
        assertFalse(meta.supportsDifferentTableCorrelationNames());
        assertTrue(meta.supportsExpressionsInOrderBy());
        assertFalse(meta.supportsExtendedSQLGrammar());
        assertFalse(meta.supportsFullOuterJoins());

        assertTrue(meta.supportsGetGeneratedKeys());
        assertFalse(meta.supportsMultipleOpenResults());
        assertFalse(meta.supportsNamedParameters());

        assertTrue(meta.supportsGroupBy());
        assertTrue(meta.supportsGroupByBeyondSelect());
        assertTrue(meta.supportsGroupByUnrelated());
        assertTrue(meta.supportsIntegrityEnhancementFacility());
        assertTrue(meta.supportsLikeEscapeClause());
        assertTrue(meta.supportsLimitedOuterJoins());
        assertTrue(meta.supportsMinimumSQLGrammar());
        assertFalse(meta.supportsMixedCaseIdentifiers());
        assertTrue(meta.supportsMixedCaseQuotedIdentifiers());
        assertFalse(meta.supportsMultipleResultSets());
        assertTrue(meta.supportsMultipleTransactions());
        assertTrue(meta.supportsNonNullableColumns());
        assertFalse(meta.supportsOpenCursorsAcrossCommit());
        assertFalse(meta.supportsOpenCursorsAcrossRollback());
        assertTrue(meta.supportsOpenStatementsAcrossCommit());
        assertTrue(meta.supportsOpenStatementsAcrossRollback());
        assertTrue(meta.supportsOrderByUnrelated());
        assertTrue(meta.supportsOuterJoins());
        assertFalse(meta.supportsPositionedDelete());
        assertFalse(meta.supportsPositionedUpdate());
        assertTrue(meta.supportsResultSetConcurrency(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        assertTrue(meta.supportsResultSetConcurrency(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));
        assertTrue(meta.supportsResultSetConcurrency(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));
        assertTrue(meta.supportsResultSetConcurrency(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE));
        assertFalse(meta.supportsResultSetConcurrency(
                ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY));
        assertFalse(meta.supportsResultSetConcurrency(
                ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE));

        assertFalse(meta.supportsResultSetHoldability(
                ResultSet.HOLD_CURSORS_OVER_COMMIT));
        assertTrue(meta.supportsResultSetHoldability(
                ResultSet.CLOSE_CURSORS_AT_COMMIT));
        assertTrue(meta.supportsSavepoints());
        assertFalse(meta.supportsStatementPooling());

        assertTrue(meta.supportsResultSetType(
                ResultSet.TYPE_FORWARD_ONLY));
        assertTrue(meta.supportsResultSetType(
                ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertFalse(meta.supportsResultSetType(
                ResultSet.TYPE_SCROLL_SENSITIVE));
        assertTrue(meta.supportsSchemasInDataManipulation());
        assertTrue(meta.supportsSchemasInIndexDefinitions());
        assertTrue(meta.supportsSchemasInPrivilegeDefinitions());
        assertTrue(meta.supportsSchemasInProcedureCalls());
        assertTrue(meta.supportsSchemasInTableDefinitions());
        assertTrue(meta.supportsSelectForUpdate());
        assertFalse(meta.supportsStoredProcedures());
        assertTrue(meta.supportsSubqueriesInComparisons());
        assertTrue(meta.supportsSubqueriesInExists());
        assertTrue(meta.supportsSubqueriesInIns());
        assertTrue(meta.supportsSubqueriesInQuantifieds());
        assertTrue(meta.supportsTableCorrelationNames());
        assertTrue(meta.supportsTransactions());
        assertFalse(meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_NONE));
        assertTrue(meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED));
        assertTrue(meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED));
        assertTrue(meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ));
        assertTrue(meta.supportsTransactionIsolationLevel(Constants.TRANSACTION_SNAPSHOT));
        assertTrue(meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE));
        assertTrue(meta.supportsUnion());
        assertTrue(meta.supportsUnionAll());
        assertFalse(meta.updatesAreDetected(ResultSet.TYPE_FORWARD_ONLY));
        assertFalse(meta.updatesAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertFalse(meta.updatesAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE));
        assertFalse(meta.usesLocalFilePerTable());
        assertTrue(meta.usesLocalFiles());
        conn.close();
    }

    private void testNullsAreSortedAt() throws SQLException {
        Connection conn = getConnection("metaData");
        Statement stat = conn.createStatement();
        DatabaseMetaData meta = conn.getMetaData();
        testNullsAreSortedAt(meta, DefaultNullOrdering.LOW);
        stat.execute("SET DEFAULT_NULL_ORDERING LOW");
        testNullsAreSortedAt(meta, DefaultNullOrdering.LOW);
        stat.execute("SET DEFAULT_NULL_ORDERING HIGH");
        testNullsAreSortedAt(meta, DefaultNullOrdering.HIGH);
        stat.execute("SET DEFAULT_NULL_ORDERING FIRST");
        testNullsAreSortedAt(meta, DefaultNullOrdering.FIRST);
        stat.execute("SET DEFAULT_NULL_ORDERING LAST");
        testNullsAreSortedAt(meta, DefaultNullOrdering.LAST);
        stat.execute("SET DEFAULT_NULL_ORDERING LOW");
        conn.close();
    }

    private void testNullsAreSortedAt(DatabaseMetaData meta, DefaultNullOrdering ordering) throws SQLException {
        assertEquals(ordering == DefaultNullOrdering.HIGH, meta.nullsAreSortedHigh());
        assertEquals(ordering == DefaultNullOrdering.LOW, meta.nullsAreSortedLow());
        assertEquals(ordering == DefaultNullOrdering.FIRST, meta.nullsAreSortedAtStart());
        assertEquals(ordering == DefaultNullOrdering.LAST, meta.nullsAreSortedAtEnd());
    }

    private void testMore() throws SQLException {
        Connection conn = getConnection("metaData");
        DatabaseMetaData meta = conn.getMetaData();
        Statement stat = conn.createStatement();
        ResultSet rs;

        conn.setReadOnly(true);
        conn.setReadOnly(false);
        assertFalse(conn.isReadOnly());
        assertTrue(conn.isReadOnly() == meta.isReadOnly());

        assertTrue(conn == meta.getConnection());

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
        int count = (meta.nullsAreSortedHigh() ? 1 : 0) +
                (meta.nullsAreSortedLow() ? 1 : 0) +
                (meta.nullsAreSortedAtStart() ? 1 : 0) +
                (meta.nullsAreSortedAtEnd() ? 1 : 0);
        assertTrue(count == 1);

        trace("meta.allProceduresAreCallable:" +
                meta.allProceduresAreCallable());
        assertTrue(meta.allProceduresAreCallable());

        trace("meta.allTablesAreSelectable:" + meta.allTablesAreSelectable());
        assertTrue(meta.allTablesAreSelectable());

        trace("getTables");
        rs = meta.getTables(null, Constants.SCHEMA_MAIN, null,
                new String[] { "TABLE" });
        assertResultSetMeta(rs, 10, new String[] { "TABLE_CAT", "TABLE_SCHEM",
                "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT",
                "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME",
                "REF_GENERATION" }, new int[] { Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR }, null, null);
        if (rs.next()) {
            fail("Database is not empty after dropping all tables");
        }
        stat.executeUpdate("CREATE TABLE TEST(" + "ID INT PRIMARY KEY,"
                + "TEXT_V VARCHAR(120)," + "DEC_V DECIMAL(12,3)," + "NUM_V NUMERIC(12,3),"
                + "DATE_V DATETIME," + "BLOB_V BLOB," + "CLOB_V CLOB" + ")");
        rs = meta.getTables(null, Constants.SCHEMA_MAIN, null,
                new String[] { "TABLE" });
        assertResultSetOrdered(rs, new String[][] { { CATALOG,
                Constants.SCHEMA_MAIN, "TEST", "BASE TABLE" } });
        trace("getColumns");
        rs = meta.getColumns(null, null, "TEST", null);
        assertResultSetMeta(rs, 24, new String[] { "TABLE_CAT", "TABLE_SCHEM",
                "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
                "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS",
                "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF",
                "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG",
                "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE",
                "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN" }, new int[] {
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.INTEGER,
                Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.VARCHAR,
                Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.INTEGER,
                Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.SMALLINT, Types.VARCHAR, Types.VARCHAR },
                null, null);
        assertResultSetOrdered(rs, new String[][] {
                { CATALOG, Constants.SCHEMA_MAIN, "TEST", "ID",
                        "" + Types.INTEGER, "INTEGER", "32", null, "0", "2",
                        "" + DatabaseMetaData.columnNoNulls, null, null,
                        null, null, "32", "1", "NO" },
                { CATALOG, Constants.SCHEMA_MAIN, "TEST", "TEXT_V",
                        "" + Types.VARCHAR, "CHARACTER VARYING", "120", null, "0", null,
                        "" + DatabaseMetaData.columnNullable, null, null,
                        null, null, "120", "2", "YES" },
                { CATALOG, Constants.SCHEMA_MAIN, "TEST", "DEC_V",
                        "" + Types.DECIMAL, "DECIMAL", "12", null, "3", "10",
                        "" + DatabaseMetaData.columnNullable, null, null,
                        null, null, "12", "3", "YES" },
                { CATALOG, Constants.SCHEMA_MAIN, "TEST", "NUM_V",
                            "" + Types.NUMERIC, "NUMERIC", "12", null, "3", "10",
                            "" + DatabaseMetaData.columnNullable, null, null,
                            null, null, "12", "4", "YES" },
                { CATALOG, Constants.SCHEMA_MAIN, "TEST", "DATE_V",
                        "" + Types.TIMESTAMP, "TIMESTAMP", "26", null, "6", null,
                        "" + DatabaseMetaData.columnNullable, null, null,
                        null, null, "26", "5", "YES" },
                { CATALOG, Constants.SCHEMA_MAIN, "TEST", "BLOB_V",
                        "" + Types.BLOB, "BINARY LARGE OBJECT", "" + Integer.MAX_VALUE, null, "0", null,
                        "" + DatabaseMetaData.columnNullable, null, null,
                        null, null, "" + Integer.MAX_VALUE, "6",
                        "YES" },
                { CATALOG, Constants.SCHEMA_MAIN, "TEST", "CLOB_V",
                        "" + Types.CLOB, "CHARACTER LARGE OBJECT", "" + Integer.MAX_VALUE, null, "0", null,
                        "" + DatabaseMetaData.columnNullable, null, null,
                        null, null, "" + Integer.MAX_VALUE, "7",
                        "YES" } });
        /*
         * rs=meta.getColumns(null,null,"TEST",null); while(rs.next()) { int
         * datatype=rs.getInt(5); }
         */
        trace("getIndexInfo");
        stat.executeUpdate("CREATE INDEX IDX_TEXT_DEC ON TEST(TEXT_V,DEC_V)");
        stat.executeUpdate("CREATE UNIQUE INDEX IDX_DATE ON TEST(DATE_V)");
        rs = meta.getIndexInfo(null, null, "TEST", false, false);
        assertResultSetMeta(rs, 13, new String[] { "TABLE_CAT", "TABLE_SCHEM",
                "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME",
                "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC",
                "CARDINALITY", "PAGES", "FILTER_CONDITION" },
                new int[] { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.BOOLEAN, Types.VARCHAR, Types.VARCHAR,
                        Types.SMALLINT, Types.SMALLINT, Types.VARCHAR,
                        Types.VARCHAR, Types.BIGINT, Types.BIGINT,
                        Types.VARCHAR }, null, null);
        assertResultSetOrdered(rs, new String[][] {
                { CATALOG, Constants.SCHEMA_MAIN, "TEST", "FALSE", CATALOG,
                        "IDX_DATE", "" + DatabaseMetaData.tableIndexOther, "1",
                        "DATE_V", "A", "0", "0" },
                { CATALOG, Constants.SCHEMA_MAIN, "TEST", "FALSE", CATALOG,
                        "PRIMARY_KEY_2", "" + DatabaseMetaData.tableIndexOther,
                        "1", "ID", "A", "0", "0" },
                { CATALOG, Constants.SCHEMA_MAIN, "TEST", "TRUE", CATALOG,
                        "IDX_TEXT_DEC", "" + DatabaseMetaData.tableIndexOther,
                        "1", "TEXT_V", "A", "0", "0" },
                { CATALOG, Constants.SCHEMA_MAIN, "TEST", "TRUE", CATALOG,
                        "IDX_TEXT_DEC", "" + DatabaseMetaData.tableIndexOther,
                        "2", "DEC_V", "A", "0", "0" }, },
                new int[] { 11 });
        stat.executeUpdate("DROP INDEX IDX_TEXT_DEC");
        stat.executeUpdate("DROP INDEX IDX_DATE");
        rs = meta.getIndexInfo(null, null, "TEST", false, false);
        assertResultSetMeta(rs, 13, new String[] { "TABLE_CAT", "TABLE_SCHEM",
                "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME",
                "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC",
                "CARDINALITY", "PAGES", "FILTER_CONDITION" },
                new int[] { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.BOOLEAN, Types.VARCHAR, Types.VARCHAR,
                        Types.SMALLINT, Types.SMALLINT, Types.VARCHAR,
                        Types.VARCHAR, Types.BIGINT, Types.BIGINT,
                        Types.VARCHAR }, null, null);
        assertResultSetOrdered(rs, new String[][] { { CATALOG,
                Constants.SCHEMA_MAIN, "TEST", "FALSE", CATALOG,
                "PRIMARY_KEY_2", "" + DatabaseMetaData.tableIndexOther, "1",
                "ID", "A", "0", "0" } },
                new int[] { 11 });
        trace("getPrimaryKeys");
        rs = meta.getPrimaryKeys(null, null, "TEST");
        assertResultSetMeta(rs, 6, new String[] { "TABLE_CAT", "TABLE_SCHEM",
                "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME" }, new int[] {
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.SMALLINT, Types.VARCHAR }, null, null);
        assertResultSetOrdered(rs, new String[][] { { CATALOG,
                Constants.SCHEMA_MAIN, "TEST", "ID", "1", "CONSTRAINT_2" }, });
        trace("getTables - using a wildcard");
        stat.executeUpdate(
                "CREATE TABLE T_2(B INT,A VARCHAR(6),C INT,PRIMARY KEY(C,A,B))");
        stat.executeUpdate(
                "CREATE TABLE TX2(B INT,A VARCHAR(6),C INT,PRIMARY KEY(C,A,B))");
        rs = meta.getTables(null, null, "T_2", null);
        assertResultSetOrdered(rs, new String[][] {
                { CATALOG, Constants.SCHEMA_MAIN, "TX2", "BASE TABLE" },
                { CATALOG, Constants.SCHEMA_MAIN, "T_2", "BASE TABLE" } });
        trace("getTables - using a quoted _ character");
        rs = meta.getTables(null, null, "T\\_2", null);
        assertResultSetOrdered(rs, new String[][] { { CATALOG,
                Constants.SCHEMA_MAIN, "T_2", "BASE TABLE" } });
        trace("getTables - using the % wildcard");
        rs = meta.getTables(null, Constants.SCHEMA_MAIN, "%",
                new String[] { "TABLE" });
        assertResultSetOrdered(rs, new String[][] {
                { CATALOG, Constants.SCHEMA_MAIN, "TEST", "BASE TABLE" },
                { CATALOG, Constants.SCHEMA_MAIN, "TX2", "BASE TABLE" },
                { CATALOG, Constants.SCHEMA_MAIN, "T_2", "BASE TABLE" } });
        stat.execute("DROP TABLE TEST");

        trace("getColumns - using wildcards");
        rs = meta.getColumns(null, null, "___", "B%");
        assertResultSetOrdered(rs, new String[][] {
                { CATALOG, Constants.SCHEMA_MAIN, "TX2", "B",
                        "" + Types.INTEGER, "INTEGER", "32" },
                { CATALOG, Constants.SCHEMA_MAIN, "T_2", "B",
                        "" + Types.INTEGER, "INTEGER", "32" }, });
        trace("getColumns - using wildcards");
        rs = meta.getColumns(null, null, "_\\__", "%");
        assertResultSetOrdered(rs, new String[][] {
                { CATALOG, Constants.SCHEMA_MAIN, "T_2", "B",
                        "" + Types.INTEGER, "INTEGER", "32" },
                { CATALOG, Constants.SCHEMA_MAIN, "T_2", "A",
                        "" + Types.VARCHAR, "CHARACTER VARYING", "6" },
                { CATALOG, Constants.SCHEMA_MAIN, "T_2", "C",
                        "" + Types.INTEGER, "INTEGER", "32" }, });
        trace("getIndexInfo");
        stat.executeUpdate("CREATE UNIQUE INDEX A_INDEX ON TX2(B,C,A)");
        stat.executeUpdate("CREATE INDEX B_INDEX ON TX2(A,B,C)");
        rs = meta.getIndexInfo(null, null, "TX2", false, false);
        assertResultSetOrdered(rs, new String[][] {
                { CATALOG, Constants.SCHEMA_MAIN, "TX2", "FALSE", CATALOG,
                        "A_INDEX", "" + DatabaseMetaData.tableIndexOther, "1",
                        "B", "A" },
                { CATALOG, Constants.SCHEMA_MAIN, "TX2", "FALSE", CATALOG,
                        "A_INDEX", "" + DatabaseMetaData.tableIndexOther, "2",
                        "C", "A" },
                { CATALOG, Constants.SCHEMA_MAIN, "TX2", "FALSE", CATALOG,
                        "A_INDEX", "" + DatabaseMetaData.tableIndexOther, "3",
                        "A", "A" },
                { CATALOG, Constants.SCHEMA_MAIN, "TX2", "FALSE", CATALOG,
                        "PRIMARY_KEY_14",
                        "" + DatabaseMetaData.tableIndexOther, "1", "C", "A" },
                { CATALOG, Constants.SCHEMA_MAIN, "TX2", "FALSE", CATALOG,
                        "PRIMARY_KEY_14",
                        "" + DatabaseMetaData.tableIndexOther, "2", "A", "A" },
                { CATALOG, Constants.SCHEMA_MAIN, "TX2", "FALSE", CATALOG,
                        "PRIMARY_KEY_14",
                        "" + DatabaseMetaData.tableIndexOther, "3", "B", "A" },
                { CATALOG, Constants.SCHEMA_MAIN, "TX2", "TRUE", CATALOG,
                        "B_INDEX", "" + DatabaseMetaData.tableIndexOther, "1",
                        "A", "A" },
                { CATALOG, Constants.SCHEMA_MAIN, "TX2", "TRUE", CATALOG,
                        "B_INDEX", "" + DatabaseMetaData.tableIndexOther, "2",
                        "B", "A" },
                { CATALOG, Constants.SCHEMA_MAIN, "TX2", "TRUE", CATALOG,
                        "B_INDEX", "" + DatabaseMetaData.tableIndexOther, "3",
                        "C", "A" }, },
                new int[] { 11 });
        trace("getPrimaryKeys");
        rs = meta.getPrimaryKeys(null, null, "T_2");
        assertResultSetOrdered(rs, new String[][] {
                { CATALOG, Constants.SCHEMA_MAIN, "T_2", "A", "2", "CONSTRAINT_1" },
                { CATALOG, Constants.SCHEMA_MAIN, "T_2", "B", "3", "CONSTRAINT_1" },
                { CATALOG, Constants.SCHEMA_MAIN, "T_2", "C", "1", "CONSTRAINT_1" }, });
        stat.executeUpdate("DROP TABLE TX2");
        stat.executeUpdate("DROP TABLE T_2");
        stat.executeUpdate("CREATE TABLE PARENT(ID INT PRIMARY KEY)");
        stat.executeUpdate("CREATE TABLE CHILD(P_ID INT,ID INT," +
                "PRIMARY KEY(P_ID,ID),FOREIGN KEY(P_ID) REFERENCES PARENT(ID))");

        trace("getImportedKeys");
        rs = meta.getImportedKeys(null, null, "CHILD");
        assertResultSetMeta(rs, 14, new String[] { "PKTABLE_CAT",
                "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
                "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME",
                "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE",
                "FK_NAME", "PK_NAME", "DEFERRABILITY" }, new int[] {
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.SMALLINT, Types.SMALLINT, Types.SMALLINT, Types.VARCHAR,
                Types.VARCHAR, Types.SMALLINT }, null, null);
        // TODO test
        // testResultSetOrdered(rs, new String[][] { { null, null, "PARENT",
        // "ID",
        // null, null, "CHILD", "P_ID", "1",
        // "" + DatabaseMetaData.importedKeyNoAction,
        // "" + DatabaseMetaData.importedKeyNoAction, "FK_1", null,
        // "" + DatabaseMetaData.importedKeyNotDeferrable}});

        trace("getExportedKeys");
        rs = meta.getExportedKeys(null, null, "PARENT");
        assertResultSetMeta(rs, 14, new String[] { "PKTABLE_CAT",
                "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
                "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME",
                "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE",
                "FK_NAME", "PK_NAME", "DEFERRABILITY" }, new int[] {
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.SMALLINT, Types.SMALLINT, Types.SMALLINT, Types.VARCHAR,
                Types.VARCHAR, Types.SMALLINT }, null, null);
        // TODO test
        /*
         * testResultSetOrdered(rs, new String[][]{ { null,null,"PARENT","ID",
         * null,null,"CHILD","P_ID",
         * "1",""+DatabaseMetaData.importedKeyNoAction,
         * ""+DatabaseMetaData.importedKeyNoAction,
         * null,null,""+DatabaseMetaData.importedKeyNotDeferrable } } );
         */
        trace("getCrossReference");
        rs = meta.getCrossReference(null, null, "PARENT", null, null, "CHILD");
        assertResultSetMeta(rs, 14, new String[] { "PKTABLE_CAT",
                "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
                "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME",
                "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE",
                "FK_NAME", "PK_NAME", "DEFERRABILITY" }, new int[] {
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.SMALLINT, Types.SMALLINT, Types.SMALLINT, Types.VARCHAR,
                Types.VARCHAR, Types.SMALLINT }, null, null);
        // TODO test
        /*
         * testResultSetOrdered(rs, new String[][]{ { null,null,"PARENT","ID",
         * null,null,"CHILD","P_ID",
         * "1",""+DatabaseMetaData.importedKeyNoAction,
         * ""+DatabaseMetaData.importedKeyNoAction,
         * null,null,""+DatabaseMetaData.importedKeyNotDeferrable } } );
         */

        rs = meta.getSchemas();
        assertResultSetMeta(rs, 2, new String[] { "TABLE_SCHEM", "TABLE_CATALOG" },
                new int[] { Types.VARCHAR, Types.VARCHAR }, null, null);
        assertTrue(rs.next());
        assertEquals("INFORMATION_SCHEMA", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("PUBLIC", rs.getString(1));
        assertFalse(rs.next());

        rs = meta.getSchemas(null, null);
        assertResultSetMeta(rs, 2, new String[] { "TABLE_SCHEM", "TABLE_CATALOG" },
                new int[] { Types.VARCHAR, Types.VARCHAR }, null, null);
        assertTrue(rs.next());
        assertEquals("INFORMATION_SCHEMA", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("PUBLIC", rs.getString(1));
        assertFalse(rs.next());

        rs = meta.getCatalogs();
        assertResultSetMeta(rs, 1, new String[] { "TABLE_CAT" },
                new int[] { Types.VARCHAR }, null, null);
        assertResultSetOrdered(rs, new String[][] { { CATALOG } });

        rs = meta.getTableTypes();
        assertResultSetMeta(rs, 1, new String[] { "TABLE_TYPE" },
                new int[] { Types.VARCHAR }, null, null);
        assertResultSetOrdered(rs, new String[][] {
                { "BASE TABLE" }, { "GLOBAL TEMPORARY" },
                { "LOCAL TEMPORARY" }, { "SYNONYM" }, { "VIEW" } });

        rs = meta.getTypeInfo();
        assertResultSetMeta(rs, 18, new String[] { "TYPE_NAME", "DATA_TYPE",
                "PRECISION", "LITERAL_PREFIX", "LITERAL_SUFFIX",
                "CREATE_PARAMS", "NULLABLE", "CASE_SENSITIVE", "SEARCHABLE",
                "UNSIGNED_ATTRIBUTE", "FIXED_PREC_SCALE", "AUTO_INCREMENT",
                "LOCAL_TYPE_NAME", "MINIMUM_SCALE", "MAXIMUM_SCALE",
                "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "NUM_PREC_RADIX" },
                new int[] { Types.VARCHAR, Types.INTEGER, Types.INTEGER,
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.SMALLINT, Types.BOOLEAN, Types.SMALLINT,
                        Types.BOOLEAN, Types.BOOLEAN, Types.BOOLEAN,
                        Types.VARCHAR, Types.SMALLINT, Types.SMALLINT,
                        Types.INTEGER, Types.INTEGER, Types.INTEGER }, null,
                null);

        rs = meta.getTablePrivileges(null, null, null);
        assertResultSetMeta(rs, 7,
                new String[] { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
                        "GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE" },
                new int[] { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR }, null, null);

        rs = meta.getColumnPrivileges(null, null, "TEST", null);
        assertResultSetMeta(rs, 8, new String[] { "TABLE_CAT", "TABLE_SCHEM",
                "TABLE_NAME", "COLUMN_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE",
                "IS_GRANTABLE" }, new int[] { Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR }, null, null);

        assertNull(conn.getWarnings());
        conn.clearWarnings();
        assertNull(conn.getWarnings());
        conn.close();
    }

    private void testGeneral() throws SQLException {
        Connection conn = getConnection("metaData");
        DatabaseMetaData meta = conn.getMetaData();

        Statement stat = conn.createStatement();

        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        stat.execute("CREATE INDEX IDXNAME ON TEST(NAME)");

        ResultSet rs;

        rs = meta.getCatalogs();
        rs.next();
        assertEquals(CATALOG, rs.getString(1));
        assertFalse(rs.next());

        rs = meta.getSchemas();
        rs.next();
        assertEquals("INFORMATION_SCHEMA", rs.getString("TABLE_SCHEM"));
        rs.next();
        assertEquals("PUBLIC", rs.getString("TABLE_SCHEM"));
        assertFalse(rs.next());

        rs = meta.getSchemas(null, null);
        rs.next();
        assertEquals("INFORMATION_SCHEMA", rs.getString("TABLE_SCHEM"));
        rs.next();
        assertEquals("PUBLIC", rs.getString("TABLE_SCHEM"));
        assertFalse(rs.next());

        rs = meta.getSchemas(null, "PUBLIC");
        rs.next();
        assertEquals("PUBLIC", rs.getString("TABLE_SCHEM"));
        assertFalse(rs.next());

        rs = meta.getTableTypes();
        rs.next();
        assertEquals("BASE TABLE", rs.getString("TABLE_TYPE"));
        rs.next();
        assertEquals("GLOBAL TEMPORARY", rs.getString("TABLE_TYPE"));
        rs.next();
        assertEquals("LOCAL TEMPORARY", rs.getString("TABLE_TYPE"));
        rs.next();
        assertEquals("SYNONYM", rs.getString("TABLE_TYPE"));
        rs.next();
        assertEquals("VIEW", rs.getString("TABLE_TYPE"));
        assertFalse(rs.next());

        rs = meta.getTables(null, Constants.SCHEMA_MAIN,
                null, new String[] { "TABLE" });
        assertNull(rs.getStatement());
        rs.next();
        assertEquals("TEST", rs.getString("TABLE_NAME"));
        assertFalse(rs.next());

        rs = meta.getTables(null, "INFORMATION_SCHEMA", null, new String[] { "BASE TABLE", "VIEW" });
        for (String name : new String[] { "CONSTANTS", "ENUM_VALUES",
                "INDEXES", "INDEX_COLUMNS", "INFORMATION_SCHEMA_CATALOG_NAME", "IN_DOUBT", "LOCKS",
                "QUERY_STATISTICS", "RIGHTS", "ROLES", "SESSIONS", "SESSION_STATE", "SETTINGS", "SYNONYMS",
                "USERS", "CHECK_CONSTRAINTS", "COLLATIONS", "COLUMNS", "COLUMN_PRIVILEGES",
                "CONSTRAINT_COLUMN_USAGE", "DOMAINS", "DOMAIN_CONSTRAINTS", "ELEMENT_TYPES", "FIELDS",
                "KEY_COLUMN_USAGE", "PARAMETERS",
                "REFERENTIAL_CONSTRAINTS", "ROUTINES", "SCHEMATA", "SEQUENCES", "TABLES", "TABLE_CONSTRAINTS",
                "TABLE_PRIVILEGES", "TRIGGERS", "VIEWS" }) {
            rs.next();
            assertEquals(name, rs.getString("TABLE_NAME"));
        }
        assertFalse(rs.next());

        rs = meta.getColumns(null, null, "TEST", null);
        rs.next();
        assertEquals("ID", rs.getString("COLUMN_NAME"));
        rs.next();
        assertEquals("NAME", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());

        rs = meta.getPrimaryKeys(null, null, "TEST");
        rs.next();
        assertEquals("ID", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());

        rs = meta.getBestRowIdentifier(null, null, "TEST",
                DatabaseMetaData.bestRowSession, false);
        rs.next();
        assertEquals("ID", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());

        rs = meta.getIndexInfo(null, null, "TEST", false, false);
        rs.next();
        String index = rs.getString("INDEX_NAME");
        assertTrue(index.startsWith("PRIMARY_KEY"));
        assertEquals("ID", rs.getString("COLUMN_NAME"));
        rs.next();
        assertEquals("IDXNAME", rs.getString("INDEX_NAME"));
        assertEquals("NAME", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());

        rs = meta.getIndexInfo(null, null, "TEST", true, false);
        rs.next();
        index = rs.getString("INDEX_NAME");
        assertTrue(index.startsWith("PRIMARY_KEY"));
        assertEquals("ID", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());

        rs = meta.getVersionColumns(null, null, "TEST");
        assertFalse(rs.next());

        stat.execute("DROP TABLE TEST");

        rs = stat.executeQuery("SELECT * FROM INFORMATION_SCHEMA.SETTINGS");
        int mvStoreSettingsCount = 0, pageStoreSettingsCount = 0;
        while (rs.next()) {
            String name = rs.getString("SETTING_NAME");
            trace(name + '=' + rs.getString("SETTING_VALUE"));
            if ("COMPRESS".equals(name) || "REUSE_SPACE".equals(name)) {
                mvStoreSettingsCount++;
            } else if (name.startsWith("PAGE_STORE_")) {
                pageStoreSettingsCount++;
            }
        }
        assertEquals(2, mvStoreSettingsCount);
        assertEquals(0, pageStoreSettingsCount);

        testMore();

        // meta.getTablePrivileges()

        // meta.getAttributes()
        // meta.getColumnPrivileges()
        // meta.getSuperTables()
        // meta.getSuperTypes()
        // meta.getTypeInfo()

        conn.close();

        deleteDb("metaData");
    }

    private void testAllowLiteralsNone() throws SQLException {
        Connection conn = getConnection("metaData");
        Statement stat = conn.createStatement();
        stat.execute("SET ALLOW_LITERALS NONE");
        DatabaseMetaData meta = conn.getMetaData();
        // meta.getAttributes(null, null, null, null);
        meta.getBestRowIdentifier(null, null, "TEST", 0, false);
        meta.getCatalogs();
        // meta.getClientInfoProperties();
        meta.getColumnPrivileges(null, null, "TEST", null);
        meta.getColumns(null, null, null, null);
        meta.getCrossReference(null, null, "TEST", null, null, "TEST");
        meta.getExportedKeys(null, null, "TEST");
        // meta.getFunctionColumns(null, null, null, null);
        // meta.getFunctions(null, null, null);
        meta.getImportedKeys(null, null, "TEST");
        meta.getIndexInfo(null, null, "TEST", false, false);
        meta.getPrimaryKeys(null, null, "TEST");
        meta.getProcedureColumns(null, null, null, null);
        meta.getProcedures(null, null, null);
        meta.getSchemas();
        meta.getSchemas(null, null);
        meta.getSuperTables(null, null, null);
        // meta.getSuperTypes(null, null, null);
        meta.getTablePrivileges(null, null, null);
        meta.getTables(null, null, null, null);
        meta.getTableTypes();
        meta.getTypeInfo();
        meta.getUDTs(null, null, null, null);
        meta.getVersionColumns(null, null, null);
        conn.close();
        deleteDb("metaData");
    }

    private void testClientInfo() throws SQLException {
        Connection conn = getConnection("metaData");
        assertNull(conn.getClientInfo("xxx"));
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet rs = meta.getClientInfoProperties();
        ResultSetMetaData rsMeta = rs.getMetaData();
        assertEquals("NAME", rsMeta.getColumnName(1));
        assertEquals("MAX_LEN", rsMeta.getColumnName(2));
        assertEquals("DEFAULT_VALUE", rsMeta.getColumnName(3));
        assertEquals("DESCRIPTION", rsMeta.getColumnName(4));
        assertEquals("VALUE", rsMeta.getColumnName(5));
        int count = 0;
        while (rs.next()) {
            count++;
        }
        if (config.networked) {
            // server0, numServers
            assertEquals(2, count);
        } else {
            // numServers
            assertEquals(1, count);
        }
        rs.close();
        conn.close();
        deleteDb("metaData");
    }

    private void testQueryStatistics() throws SQLException {
        Connection conn = getConnection("metaData");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar) as " +
                "select x, space(1000) from system_range(1, 2000)");

        ResultSet rs = stat.executeQuery(
                "select * from INFORMATION_SCHEMA.QUERY_STATISTICS");
        assertFalse(rs.next());
        rs.close();
        stat.execute("SET QUERY_STATISTICS TRUE");
        int count = 100;
        for (int i = 0; i < count; i++) {
            execute(stat, "select * from test limit 10");
        }
        // The "order by" makes the result set more stable on windows, where the
        // timer resolution is not that great
        rs = stat.executeQuery(
                "select * from INFORMATION_SCHEMA.QUERY_STATISTICS " +
                "ORDER BY EXECUTION_COUNT desc");
        assertTrue(rs.next());
        assertEquals("select * from test limit 10", rs.getString("SQL_STATEMENT"));
        assertEquals(count, rs.getInt("EXECUTION_COUNT"));
        assertEquals(config.lazy ? 0 : 10 * count, rs.getInt("CUMULATIVE_ROW_COUNT"));
        rs.close();
        conn.close();
        deleteDb("metaData");
    }

    private void testQueryStatisticsLimit() throws SQLException {
        Connection conn = getConnection("metaData");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar) as " +
                "select x, space(1000) from system_range(1, 2000)");

        ResultSet rs = stat.executeQuery(
                "select * from INFORMATION_SCHEMA.QUERY_STATISTICS");
        assertFalse(rs.next());
        rs.close();

        //first, test setting the limit before activating statistics
        int statisticsMaxEntries = 200;
        //prevent test limit being less than or equal to default limit
        assertTrue(statisticsMaxEntries > Constants.QUERY_STATISTICS_MAX_ENTRIES);
        stat.execute("SET QUERY_STATISTICS_MAX_ENTRIES " + statisticsMaxEntries);
        stat.execute("SET QUERY_STATISTICS TRUE");
        for (int i = 0; i < statisticsMaxEntries * 2; i++) {
            stat.execute("select * from test where id = " + i);
        }
        rs = stat.executeQuery("select count(*) from INFORMATION_SCHEMA.QUERY_STATISTICS");
        assertTrue(rs.next());
        assertEquals(statisticsMaxEntries, rs.getInt(1));
        rs.close();

        //first, test changing the limit once statistics is activated
        int statisticsMaxEntriesNew = 50;
        //prevent new test limit being greater than or equal to default limit
        assertTrue(statisticsMaxEntriesNew < Constants.QUERY_STATISTICS_MAX_ENTRIES);
        stat.execute("SET QUERY_STATISTICS_MAX_ENTRIES " + statisticsMaxEntriesNew);
        for (int i = 0; i < statisticsMaxEntriesNew * 2; i++) {
            stat.execute("select * from test where id = " + i);
        }
        rs = stat.executeQuery("select count(*) from INFORMATION_SCHEMA.QUERY_STATISTICS");
        assertTrue(rs.next());
        assertEquals(statisticsMaxEntriesNew, rs.getInt(1));
        rs.close();

        conn.close();
        deleteDb("metaData");
    }
}
