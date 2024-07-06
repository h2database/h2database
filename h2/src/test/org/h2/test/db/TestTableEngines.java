/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.h2.api.ErrorCode;
import org.h2.api.TableEngine;
import org.h2.command.ddl.CreateTableData;
import org.h2.command.query.AllColumnsForPlan;
import org.h2.engine.SessionLocal;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.SingleRowCursor;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableBase;
import org.h2.table.TableFilter;
import org.h2.table.TableType;
import org.h2.test.TestBase;
import org.h2.test.TestDb;
import org.h2.value.Value;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;

/**
 * The class for external table engines mechanism testing.
 *
 * @author Sergi Vladykin
 */
public class TestTableEngines extends TestDb {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws Exception {
        testAdminPrivileges();
        testQueryExpressionFlag();
        testSubQueryInfo();
        testEngineParams();
        testSchemaEngineParams();
        testSimpleQuery();
        testMultiColumnTreeSetIndex();
    }

    private void testAdminPrivileges() throws SQLException {
        deleteDb("tableEngine");
        Connection conn = getConnection("tableEngine");
        Statement stat = conn.createStatement();
        stat.execute("CREATE USER U PASSWORD '1'");
        stat.execute("GRANT ALTER ANY SCHEMA TO U");
        Connection connUser = getConnection("tableEngine", "U", getPassword("1"));
        Statement statUser = connUser.createStatement();
        assertThrows(ErrorCode.ADMIN_RIGHTS_REQUIRED, statUser)
                .execute("CREATE TABLE T(ID INT, NAME VARCHAR) ENGINE \"" + EndlessTableEngine.class.getName() + '"');
        connUser.close();
        conn.close();
        deleteDb("tableEngine");
    }

    private void testEngineParams() throws SQLException {
        deleteDb("tableEngine");
        Connection conn = getConnection("tableEngine");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE t1(id int, name varchar) ENGINE \"" +
                EndlessTableEngine.class.getName() + "\" WITH \"param1\", \"param2\"");
        assertEquals(2,
                EndlessTableEngine.createTableData.tableEngineParams.size());
        assertEquals("param1",
                EndlessTableEngine.createTableData.tableEngineParams.get(0));
        assertEquals("param2",
                EndlessTableEngine.createTableData.tableEngineParams.get(1));
        stat.execute("CREATE TABLE t2(id int, name varchar) WITH \"param1\", \"param2\"");
        assertEquals(2,
            EndlessTableEngine.createTableData.tableEngineParams.size());
        assertEquals("param1",
            EndlessTableEngine.createTableData.tableEngineParams.get(0));
        assertEquals("param2",
            EndlessTableEngine.createTableData.tableEngineParams.get(1));
        conn.close();
        if (!config.memory) {
            // Test serialization of table parameters
            EndlessTableEngine.createTableData.tableEngineParams.clear();
            conn = getConnection("tableEngine");
            assertEquals(2,
                    EndlessTableEngine.createTableData.tableEngineParams.size());
            assertEquals("param1",
                    EndlessTableEngine.createTableData.tableEngineParams.get(0));
            assertEquals("param2",
                    EndlessTableEngine.createTableData.tableEngineParams.get(1));
            conn.close();
        }
        // Prevent memory leak
        EndlessTableEngine.createTableData = null;
        deleteDb("tableEngine");
    }

    private void testSchemaEngineParams() throws SQLException {
        deleteDb("tableEngine");
        Connection conn = getConnection("tableEngine");
        Statement stat = conn.createStatement();
        stat.execute("CREATE SCHEMA s1 WITH \"param1\", \"param2\"");

        stat.execute("CREATE TABLE s1.t1(id int, name varchar) ENGINE \"" +
                EndlessTableEngine.class.getName() + '\"');
        assertEquals(2,
            EndlessTableEngine.createTableData.tableEngineParams.size());
        assertEquals("param1",
            EndlessTableEngine.createTableData.tableEngineParams.get(0));
        assertEquals("param2",
            EndlessTableEngine.createTableData.tableEngineParams.get(1));
        conn.close();
        // Prevent memory leak
        EndlessTableEngine.createTableData = null;
        deleteDb("tableEngine");
    }

    private void testSimpleQuery() throws SQLException {

        deleteDb("tableEngine");

        Connection conn = getConnection("tableEngine");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE t1(id int, name varchar) ENGINE \"" +
                OneRowTableEngine.class.getName() + "\"");

        testStatements(stat);

        stat.close();
        conn.close();

        if (!config.memory) {
            conn = getConnection("tableEngine");
            stat = conn.createStatement();

            ResultSet rs = stat.executeQuery("SELECT name FROM t1");
            assertFalse(rs.next());
            rs.close();

            testStatements(stat);

            stat.close();
            conn.close();
        }

        deleteDb("tableEngine");

    }

    private void testStatements(Statement stat) throws SQLException {
        assertEquals(stat.executeUpdate("INSERT INTO t1 VALUES(2, 'abc')"), 1);
        assertEquals(stat.executeUpdate("UPDATE t1 SET name = 'abcdef' WHERE id=2"), 1);
        assertEquals(stat.executeUpdate("INSERT INTO t1 VALUES(3, 'abcdefghi')"), 1);

        assertEquals(stat.executeUpdate("DELETE FROM t1 WHERE id=2"), 0);
        assertEquals(stat.executeUpdate("DELETE FROM t1 WHERE id=3"), 1);

        ResultSet rs = stat.executeQuery("SELECT name FROM t1");
        assertFalse(rs.next());
        rs.close();

        assertEquals(stat.executeUpdate("INSERT INTO t1 VALUES(2, 'abc')"), 1);
        assertEquals(stat.executeUpdate("UPDATE t1 SET name = 'abcdef' WHERE id=2"), 1);
        assertEquals(stat.executeUpdate("INSERT INTO t1 VALUES(3, 'abcdefghi')"), 1);

        rs = stat.executeQuery("SELECT name FROM t1");
        assertTrue(rs.next());
        assertEquals(rs.getString(1), "abcdefghi");
        assertFalse(rs.next());
        rs.close();

    }

    private void testMultiColumnTreeSetIndex() throws SQLException {
        deleteDb("tableEngine");
        Connection conn = getConnection("tableEngine");
        Statement stat = conn.createStatement();

        stat.executeUpdate("CREATE TABLE T(A INT, B VARCHAR, C BIGINT, " +
                "D BIGINT DEFAULT 0) ENGINE \"" +
                TreeSetIndexTableEngine.class.getName() + "\"");

        stat.executeUpdate("CREATE INDEX IDX_C_B_A ON T(C, B, A)");
        stat.executeUpdate("CREATE INDEX IDX_B_A ON T(B, A)");

        List<List<Object>> dataSet = new ArrayList<>();

        dataSet.add(Arrays.asList(1, "1", 1L));
        dataSet.add(Arrays.asList(1, "0", 2L));
        dataSet.add(Arrays.asList(2, "0", -1L));
        dataSet.add(Arrays.asList(0, "0", 1L));
        dataSet.add(Arrays.asList(0, "1", null));
        dataSet.add(Arrays.asList(2, null, 0L));

        PreparedStatement prep = conn.prepareStatement("INSERT INTO T(A,B,C) VALUES(?,?,?)");
        for (List<Object> row : dataSet) {
            for (int i = 0; i < row.size(); i++) {
                prep.setObject(i + 1, row.get(i));
            }
            assertEquals(1, prep.executeUpdate());
        }
        prep.close();

        checkPlan(stat, "select max(c) from t", "direct lookup");
        checkPlan(stat, "select min(c) from t", "direct lookup");
        checkPlan(stat, "select count(*) from t", "direct lookup");

        checkPlan(stat, "select * from t", "scan");

        checkPlan(stat, "select * from t order by c", "IDX_C_B_A");
        checkPlan(stat, "select * from t order by c, b", "IDX_C_B_A");
        checkPlan(stat, "select * from t order by b", "IDX_B_A");
        checkPlan(stat, "select * from t order by b, a", "IDX_B_A");
        checkPlan(stat, "select * from t order by b, c", "IDX_B_A");
        checkPlan(stat, "select * from t order by a, b", "scan");
        checkPlan(stat, "select * from t order by a, c, b", "scan");

        checkPlan(stat, "select * from t where b > ''", "IDX_B_A");
        checkPlan(stat, "select * from t where a > 0 and b > ''", "IDX_B_A");
        checkPlan(stat, "select * from t where b < ''", "IDX_B_A");
        checkPlan(stat, "select * from t where b < '' and c < 1", "IDX_C_B_A");
        checkPlan(stat, "select * from t where a = 0", "scan");
        checkPlan(stat, "select * from t where a > 0 order by c, b", "IDX_C_B_A");
        checkPlan(stat, "select * from t where a = 0 and c > 0", "IDX_C_B_A");
        checkPlan(stat, "select * from t where a = 0 and b < '0'", "IDX_B_A");

        assertEquals(6, ((Number) query(stat, "select count(*) from t").get(0).get(0)).intValue());

        checkResultsNoOrder(stat, 6, "select * from t", "select * from t order by a");
        checkResultsNoOrder(stat, 6, "select * from t", "select * from t order by b");
        checkResultsNoOrder(stat, 6, "select * from t", "select * from t order by c");
        checkResultsNoOrder(stat, 6, "select * from t", "select * from t order by c, a");
        checkResultsNoOrder(stat, 6, "select * from t", "select * from t order by b, a");
        checkResultsNoOrder(stat, 6, "select * from t", "select * from t order by c, b, a");
        checkResultsNoOrder(stat, 6, "select * from t", "select * from t order by a, c, b");

        checkResultsNoOrder(stat, 4, "select * from t where a > 0",
                "select * from t where a > 0 order by a");
        checkResultsNoOrder(stat, 4, "select * from t where a > 0",
                "select * from t where a > 0 order by b");
        checkResultsNoOrder(stat, 4, "select * from t where a > 0",
                "select * from t where a > 0 order by c");
        checkResultsNoOrder(stat, 4, "select * from t where a > 0",
                "select * from t where a > 0 order by c, a");
        checkResultsNoOrder(stat, 4, "select * from t where a > 0",
                "select * from t where a > 0 order by b, a");
        checkResultsNoOrder(stat, 4, "select * from t where a > 0",
                "select * from t where a > 0 order by c, b, a");
        checkResultsNoOrder(stat, 4, "select * from t where a > 0",
                "select * from t where a > 0 order by a, c, b");

        checkResults(6, dataSet, stat,
                "select * from t order by a", null, new RowComparator(0));
        checkResults(6, dataSet, stat,
                "select * from t order by a desc", null, new RowComparator(true, 0));
        checkResults(6, dataSet, stat,
                "select * from t order by b, c", null, new RowComparator(1, 2));
        checkResults(6, dataSet, stat,
                "select * from t order by c, a", null, new RowComparator(2, 0));
        checkResults(6, dataSet, stat,
                "select * from t order by b, a", null, new RowComparator(1, 0));
        checkResults(6, dataSet, stat,
                "select * from t order by c, b, a", null, new RowComparator(2, 1, 0));

        checkResults(4, dataSet, stat,
                "select * from t where a > 0", new RowFilter() {
            @Override
            protected boolean accept(List<Object> row) {
                return getInt(row, 0) > 0;
            }
        }, null);
        checkResults(3, dataSet, stat, "select * from t where b = '0'", new RowFilter() {
            @Override
            protected boolean accept(List<Object> row) {
                return "0".equals(getString(row, 1));
            }
        }, null);
        checkResults(5, dataSet, stat, "select * from t where b >= '0'", new RowFilter() {
            @Override
            protected boolean accept(List<Object> row) {
                String b = getString(row, 1);
                return b != null && b.compareTo("0") >= 0;
            }
        }, null);
        checkResults(2, dataSet, stat, "select * from t where b > '0'", new RowFilter() {
            @Override
            protected boolean accept(List<Object> row) {
                String b = getString(row, 1);
                return b != null && b.compareTo("0") > 0;
            }
        }, null);
        checkResults(1, dataSet, stat, "select * from t where b > '0' and c > 0", new RowFilter() {
            @Override
            protected boolean accept(List<Object> row) {
                String b = getString(row, 1);
                Long c = getLong(row, 2);
                return b != null && b.compareTo("0") > 0 && c != null && c > 0;
            }
        }, null);
        checkResults(1, dataSet, stat, "select * from t where b > '0' and c < 2", new RowFilter() {
            @Override
            protected boolean accept(List<Object> row) {
                String b = getString(row, 1);
                Long c = getLong(row, 2);
                return b != null && b.compareTo("0") > 0 && c != null && c < 2;
            }
        }, null);
        checkResults(2, dataSet, stat, "select * from t where b > '0' and a < 2", new RowFilter() {
            @Override
            protected boolean accept(List<Object> row) {
                Integer a = getInt(row, 0);
                String b = getString(row, 1);
                return b != null && b.compareTo("0") > 0 && a != null && a < 2;
            }
        }, null);
        checkResults(1, dataSet, stat, "select * from t where b > '0' and a > 0", new RowFilter() {
            @Override
            protected boolean accept(List<Object> row) {
                Integer a = getInt(row, 0);
                String b = getString(row, 1);
                return b != null && b.compareTo("0") > 0 && a != null && a > 0;
            }
        }, null);
        checkResults(2, dataSet, stat, "select * from t where b = '0' and a > 0", new RowFilter() {
            @Override
            protected boolean accept(List<Object> row) {
                Integer a = getInt(row, 0);
                String b = getString(row, 1);
                return "0".equals(b) && a != null && a > 0;
            }
        }, null);
        checkResults(2, dataSet, stat, "select * from t where b = '0' and a < 2", new RowFilter() {
            @Override
            protected boolean accept(List<Object> row) {
                Integer a = getInt(row, 0);
                String b = getString(row, 1);
                return "0".equals(b) && a != null && a < 2;
            }
        }, null);
        conn.close();
        deleteDb("tableEngine");
    }

    private void testQueryExpressionFlag() throws SQLException {
        deleteDb("testQueryExpressionFlag");
        Connection conn = getConnection("testQueryExpressionFlag");
        Statement stat = conn.createStatement();
        stat.execute("create table QUERY_EXPR_TEST(id int) ENGINE \"" +
                TreeSetIndexTableEngine.class.getName() + "\"");
        stat.execute("create table QUERY_EXPR_TEST_NO(id int) ENGINE \"" +
                TreeSetIndexTableEngine.class.getName() + "\"");
        stat.executeQuery("select 1 + (select 1 from QUERY_EXPR_TEST)").next();
        stat.executeQuery("select 1 from QUERY_EXPR_TEST_NO where id in "
                + "(select id from QUERY_EXPR_TEST)");
        stat.executeQuery("select 1 from QUERY_EXPR_TEST_NO n "
                + "where exists(select 1 from QUERY_EXPR_TEST y where y.id = n.id)");
        conn.close();
        deleteDb("testQueryExpressionFlag");
    }

    private void testSubQueryInfo() throws SQLException {
        deleteDb("testSubQueryInfo");
        Connection conn = getConnection("testSubQueryInfo");
        Statement stat = conn.createStatement();
        stat.execute("create table SUB_QUERY_TEST(id int primary key, name varchar) ENGINE \"" +
                TreeSetIndexTableEngine.class.getName() + "\"");
        // test sub-queries
        stat.executeQuery("select * from "
                + "(select t2.id from "
                + "(select t3.id from sub_query_test t3 where t3.name = '') t4, "
                + "sub_query_test t2 "
                + "where t2.id = t4.id) t5").next();
        // test view 1
        stat.execute("create view t4 as (select t3.id from sub_query_test t3 where t3.name = '')");
        stat.executeQuery("select * from "
                + "(select t2.id from t4, sub_query_test t2 where t2.id = t4.id) t5").next();
        // test view 2
        stat.execute("create view t5 as "
                + "(select t2.id from t4, sub_query_test t2 where t2.id = t4.id)");
        stat.executeQuery("select * from t5").next();
        // test select expressions
        stat.execute("create table EXPR_TEST(id int) ENGINE \"" +
                TreeSetIndexTableEngine.class.getName() + "\"");
        stat.executeQuery("select * from (select (select id from EXPR_TEST x limit 1) a "
                + "from dual where 1 = (select id from EXPR_TEST y limit 1)) z").next();
        // test select expressions 2
        stat.execute("create table EXPR_TEST2(id int) ENGINE \"" +
                TreeSetIndexTableEngine.class.getName() + "\"");
        stat.executeQuery("select * from (select (select 1 from "
                + "(select (select 2 from EXPR_TEST) from EXPR_TEST2) ZZ) from dual)").next();
        // test select expression plan
        stat.execute("create table test_plan(id int primary key, name varchar)");
        stat.execute("create index MY_NAME_INDEX on test_plan(name)");
        checkPlan(stat, "select * from (select (select id from test_plan "
                + "where name = 'z') from dual)",
                "MY_NAME_INDEX");
        conn.close();
        deleteDb("testSubQueryInfo");
    }

    /**
     * A static assertion method.
     *
     * @param condition the condition
     * @param message the error message
     */
    static void assert0(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private void checkResultsNoOrder(Statement stat, int size, String query1, String query2)
            throws SQLException {
        List<List<Object>> res1 = query(stat, query1);
        List<List<Object>> res2 = query(stat, query2);
        if (size != res1.size() || size != res2.size()) {
            fail("Wrong size: \n" + res1 + "\n" + res2);
        }
        if (size == 0) {
            return;
        }
        int[] cols = new int[res1.get(0).size()];
        for (int i = 0; i < cols.length; i++) {
            cols[i] = i;
        }
        Comparator<List<Object>> comp = new RowComparator(cols);
        res1.sort(comp);
        res2.sort(comp);
        assertTrue("Wrong data: \n" + res1 + "\n" + res2, res1.equals(res2));
    }

    private void checkResults(int size, List<List<Object>> dataSet,
            Statement stat, String query, RowFilter filter, RowComparator sort)
            throws SQLException {
        List<List<Object>> res1 = query(stat, query);
        List<List<Object>> res2 = query(dataSet, filter, sort);

        assertTrue("Wrong size: " + size + " \n" + res1 + "\n" + res2,
                res1.size() == size && res2.size() == size);
        assertTrue(filter != null || sort  != null);

        for (int i = 0; i < res1.size(); i++) {
            List<Object> row1 = res1.get(i);
            List<Object> row2 = res2.get(i);

            assertTrue("Filter failed on row " + i + " of \n" + res1 + "\n" + res2,
                    filter == null || filter.accept(row1));
            assertTrue("Sort failed on row "  + i + " of \n" + res1 + "\n" + res2,
                    sort == null || sort.compare(row1, row2) == 0);
        }
    }

    private static List<List<Object>> query(List<List<Object>> dataSet,
            RowFilter filter, RowComparator sort) {
        List<List<Object>> res = new ArrayList<>();
        if (filter == null) {
            res.addAll(dataSet);
        } else {
            for (List<Object> row : dataSet) {
                if (filter.accept(row)) {
                    res.add(row);
                }
            }
        }
        if (sort != null) {
            res.sort(sort);
        }
        return res;
    }

    private static List<List<Object>> query(Statement stat, String query) throws SQLException {
        ResultSet rs = stat.executeQuery(query);
        int cols = rs.getMetaData().getColumnCount();
        List<List<Object>> list = new ArrayList<>();
        while (rs.next()) {
            List<Object> row = new ArrayList<>(cols);
            for (int i = 1; i <= cols; i++) {
                row.add(rs.getObject(i));
            }
            list.add(row);
        }
        rs.close();
        return list;
    }

    private void checkPlan(Statement stat, String query, String index)
            throws SQLException {
        String plan = query(stat, "EXPLAIN " + query).get(0).get(0).toString();
        assertTrue("Index '" + index + "' is not used in query plan: " + plan,
                plan.contains(index));
    }

    /**
     * A test table factory.
     */
    public static class OneRowTableEngine implements TableEngine {

        /**
         * A table implementation with one row.
         */
        private static class OneRowTable extends TableBase {

            /**
             * A scan index for one row.
             */
            public class Scan extends Index {

                Scan(Table table) {
                    super(table, table.getId(), table.getName() + "_SCAN",
                            IndexColumn.wrap(table.getColumns()), 0, IndexType.createScan(false));
                }

                @Override
                public long getRowCountApproximation(SessionLocal session) {
                    return table.getRowCountApproximation(session);
                }

                @Override
                public long getDiskSpaceUsed(boolean approximate) {
                    return table.getDiskSpaceUsed(false, approximate);
                }

                @Override
                public long getRowCount(SessionLocal session) {
                    return table.getRowCount(session);
                }

                @Override
                public void truncate(SessionLocal session) {
                    // do nothing
                }

                @Override
                public void remove(SessionLocal session) {
                    // do nothing
                }

                @Override
                public void remove(SessionLocal session, Row r) {
                    // do nothing
                }

                @Override
                public boolean needRebuild() {
                    return false;
                }

                @Override
                public double getCost(SessionLocal session, int[] masks,
                        TableFilter[] filters, int filter, SortOrder sortOrder,
                        AllColumnsForPlan allColumnsSet) {
                    return 0;
                }

                @Override
                public Cursor findFirstOrLast(SessionLocal session, boolean first) {
                    return new SingleRowCursor(row);
                }

                @Override
                public Cursor find(SessionLocal session, SearchRow first, SearchRow last, boolean reverse) {
                    return new SingleRowCursor(row);
                }

                @Override
                public void close(SessionLocal session) {
                    // do nothing
                }

                @Override
                public boolean canGetFirstOrLast() {
                    return true;
                }

                @Override
                public void add(SessionLocal session, Row r) {
                    // do nothing
                }
            }

            protected Index scanIndex;

            volatile Row row;

            OneRowTable(CreateTableData data) {
                super(data);
                scanIndex = new Scan(this);
            }

            @Override
            public Index addIndex(SessionLocal session, String indexName, int indexId, IndexColumn[] cols,
                    int uniqueColumnCount, IndexType indexType, boolean create, String indexComment) {
                return null;
            }

            @Override
            public void addRow(SessionLocal session, Row r) {
                this.row = r;
            }

            @Override
            public boolean canDrop() {
                return true;
            }

            @Override
            public boolean canGetRowCount(SessionLocal session) {
                return true;
            }

            @Override
            public void checkSupportAlter() {
                // do nothing
            }

            @Override
            public void close(SessionLocal session) {
                // do nothing
            }

            @Override
            public ArrayList<Index> getIndexes() {
                return null;
            }

            @Override
            public long getMaxDataModificationId() {
                return 0;
            }

            @Override
            public long getRowCount(SessionLocal session) {
                return getRowCountApproximation(session);
            }

            @Override
            public long getRowCountApproximation(SessionLocal session) {
                return row == null ? 0 : 1;
            }

            @Override
            public Index getScanIndex(SessionLocal session) {
                return scanIndex;
            }

            @Override
            public TableType getTableType() {
                return TableType.EXTERNAL_TABLE_ENGINE;
            }

            @Override
            public boolean isDeterministic() {
                return false;
            }

            @Override
            public void removeRow(SessionLocal session, Row r) {
                this.row = null;
            }

            @Override
            public long truncate(SessionLocal session) {
                long result = row != null ? 1L : 0L;
                row = null;
                return result;
            }

        }

        /**
         * Create a new OneRowTable.
         *
         * @param data the meta data of the table to create
         * @return the new table
         */
        @Override
        public OneRowTable createTable(CreateTableData data) {
            return new OneRowTable(data);
        }

    }

    /**
     * A test table factory.
     */
    public static class EndlessTableEngine implements TableEngine {

        public static CreateTableData createTableData;

        /**
         * A table implementation with one row.
         */
        private static class EndlessTable extends OneRowTableEngine.OneRowTable {

            EndlessTable(CreateTableData data) {
                super(data);
                row = Row.get(new Value[] { ValueInteger.get(1), ValueNull.INSTANCE }, 0);
                scanIndex = new Auto(this);
            }

            /**
             * A scan index for one row.
             */
            public class Auto extends OneRowTableEngine.OneRowTable.Scan {

                Auto(Table table) {
                    super(table);
                }

                @Override
                public Cursor find(SessionLocal session, SearchRow first, SearchRow last, boolean reverse) {
                    return new SingleRowCursor(row);
                }

            }

        }

        /**
         * Create a new table.
         *
         * @param data the meta data of the table to create
         * @return the new table
         */
        @Override
        public EndlessTable createTable(CreateTableData data) {
            createTableData = data;
            return new EndlessTable(data);
        }

    }

    /**
     * A table engine that internally uses a tree set.
     */
    public static class TreeSetIndexTableEngine implements TableEngine {

        static TreeSetTable created;

        @Override
        public Table createTable(CreateTableData data) {
            return created = new TreeSetTable(data);
        }
    }

    /**
     * A table that internally uses a tree set.
     */
    private static class TreeSetTable extends TableBase {
        int dataModificationId;

        ArrayList<Index> indexes;

        TreeSetIndex scan = new TreeSetIndex(this, "scan",
                IndexColumn.wrap(getColumns()), IndexType.createScan(false)) {
            @Override
            public double getCost(SessionLocal session, int[] masks,
                    TableFilter[] filters, int filter, SortOrder sortOrder,
                    AllColumnsForPlan allColumnsSet) {
                return getCostRangeIndex(masks, getRowCount(session), filters,
                        filter, sortOrder, true, allColumnsSet);
            }
        };

        TreeSetTable(CreateTableData data) {
            super(data);
        }

        @Override
        public long truncate(SessionLocal session) {
            long result = getRowCountApproximation(session);
            if (indexes != null) {
                for (Index index : indexes) {
                    index.truncate(session);
                }
            } else {
                scan.truncate(session);
            }
            dataModificationId++;
            return result;
        }

        @Override
        public void removeRow(SessionLocal session, Row row) {
            if (indexes != null) {
                for (Index index : indexes) {
                    index.remove(session, row);
                }
            } else {
                scan.remove(session, row);
            }
            dataModificationId++;
        }

        @Override
        public void addRow(SessionLocal session, Row row) {
            if (indexes != null) {
                for (Index index : indexes) {
                    index.add(session, row);
                }
            } else {
                scan.add(session, row);
            }
            dataModificationId++;
        }

        @Override
        public Index addIndex(SessionLocal session, String indexName, int indexId, IndexColumn[] cols,
                int uniqueColumnCount, IndexType indexType, boolean create, String indexComment) {
            if (indexes == null) {
                indexes = new ArrayList<>(2);
                // Scan must be always at 0.
                indexes.add(scan);
            }
            Index index = new TreeSetIndex(this, indexName, cols, indexType);
            for (SearchRow row : scan.set) {
                index.add(session, (Row) row);
            }
            indexes.add(index);
            dataModificationId++;
            setModified();
            return index;
        }

        @Override
        public boolean isDeterministic() {
            return false;
        }

        @Override
        public TableType getTableType() {
            return TableType.EXTERNAL_TABLE_ENGINE;
        }

        @Override
        public Index getScanIndex(SessionLocal session) {
            return scan;
        }

        @Override
        public long getRowCountApproximation(SessionLocal session) {
            return getScanIndex(null).getRowCountApproximation(session);
        }

        @Override
        public long getRowCount(SessionLocal session) {
            return scan.getRowCount(session);
        }

        @Override
        public long getMaxDataModificationId() {
            return dataModificationId;
        }

        @Override
        public ArrayList<Index> getIndexes() {
            return indexes;
        }

        @Override
        public void close(SessionLocal session) {
            // No-op.
        }

        @Override
        public void checkSupportAlter() {
            // No-op.
        }

        @Override
        public boolean canGetRowCount(SessionLocal session) {
            return true;
        }

        @Override
        public boolean canDrop() {
            return true;
        }
    }

    /**
     * An index that internally uses a tree set.
     */
    private static class TreeSetIndex extends Index implements Comparator<SearchRow> {

        final TreeSet<SearchRow> set = new TreeSet<>(this);

        TreeSetIndex(Table t, String name, IndexColumn[] cols, IndexType type) {
            super(t, 0, name, cols, 0, type);
        }

        @Override
        public int compare(SearchRow o1, SearchRow o2) {
            int res = compareRows(o1, o2);
            if (res == 0) {
                if (o1.getKey() == Long.MAX_VALUE || o2.getKey() == Long.MIN_VALUE) {
                    res = 1;
                } else if (o1.getKey() == Long.MIN_VALUE || o2.getKey() == Long.MAX_VALUE) {
                    res = -1;
                }
            }
            return res;
        }

        @Override
        public void close(SessionLocal session) {
            // No-op.
        }

        @Override
        public void add(SessionLocal session, Row row) {
            set.add(row);
        }

        @Override
        public void remove(SessionLocal session, Row row) {
            set.remove(row);
        }

        private static SearchRow mark(SearchRow row, boolean first) {
            if (row != null) {
                // Mark this row to be a search row.
                row.setKey(first ? Long.MIN_VALUE : Long.MAX_VALUE);
            }
            return row;
        }

        @Override
        public Cursor find(SessionLocal session, SearchRow first, SearchRow last, boolean reverse) {
            if (reverse) {
                SearchRow temp = first;
                first = last;
                last = temp;
            }
            NavigableSet<SearchRow> subSet;
            if (first != null && last != null && compareRows(last, first) < 0) {
                subSet = Collections.emptyNavigableSet();
            } else {
                if (first != null) {
                    first = set.floor(mark(first, true));
                }
                if (last != null) {
                    last = set.ceiling(mark(last, false));
                }
                if (first == null && last == null) {
                    subSet = set;
                } else if (first != null) {
                    if (last != null) {
                        subSet = set.subSet(first,  true, last, true);
                    } else {
                        subSet = set.tailSet(first, true);
                    }
                } else if (last != null) {
                    subSet = set.headSet(last, true);
                } else {
                    throw new IllegalStateException();
                }
                if (reverse) {
                    subSet = subSet.descendingSet();
                }
            }
            return new IteratorCursor(subSet.iterator());
        }

        @Override
        public double getCost(SessionLocal session, int[] masks,
                TableFilter[] filters, int filter, SortOrder sortOrder,
                AllColumnsForPlan allColumnsSet) {
            return getCostRangeIndex(masks, set.size(), filters, filter,
                    sortOrder, false, allColumnsSet);
        }

        @Override
        public void remove(SessionLocal session) {
            // No-op.
        }

        @Override
        public void truncate(SessionLocal session) {
            set.clear();
        }

        @Override
        public boolean canGetFirstOrLast() {
            return true;
        }

        @Override
        public Cursor findFirstOrLast(SessionLocal session, boolean first) {
            return set.isEmpty() ? SingleRowCursor.EMPTY
                    : new SingleRowCursor((Row) (first ? set.first() : set.last()));
        }

        @Override
        public boolean needRebuild() {
            return true;
        }

        @Override
        public long getRowCount(SessionLocal session) {
            return set.size();
        }

        @Override
        public long getRowCountApproximation(SessionLocal session) {
            return getRowCount(null);
        }

    }

    /**
     */
    private static class IteratorCursor implements Cursor {
        Iterator<SearchRow> it;
        private Row current;

        IteratorCursor(Iterator<SearchRow> it) {
            this.it = it;
        }

        @Override
        public boolean previous() {
            throw DbException.getUnsupportedException("prev");
        }

        @Override
        public boolean next() {
            if (it.hasNext()) {
                current = (Row) it.next();
                return true;
            }
            current = null;
            return false;
        }

        @Override
        public SearchRow getSearchRow() {
            return get();
        }

        @Override
        public Row get() {
            return current;
        }

        @Override
        public String toString() {
            return "IteratorCursor->" + current;
        }
    }

    /**
     * A comparator for rows (lists of comparable objects).
     */
    private static class RowComparator implements Comparator<List<Object>> {
        private int[] cols;
        private boolean descending;

        RowComparator(int... cols) {
            this.descending = false;
            this.cols = cols;
        }

        RowComparator(boolean descending, int... cols) {
            this.descending = descending;
            this.cols = cols;
        }

        @SuppressWarnings("unchecked")
        @Override
        public int compare(List<Object> row1, List<Object> row2) {
            for (int i = 0; i < cols.length; i++) {
                int col = cols[i];
                Comparable<Object> o1 = (Comparable<Object>) row1.get(col);
                Comparable<Object> o2 = (Comparable<Object>) row2.get(col);
                if (o1 == null) {
                    return applyDescending(o2 == null ? 0 : -1);
                }
                if (o2 == null) {
                    return applyDescending(1);
                }
                int res = o1.compareTo(o2);
                if (res != 0) {
                    return applyDescending(res);
                }
            }
            return 0;
        }

        private int applyDescending(int v) {
            if (!descending) {
                return v;
            }
            if (v == 0) {
                return v;
            }
            return -v;
        }
    }

    /**
     * A filter for rows (lists of objects).
     */
    abstract static class RowFilter {

        /**
         * Check whether the row needs to be processed.
         *
         * @param row the row
         * @return true if yes
         */
        protected abstract boolean accept(List<Object> row);

        /**
         * Get an integer from a row.
         *
         * @param row the row
         * @param col the column index
         * @return the value
         */
        protected Integer getInt(List<Object> row, int col) {
            return (Integer) row.get(col);
        }

        /**
         * Get a long from a row.
         *
         * @param row the row
         * @param col the column index
         * @return the value
         */
        protected Long getLong(List<Object> row, int col) {
            return (Long) row.get(col);
        }

        /**
         * Get a string from a row.
         *
         * @param row the row
         * @param col the column index
         * @return the value
         */
        protected String getString(List<Object> row, int col) {
            return (String) row.get(col);
        }

    }

}
