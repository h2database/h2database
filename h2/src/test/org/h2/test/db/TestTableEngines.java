/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
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
import java.util.Set;
import java.util.TreeSet;
import org.h2.api.TableEngine;
import org.h2.command.ddl.CreateTableData;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.index.BaseIndex;
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
import org.h2.test.TestBase;
import org.h2.util.New;
import org.h2.value.Value;
import org.h2.value.ValueInt;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;

/**
 * The class for external table engines mechanism testing.
 *
 * @author Sergi Vladykin
 */
public class TestTableEngines extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        testEarlyFilter();
        testEngineParams();
        testSimpleQuery();
        testMultiColumnTreeSetIndex();
    }

    private void testEarlyFilter() throws SQLException {
        deleteDb("tableEngine");
        Connection conn = getConnection("tableEngine;EARLY_FILTER=TRUE");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE t1(id int, name varchar) ENGINE \"" +
        EndlessTableEngine.class.getName() + "\"");
        ResultSet rs = stat.executeQuery(
                "SELECT name FROM t1 where id=1 and name is not null");
        assertTrue(rs.next());
        assertEquals("((ID = 1)\n    AND (NAME IS NOT NULL))", rs.getString(1));
        rs.close();
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

        stat.executeUpdate("CREATE TABLE T(A INT, B VARCHAR, C BIGINT) ENGINE \"" +
                TreeSetIndexTableEngine.class.getName() + "\"");

        stat.executeUpdate("CREATE INDEX IDX_C_B_A ON T(C, B, A)");
        stat.executeUpdate("CREATE INDEX IDX_B_A ON T(B, A)");

        List<List<Object>> dataSet = New.arrayList();

        dataSet.add(Arrays.<Object>asList(1, "1", 1L));
        dataSet.add(Arrays.<Object>asList(1, "0", 2L));
        dataSet.add(Arrays.<Object>asList(2, "0", -1L));
        dataSet.add(Arrays.<Object>asList(0, "0", 1L));
        dataSet.add(Arrays.<Object>asList(0, "1", null));
        dataSet.add(Arrays.<Object>asList(2, null, 0L));

        PreparedStatement prep = conn.prepareStatement("INSERT INTO T VALUES(?,?,?)");
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
        checkPlan(stat, "select * from t order by b, c", "scan");
        checkPlan(stat, "select * from t order by a, b", "scan");
        checkPlan(stat, "select * from t order by a, c, b", "scan");

        checkPlan(stat, "select * from t where b > ''", "IDX_B_A");
        checkPlan(stat, "select * from t where a > 0 and b > ''", "IDX_B_A");
        checkPlan(stat, "select * from t where b < ''", "IDX_B_A");
        checkPlan(stat, "select * from t where b < '' and c < 1", "IDX_C_B_A");
        checkPlan(stat, "select * from t where a = 0", "scan");
        checkPlan(stat, "select * from t where a > 0 order by c, b", "IDX_C_B_A");
        checkPlan(stat, "select * from t where a = 0 and c > 0", "IDX_C_B_A");
        checkPlan(stat, "select * from t where a = 0 and b < 0", "IDX_B_A");

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

        deleteDb("tableEngine");
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
        Collections.sort(res1, comp);
        Collections.sort(res2, comp);
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
        List<List<Object>> res = New.arrayList();
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
            Collections.sort(res, sort);
        }
        return res;
    }

    private static List<List<Object>> query(Statement stat, String query) throws SQLException {
        ResultSet rs = stat.executeQuery(query);
        int cols = rs.getMetaData().getColumnCount();
        List<List<Object>> list = New.arrayList();
        while (rs.next()) {
            List<Object> row = New.arrayList(cols);
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
            public class Scan extends BaseIndex {

                Scan(Table table) {
                    initBaseIndex(table, table.getId(), table.getName() + "_SCAN",
                            IndexColumn.wrap(table.getColumns()), IndexType.createScan(false));
                }

                @Override
                public long getRowCountApproximation() {
                    return table.getRowCountApproximation();
                }

                @Override
                public long getDiskSpaceUsed() {
                    return table.getDiskSpaceUsed();
                }

                @Override
                public long getRowCount(Session session) {
                    return table.getRowCount(session);
                }

                @Override
                public void checkRename() {
                    // do nothing
                }

                @Override
                public void truncate(Session session) {
                    // do nothing
                }

                @Override
                public void remove(Session session) {
                    // do nothing
                }

                @Override
                public void remove(Session session, Row r) {
                    // do nothing
                }

                @Override
                public boolean needRebuild() {
                    return false;
                }

                @Override
                public double getCost(Session session, int[] masks,
                        TableFilter filter, SortOrder sortOrder) {
                    return 0;
                }

                @Override
                public Cursor findFirstOrLast(Session session, boolean first) {
                    return new SingleRowCursor(row);
                }

                @Override
                public Cursor find(Session session, SearchRow first, SearchRow last) {
                    return new SingleRowCursor(row);
                }

                @Override
                public void close(Session session) {
                    // do nothing
                }

                @Override
                public boolean canGetFirstOrLast() {
                    return true;
                }

                @Override
                public void add(Session session, Row r) {
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
            public Index addIndex(Session session, String indexName,
                    int indexId, IndexColumn[] cols, IndexType indexType,
                    boolean create, String indexComment) {
                return null;
            }

            @Override
            public void addRow(Session session, Row r) {
                this.row = r;
            }

            @Override
            public boolean canDrop() {
                return true;
            }

            @Override
            public boolean canGetRowCount() {
                return true;
            }

            @Override
            public void checkSupportAlter() {
                // do nothing
            }

            @Override
            public void close(Session session) {
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
            public long getRowCount(Session session) {
                return getRowCountApproximation();
            }

            @Override
            public long getRowCountApproximation() {
                return row == null ? 0 : 1;
            }

            @Override
            public long getDiskSpaceUsed() {
                return 0;
            }

            @Override
            public Index getScanIndex(Session session) {
                return scanIndex;
            }

            @Override
            public String getTableType() {
                return EXTERNAL_TABLE_ENGINE;
            }

            @Override
            public Index getUniqueIndex() {
                return null;
            }

            @Override
            public boolean isDeterministic() {
                return false;
            }

            @Override
            public boolean isLockedExclusively() {
                return false;
            }

            @Override
            public boolean lock(Session session, boolean exclusive, boolean force) {
                // do nothing
                return false;
            }

            @Override
            public void removeRow(Session session, Row r) {
                this.row = null;
            }

            @Override
            public void truncate(Session session) {
                row = null;
            }

            @Override
            public void unlock(Session s) {
                // do nothing
            }

            @Override
            public void checkRename() {
                // do nothing
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
                row = new Row(new Value[] { ValueInt.get(1), ValueNull.INSTANCE }, 0);
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
                public Cursor find(TableFilter filter, SearchRow first, SearchRow last) {
                    return find(filter.getFilterCondition());
                }

                @Override
                public Cursor find(Session session, SearchRow first, SearchRow last) {
                    return find(null);
                }

                /**
                 * Search within the table.
                 *
                 * @param filter the table filter (optional)
                 * @return the cursor
                 */
                private Cursor find(Expression filter) {
                    if (filter != null) {
                        row.setValue(1, ValueString.get(filter.getSQL()));
                    }
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
        @Override
        public Table createTable(CreateTableData data) {
            return new TreeSetTable(data);
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
            public double getCost(Session session, int[] masks, TableFilter filter,
                SortOrder sortOrder) {
                return getRowCount(session) + Constants.COST_ROW_OFFSET;
            }
        };

        public TreeSetTable(CreateTableData data) {
            super(data);
        }

        @Override
        public void checkRename() {
            // No-op.
        }

        @Override
        public void unlock(Session s) {
            // No-op.
        }

        @Override
        public void truncate(Session session) {
            if (indexes != null) {
                for (Index index : indexes) {
                    index.truncate(session);
                }
            } else {
                scan.truncate(session);
            }
            dataModificationId++;
        }

        @Override
        public void removeRow(Session session, Row row) {
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
        public void addRow(Session session, Row row) {
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
        public Index addIndex(Session session, String indexName, int indexId, IndexColumn[] cols,
                IndexType indexType, boolean create, String indexComment) {
            if (indexes == null) {
                indexes = New.arrayList(2);
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
        public boolean lock(Session session, boolean exclusive, boolean forceLockEvenInMvcc) {
            return true;
        }

        @Override
        public boolean isLockedExclusively() {
            return false;
        }

        @Override
        public boolean isDeterministic() {
            return false;
        }

        @Override
        public Index getUniqueIndex() {
            return null;
        }

        @Override
        public String getTableType() {
            return EXTERNAL_TABLE_ENGINE;
        }

        @Override
        public Index getScanIndex(Session session) {
            return scan;
        }

        @Override
        public long getRowCountApproximation() {
            return getScanIndex(null).getRowCountApproximation();
        }

        @Override
        public long getRowCount(Session session) {
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
        public long getDiskSpaceUsed() {
            return 0;
        }

        @Override
        public void close(Session session) {
            // No-op.
        }

        @Override
        public void checkSupportAlter() {
            // No-op.
        }

        @Override
        public boolean canGetRowCount() {
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
    private static class TreeSetIndex extends BaseIndex implements Comparator<SearchRow> {
        final TreeSet<SearchRow> set = new TreeSet<SearchRow>(this);

        TreeSetIndex(Table t, String name, IndexColumn[] cols, IndexType type) {
            initBaseIndex(t, 0, name, cols, type);
        }

        @Override
        public int compare(SearchRow o1, SearchRow o2) {
            int res = compareRows(o1, o2);
            if (res == 0 && (o1.getKey() == Long.MAX_VALUE || o2.getKey() == Long.MAX_VALUE)) {
                res = -1;
            }
            return res;
        }

        @Override
        public void close(Session session) {
            // No-op.
        }

        @Override
        public void add(Session session, Row row) {
            set.add(row);
        }

        @Override
        public void remove(Session session, Row row) {
            set.remove(row);
        }

        private static SearchRow mark(SearchRow row) {
            if (row != null) {
                // Mark this row to be a search row.
                row.setKey(Long.MAX_VALUE);
            }
            return row;
        }

        @Override
        public Cursor find(Session session, SearchRow first, SearchRow last) {
            Set<SearchRow> subSet;
            if (first != null && last != null && compareRows(last, first) < 0) {
                subSet = Collections.emptySet();
            } else {
                if (first != null) {
                    first = set.floor(mark(first));
                }
                if (last != null) {
                    last = set.ceiling(mark(last));
                }
                if (first == null && last == null) {
                    subSet = set;
                } else if (first != null) {
                    subSet = set.tailSet(first, true);
                } else if (last != null) {
                    subSet = set.headSet(last, true);
                } else {
                    subSet = set.subSet(first,  true, last, true);
                }
            }
            return new IteratorCursor(subSet.iterator());
        }

        @Override
        public double getCost(Session session, int[] masks, TableFilter filter,
                SortOrder sortOrder) {
            return getCostRangeIndex(masks, set.size(), filter, sortOrder);
        }

        @Override
        public void remove(Session session) {
            // No-op.
        }

        @Override
        public void truncate(Session session) {
            set.clear();
        }

        @Override
        public boolean canGetFirstOrLast() {
            return true;
        }

        @Override
        public Cursor findFirstOrLast(Session session, boolean first) {
            return new SingleRowCursor((Row)
                    (set.isEmpty() ? null : first ? set.first() : set.last()));
        }

        @Override
        public boolean needRebuild() {
            return true;
        }

        @Override
        public long getRowCount(Session session) {
            return set.size();
        }

        @Override
        public long getRowCountApproximation() {
            return getRowCount(null);
        }

        @Override
        public long getDiskSpaceUsed() {
            return 0;
        }

        @Override
        public void checkRename() {
            // No-op.
        }
    }

    /**
     */
    private static class IteratorCursor implements Cursor {
        private Iterator<SearchRow> it;
        private Row current;

        public IteratorCursor(Iterator<SearchRow> it) {
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
    }

    /**
     * A comparator for rows (lists of comparable objects).
     */
    private static class RowComparator implements Comparator<List<Object>> {
        private int[] cols;

        public RowComparator(int... cols) {
            this.cols = cols;
        }

        @SuppressWarnings("unchecked")
        @Override
        public int compare(List<Object> row1, List<Object> row2) {
            if (row1.size() != row2.size()) {
                throw new IllegalStateException("Row size mismatch.");
            }
            for (int i = 0; i < cols.length; i++) {
                int col = cols[i];
                Comparable<Object> o1 = (Comparable<Object>) row1.get(col);
                Comparable<Object> o2 = (Comparable<Object>) row2.get(col);
                if (o1 == null) {
                    return o2 == null ? 0 : -1;
                }
                if (o2 == null) {
                    return 1;
                }
                int res = o1.compareTo(o2);
                if (res != 0) {
                    return res;
                }
            }
            return 0;
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
