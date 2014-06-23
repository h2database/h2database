/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import org.h2.api.TableEngine;
import org.h2.command.ddl.CreateTableData;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.index.BaseIndex;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.SingleRowCursor;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableBase;
import org.h2.table.TableFilter;
import org.h2.test.TestBase;
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
        if (config.mvcc) {
            return;
        }
        testEarlyFilter();
        testEngineParams();
        testSimpleQuery();
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
            public void lock(Session session, boolean exclusive, boolean force) {
                // do nothing
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

}
