/*
 * Copyright 2004-2026 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;
import org.h2.test.TestDb;

/**
 * Tests the query cache.
 */
public class TestQueryCache extends TestDb {

    /** Calls of {@link #countPrepare()}. */
    private static final AtomicInteger PREPARED = new AtomicInteger();

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws Exception {
        deleteDb("queryCache");
        testCacheReuse();
        testClearingCacheWithTableStructureChanges();
        deleteDb("queryCache");
    }

    private void testCacheReuse() throws Exception {
        try (Connection conn = getConnection("queryCache;QUERY_CACHE_SIZE=10")) {
            Statement stat = conn.createStatement();
            stat.execute("create table test(id int, name varchar)");
            // a deterministic function without arguments is evaluated when
            // the statement is prepared, so its calls count the parsed commands
            stat.execute("create alias count_prepare deterministic for '"
                    + getClass().getName() + ".countPrepare'");
            String query = "select count(*) from test where count_prepare() = 1";
            int prepared = PREPARED.get();
            PreparedStatement prep1 = conn.prepareStatement(query);
            assertQueryResult(prep1.executeQuery());
            assertTrue(PREPARED.get() > prepared);
            // the cached command is still in use by prep1: a new one is parsed
            prepared = PREPARED.get();
            PreparedStatement prep2 = conn.prepareStatement(query);
            assertQueryResult(prep2.executeQuery());
            assertTrue(PREPARED.get() > prepared);
            prep2.close();
            // closed commands are taken from the cache
            prepared = PREPARED.get();
            PreparedStatement prep3 = conn.prepareStatement(query);
            assertQueryResult(prep3.executeQuery());
            prep3.close();
            assertQueryResult(stat.executeQuery(query));
            assertEquals(prepared, PREPARED.get());
            prep1.close();
            // a change of the schema clears the cache
            stat.execute("create table test2(id int)");
            prepared = PREPARED.get();
            assertQueryResult(stat.executeQuery(query));
            assertTrue(PREPARED.get() > prepared);
            stat.execute("drop table test, test2");
            stat.execute("drop alias count_prepare");
        }
    }

    private void assertQueryResult(ResultSet rs) throws SQLException {
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();
    }

    /**
     * This method is called via reflection from the database.
     *
     * @return 1
     */
    public static int countPrepare() {
        PREPARED.incrementAndGet();
        return 1;
    }

    private void testClearingCacheWithTableStructureChanges() throws Exception {
        try (Connection conn = getConnection("queryCache;QUERY_CACHE_SIZE=10")) {
            assertThrows(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_DATABASE_EMPTY_1, conn).
                    prepareStatement("SELECT * FROM TEST");
            Statement stat = conn.createStatement();
            stat.executeUpdate("CREATE TABLE TEST(col1 bigint, col2 varchar(255))");
            PreparedStatement prep = conn.prepareStatement("SELECT * FROM TEST");
            prep.close();
            stat.executeUpdate("DROP TABLE TEST");
            assertThrows(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_DATABASE_EMPTY_1, conn).
                    prepareStatement("SELECT * FROM TEST");
        }
    }
}
