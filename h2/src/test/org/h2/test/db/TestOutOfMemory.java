/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Random;
import org.h2.api.ErrorCode;
import org.h2.mvstore.MVStore;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FilePathMem;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;

/**
 * Tests out of memory situations. The database must not get corrupted, and
 * transactions must stay atomic.
 */
public class TestOutOfMemory extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws SQLException, InterruptedException {
        if (config.travis) {
            // fails regularly under Travis, not sure why
            return;
        }
        if (config.vmlens) {
            // running out of memory will cause the vmlens agent to stop working
            return;
        }
        try {
            System.gc();
            testMVStoreUsingInMemoryFileSystem();
            System.gc();
            testDatabaseUsingInMemoryFileSystem();
            System.gc();
            testUpdateWhenNearlyOutOfMemory();
        } finally {
            System.gc();
        }
    }

    private void testMVStoreUsingInMemoryFileSystem() {
        FilePath.register(new FilePathMem());
        String fileName = "memFS:" + getTestName();
        MVStore store = MVStore.open(fileName);
        try {
            Map<Integer, byte[]> map = store.openMap("test");
            Random r = new Random(1);
            try {
                for (int i = 0; i < 100; i++) {
                    byte[] data = new byte[10 * 1024 * 1024];
                    r.nextBytes(data);
                    map.put(i, data);
                }
                fail();
            } catch (OutOfMemoryError e) {
                // expected
            } catch (IllegalStateException e) {
                // expected
            }
            try {
                store.close();
            } catch (IllegalStateException e) {
                // expected
            }
            store.closeImmediately();
            store = MVStore.open(fileName);
            map = store.openMap("test");
            store.close();
        } finally {
            // just in case, otherwise if this test suffers a spurious failure,
            // succeeding tests will too, because they will OOM
            store.closeImmediately();
            FileUtils.delete(fileName);
        }
    }

    private void testDatabaseUsingInMemoryFileSystem() throws SQLException, InterruptedException {
        String filename = "memFS:" + getTestName();
        String url = "jdbc:h2:" + filename;
        Connection conn = DriverManager.getConnection(url);
        Statement stat = conn.createStatement();
        try {
            stat.execute("create table test(id int, name varchar) as " +
                    "select x, space(10000000) from system_range(1, 1000)");
            fail();
        } catch (SQLException e) {
            int err = e.getErrorCode();
            assertTrue(e.getMessage(), err == ErrorCode.GENERAL_ERROR_1
                    || err == ErrorCode.OUT_OF_MEMORY);
        }
        try {
            conn.close();
            fail();
        } catch (SQLException e) {
            int err = e.getErrorCode();
            assertTrue(e.getMessage(), err == ErrorCode.GENERAL_ERROR_1
                    || err == ErrorCode.OUT_OF_MEMORY
                    || err == ErrorCode.DATABASE_IS_CLOSED);
        }
        for (int i = 0; i < 5; i++) {
            System.gc();
            Thread.sleep(20);
        }
        conn = DriverManager.getConnection(url);
        stat = conn.createStatement();
        stat.execute("select 1");
        conn.close();
        // release the static data this test generates
        FileUtils.delete(filename);
    }

    private void testUpdateWhenNearlyOutOfMemory() throws SQLException, InterruptedException {
        if (config.memory || config.mvcc || config.mvStore) {
            return;
        }
        for (int i = 0; i < 5; i++) {
            System.gc();
            Thread.sleep(20);
        }
        deleteDb("outOfMemory");
        Connection conn = getConnection("outOfMemory;MAX_OPERATION_MEMORY=1000000");
        Statement stat = conn.createStatement();
        stat.execute("drop all objects");
        stat.execute("create table stuff (id int, text varchar as space(100) || id)");
        stat.execute("insert into stuff(id) select x from system_range(1, 3000)");
        PreparedStatement prep = conn.prepareStatement(
                "update stuff set text = text || space(1000) || id");
        prep.execute();
        stat.execute("checkpoint");
        eatMemory(80);
        try {
            try {
                prep.execute();
                fail();
            } catch (SQLException e) {
                assertEquals(ErrorCode.OUT_OF_MEMORY, e.getErrorCode());
            }
            assertThrows(ErrorCode.DATABASE_IS_CLOSED, conn).close();
            freeMemory();
            conn = null;
            conn = getConnection("outOfMemory");
            stat = conn.createStatement();
            ResultSet rs = stat.executeQuery("select count(*) from stuff");
            rs.next();
            assertEquals(3000, rs.getInt(1));
        } catch (OutOfMemoryError e) {
            freeMemory();
            // out of memory not detected
            throw (Error) new AssertionError("Out of memory not detected").initCause(e);
        } finally {
            freeMemory();
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    // out of memory will / may close the database
                    assertKnownException(e);
                }
            }
        }
        deleteDb("outOfMemory");
    }

}
