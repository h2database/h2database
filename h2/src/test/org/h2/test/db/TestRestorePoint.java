/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: Enno Thieleke
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import org.h2.api.ErrorCode;
import org.h2.test.TestAll;
import org.h2.test.TestBase;
import org.h2.test.TestDb;

/**
 * Test restore point statements.
 */
public class TestRestorePoint extends TestDb {

    private boolean pseudoReconnect, autoCommit;

    private Connection conn;
    private Statement stat;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        for (boolean memory : new boolean[] {false, true}) {
            TestAll conf = new TestAll();
            conf.memory = memory;
            TestBase.createCaller().init(conf).testFromMain();
        }
    }

    @Override
    public void test() throws Exception {
        if (!config.memory) {
            test(false, false);
            test(false, true);
        }
        test(true, false);
        test(true, true);
    }

    private void test(boolean pseudoReconnect, boolean autoCommit) throws Exception {
        this.pseudoReconnect = pseudoReconnect;
        this.autoCommit = autoCommit;
        testCreateRestorePoint();
        testDropRestorePoint();
        testRestoreToPoint1();
        testRestoreToPoint2();
        testCreateRestorePointWithConcurrentTransaction();
        testRestoreToPointWithConcurrentTransaction();
        testMemoryFreeing();
        testDefragAlways();
        deleteDb(getTestName());
    }

    private void connect() throws Exception {
        connect(true);
    }

    private void connect(boolean deleteDb) throws Exception {
        if (deleteDb) {
            deleteDb(getTestName());
        }
        conn = getConnection(getTestName());
        conn.setAutoCommit(autoCommit);
        stat = conn.createStatement();
    }

    private void connect(String url) throws Exception {
        conn = DriverManager.getConnection(url, getUser(), getPassword());
        stat = conn.createStatement();
    }

    private void disconnect() throws Exception {
        conn.close();
    }

    private void reconnect() throws Exception {
        if (!autoCommit) {
            conn.commit();
        }
        if (!pseudoReconnect) {
            disconnect();
            connect(false);
        }
    }

    private void reconnect(String url) throws Exception {
        disconnect();
        connect(url);
    }

    private void testCreateRestorePoint() throws Exception {
        connect();
        stat.execute("create restore point rp1");
        assertRestorePoints(stat, "RP1");
        assertThrows(ErrorCode.RESTORE_POINT_ALREADY_EXISTS, stat).execute("create restore point rp1");

        reconnect();
        stat.execute("create restore point rp2");
        assertRestorePoints(stat, "RP1", "RP2");
        assertThrows(ErrorCode.RESTORE_POINT_ALREADY_EXISTS, stat).execute("create restore point rp2");

        reconnect();
        stat.execute("create restore point rp3");
        assertRestorePoints(stat, "RP1", "RP2", "RP3");
        assertThrows(ErrorCode.RESTORE_POINT_ALREADY_EXISTS, stat).execute("create restore point rp3");

        disconnect();
    }

    private void testDropRestorePoint() throws Exception {
        connect();
        stat.execute("create restore point rp1");
        assertRestorePoints(stat, "RP1");

        reconnect();
        stat.execute("create restore point rp2");
        assertRestorePoints(stat, "RP1", "RP2");
        stat.execute("drop restore point rp1");
        assertRestorePoints(stat, "RP2");

        disconnect();
    }

    private void testRestoreToPoint1() throws Exception {
        connect();
        stat.execute("create table my_table(id int primary key)");
        stat.execute("insert into my_table values(1)");
        stat.execute("create restore point rp1");

        reconnect();
        stat.execute("create restore point rp2");
        stat.execute("insert into my_table values(2)");
        stat.execute("insert into my_table values(3)");
        stat.execute("create restore point rp3");
        stat.execute("insert into my_table values(4)");

        reconnect();
        assertSingleValue(stat, "select count(*) from my_table", 4);
        stat.execute("restore to point rp3");
        assertSingleValue(stat, "select count(*) from my_table", 3);

        reconnect();
        assertSingleValue(stat, "select count(*) from my_table", 3);
        stat.execute("restore to point rp1");
        assertSingleValue(stat, "select count(*) from my_table", 1);
        // Restoring to a point before other RPs discards those other RPs too.
        assertRestorePoints(stat, "RP1");

        disconnect();
    }

    private void testRestoreToPoint2() throws Exception {
        connect();
        stat.execute("create restore point rp1");

        reconnect();
        stat.execute("create restore point rp2");

        reconnect();
        stat.execute("drop restore point rp1");
        assertRestorePoints(stat, "RP2");

        reconnect();
        stat.execute("restore to point rp2");

        reconnect();
        /* Restoring to the point RP2, which has been created before the
         * earlier restore point RP1 has been dropped, also restores said
         * earlier restore point RP1.
         */
        assertRestorePoints(stat, "RP1", "RP2");

        disconnect();
    }

    private void testCreateRestorePointWithConcurrentTransaction() throws Exception {
        if (!autoCommit) {
            return;
        }
        connect();
        stat.execute("create restore point rp1");

        Connection conn2 = getConnection(getTestName());
        conn2.setAutoCommit(false);
        Statement stat2 = conn2.createStatement();
        stat2.execute("create table my_table(id int primary key)");
        stat2.execute("insert into my_table values(1)");
        stat.execute("create restore point rp2");

        // Because table creations are not transactional, but inserts are,
        // we should be able to see the table, but without any data, because it's not committed yet.
        assertSingleValue(stat, "select count(*) from my_table", 0);

        // Let the other client commit its pending transaction.
        conn2.commit();
        conn2.close();

        conn.rollback(); // This dissociates the previous result from our session so we can read the now committed value from the other session.
        assertSingleValue(stat, "select count(*) from my_table", 1);

        stat.execute("restore to point rp2");
        // The data should not exist anymore, because we've restored the database to a point where it never existed in the first place.
        assertSingleValue(stat, "select count(*) from my_table", 0);

        stat.execute("restore to point rp1");
        // Now the table should be gone as well, because RP1 has been created before the table.
        assertThrows(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_DATABASE_EMPTY_1, stat).execute("select count(*) from my_table");

        disconnect();
    }

    private void testRestoreToPointWithConcurrentTransaction() throws Exception {
        if (!autoCommit) {
            return;
        }
        connect();

        Connection conn2 = getConnection(getTestName());
        conn2.setAutoCommit(false);
        Statement stat2 = conn2.createStatement();
        stat2.execute("create table my_table(id int primary key)");
        stat2.execute("insert into my_table values(1)");

        stat.execute("create restore point rp1");
        stat.execute("restore to point rp1");
        try {
            stat2.execute("select count(*) from my_table");
            fail("Got a result, but expected the connection to be closed");
        } catch (SQLException ignore) {
            // I have observed exceptions with different error codes, so I'm not checking for a specific one.
        }

        disconnect();
    }

    /**
     * This test ensures that returning to a restore point actually frees up memory.
     * If we don't return to a restore point, we essentially hog memory,
     * because with every statement we create a new MVStore version (auto-commit is turned on).
     */
    private void testMemoryFreeing() throws Exception {
        if (!config.memory || !autoCommit) {
            return;
        }
        connect();

        stat.execute("create table test(id bigint generated always as identity primary key, v binary varying)");
        PreparedStatement prepInsert = conn.prepareStatement("insert into test(v) values ?");
        PreparedStatement prepSelect = conn.prepareStatement("select v from test where id = 1");
        prepInsert.setBytes(1, new byte[9_999]);
        prepInsert.executeUpdate();
        stat.execute("create restore point p");
        PreparedStatement prepDelete = conn.prepareStatement("delete from test");
        for (int i = 1; i <= 5_000; i++) {
            prepDelete.executeUpdate();
            prepInsert.setBytes(1, new byte[1_000_000]);
            prepInsert.executeUpdate();

            stat.execute("restore to point p");
            try (ResultSet r = prepSelect.executeQuery()) {
                assertTrue(r.next());
                assertTrue(r.getBytes(1).length == 9_999);
            }
        }

        disconnect();
    }

    private void testDefragAlways() throws Exception {
        if (config.memory || !autoCommit || pseudoReconnect) {
            return;
        }
        deleteDb(getTestName());

        String url = "jdbc:h2:" + getBaseDir() + "/" + getTestName() + ";DEFRAG_ALWAYS=true";
//        String url = "jdbc:h2:" + getBaseDir() + "/" + getTestName() + ";MAX_COMPACT_TIME=5000";
        connect(url);
        stat.execute("create table test(id bigint generated always as identity primary key, v int)");

        PreparedStatement prepInsert = conn.prepareStatement("insert into test(v) values ?");
        for (int i = 1; i <= 10; i++) {
            prepInsert.setInt(1, i);
            prepInsert.executeUpdate();
        }
        stat.execute("create restore point p1");

        reconnect(url);
        prepInsert = conn.prepareStatement("insert into test(v) values ?");
        for (int i = 11; i <= 20; i++) {
            prepInsert.setInt(1, i);
            prepInsert.executeUpdate();
        }
        stat.execute("create restore point p2");

        reconnect(url);
        prepInsert = conn.prepareStatement("insert into test(v) values ?");
        for (int i = 21; i <= 30; i++) {
            prepInsert.setInt(1, i);
            prepInsert.executeUpdate();
        }

        reconnect(url);
        assertSingleValue(stat, "select count(*) from test", 30);
        stat.execute("restore to point p2");
        assertSingleValue(stat, "select count(*) from test", 20);
        stat.execute("restore to point p1");
        assertSingleValue(stat, "select count(*) from test", 10);

        disconnect();
    }

    private void assertRestorePoints(Statement stat, String... restorePointNames) throws Exception {
        try (ResultSet r = stat.executeQuery("select * from information_schema.restore_points order by restore_point_name")) {
            List<String> actualRestorePointNames = new ArrayList<>();
            while (r.next()) {
                actualRestorePointNames.add(r.getString("restore_point_name"));
            }
            assertEquals(List.of(restorePointNames), actualRestorePointNames);
        }
    }

    private void printRestorePoints(Statement stat) throws Exception {
        try (ResultSet r = stat.executeQuery("select * from information_schema.restore_points order by restore_point_name")) {
            while (r.next()) {
                System.out.printf(
                        "restore_point_name=%s, created_at=%s, oldest_database_version_to_keep=%s, database_version=%s%n",
                        r.getString("restore_point_name"),
                        r.getObject("created_at", ZonedDateTime.class),
                        r.getLong("oldest_database_version_to_keep"),
                        r.getLong("database_version")
                );
            }
        }
    }
}
