/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import org.h2.message.Trace;
import org.h2.test.TestBase;
import org.h2.test.TestDb;
import org.h2.util.Cache;
import org.h2.util.CacheLRU;
import org.h2.util.CacheObject;
import org.h2.util.CacheWriter;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.value.Value;

/**
 * Tests the cache.
 */
public class TestCache extends TestDb implements CacheWriter {

    private String out;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
//        test.config.traceTest = true;
        test.testFromMain();
    }

    @Override
    public void test() throws Exception {
        testMemoryUsage();
        testCache();
        testCacheDb(false);
        testCacheDb(true);
    }

    private void testMemoryUsage() throws SQLException {
        if (!config.traceTest) {
            return;
        }
        if (config.memory) {
            return;
        }
        deleteDb("cache");
        Connection conn;
        Statement stat;
        ResultSet rs;
        conn = getConnection("cache;CACHE_SIZE=16384");
        stat = conn.createStatement();
        // test DataOverflow
        stat.execute("create table test(id int primary key, data varchar)");
        stat.execute("set max_memory_undo 10000");
        conn.close();
        stat = null;
        conn = null;
        long before = getRealMemory();

        conn = getConnection("cache;CACHE_SIZE=16384;DB_CLOSE_ON_EXIT=FALSE");
        stat = conn.createStatement();

        //  -XX:+HeapDumpOnOutOfMemoryError

        stat.execute(
                "insert into test select x, random_uuid() || space(1) " +
                "from system_range(1, 10000)");

        // stat.execute("create index idx_test_n on test(data)");
        // stat.execute("select data from test where data >= ''");

        rs = stat.executeQuery(
                "SELECT SETTING_VALUE FROM INFORMATION_SCHEMA.SETTINGS WHERE SETTING_NAME = 'info.CACHE_SIZE'");
        rs.next();
        int calculated = rs.getInt(1);
        rs = null;
        long afterInsert = getRealMemory();

        conn.close();
        stat = null;
        conn = null;
        long afterClose = getRealMemory();
        trace("Used memory: " + (afterInsert - afterClose) +
                " calculated cache size: " + calculated);
        trace("Before: " + before + " after: " + afterInsert +
                " after closing: " + afterClose);
    }

    private static long getRealMemory() {
        StringUtils.clearCache();
        Value.clearCache();
        return Utils.getMemoryUsed();
    }

    private void testCache() {
        out = "";
        Cache c = CacheLRU.getCache(this, "LRU", 16);
        for (int i = 0; i < 20; i++) {
            c.put(new Obj(i));
        }
        assertEquals("flush 0 flush 1 flush 2 flush 3 ", out);
    }

    /**
     * A simple cache object
     */
    static class Obj extends CacheObject {

        Obj(int pos) {
            setPos(pos);
        }

        @Override
        public int getMemory() {
            return 1024;
        }

        @Override
        public boolean canRemove() {
            return true;
        }

        @Override
        public boolean isChanged() {
            return true;
        }

        @Override
        public String toString() {
            return "[" + getPos() + "]";
        }

    }

    @Override
    public void flushLog() {
        out += "flush ";
    }

    @Override
    public Trace getTrace() {
        return null;
    }

    @Override
    public void writeBack(CacheObject entry) {
        out += entry.getPos() + " ";
    }

    private void testCacheDb(boolean lru) throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("cache");
        Connection conn = getConnection(
                "cache;CACHE_TYPE=" + (lru ? "LRU" : "SOFT_LRU"));
        Statement stat = conn.createStatement();
        stat.execute("SET CACHE_SIZE 1024");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("CREATE TABLE MAIN(ID INT PRIMARY KEY, NAME VARCHAR)");
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST VALUES(?, ?)");
        PreparedStatement prep2 = conn.prepareStatement(
                "INSERT INTO MAIN VALUES(?, ?)");
        int max = 10000;
        for (int i = 0; i < max; i++) {
            prep.setInt(1, i);
            prep.setString(2, "Hello " + i);
            prep.execute();
            prep2.setInt(1, i);
            prep2.setString(2, "World " + i);
            prep2.execute();
        }
        conn.close();
        conn = getConnection("cache");
        stat = conn.createStatement();
        stat.execute("SET CACHE_SIZE 1024");
        Random random = new Random(1);
        for (int i = 0; i < 100; i++) {
            stat.executeQuery("SELECT * FROM MAIN WHERE ID BETWEEN 40 AND 50");
            stat.executeQuery("SELECT * FROM MAIN WHERE ID = " + random.nextInt(max));
            if ((i % 10) == 0) {
                stat.executeQuery("SELECT * FROM TEST");
            }
        }
        conn.close();
        deleteDb("cache");
    }

}
