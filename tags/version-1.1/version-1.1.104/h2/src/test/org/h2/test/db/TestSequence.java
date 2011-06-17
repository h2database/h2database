/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestBase;

/**
 * Tests the sequence feature of this database.
 */
public class TestSequence extends TestBase {

    public void test() throws SQLException {
        testAlterSequenceColumn();
        testAlterSequence();
        testCache();
        testTwo();
        deleteDb("sequence");
    }
    
    private void testAlterSequenceColumn() throws SQLException {
        deleteDb("sequence");
        Connection conn = getConnection("sequence");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT , NAME VARCHAR(255))");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        stat.execute("ALTER TABLE TEST ALTER COLUMN ID INT IDENTITY");
        stat.execute("ALTER TABLE test ALTER COLUMN ID RESTART WITH 3");
        stat.execute("INSERT INTO TEST (name) VALUES('Other World')");
        conn.close();
    }
    
    private void testAlterSequence() throws SQLException {
        deleteDb("sequence");
        Connection conn = getConnection("sequence");
        Statement stat = conn.createStatement();
        stat.execute("create sequence test");
        conn.setAutoCommit(false);
        stat.execute("alter sequence test restart with 1");
        for (int i = 0; i < 40; i++) {
            stat.execute("select nextval('test')");
        }
        conn.close();
    }

    private void testCache() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("sequence");
        Connection conn = getConnection("sequence");
        Statement stat = conn.createStatement();
        stat.execute("create sequence test_Sequence");
        stat.execute("create sequence test_Sequence3 cache 3");
        conn.close();
        conn = getConnection("sequence");
        stat = conn.createStatement();
        stat.execute("call next value for test_Sequence");
        stat.execute("call next value for test_Sequence3");
        ResultSet rs = stat.executeQuery("select * from information_schema.sequences order by sequence_name");
        rs.next();
        assertEquals(rs.getString("SEQUENCE_NAME"), "TEST_SEQUENCE");
        assertEquals(rs.getString("CACHE"), "32");
        rs.next();
        assertEquals(rs.getString("SEQUENCE_NAME"), "TEST_SEQUENCE3");
        assertEquals(rs.getString("CACHE"), "3");
        assertFalse(rs.next());
        conn.close();
    }

    private void testTwo() throws SQLException {
        deleteDb("sequence");
        Connection conn = getConnection("sequence");
        Statement stat = conn.createStatement();
        stat.execute("create sequence testSequence");
        conn.setAutoCommit(false);

        Connection conn2 = getConnection("sequence");
        Statement stat2 = conn2.createStatement();
        conn2.setAutoCommit(false);

        long last = 0;
        for (int i = 0; i < 100; i++) {
            long v1 = getNext(stat);
            assertTrue(v1 > last);
            last = v1;
            for (int j = 0; j < 100; j++) {
                long v2 = getNext(stat2);
                assertTrue(v2 > last);
                last = v2;
            }
        }

        conn2.close();
        conn.close();
    }

    private long getNext(Statement stat) throws SQLException {
        ResultSet rs = stat.executeQuery("call next value for testSequence");
        rs.next();
        long value = rs.getLong(1);
        return value;
    }
}
