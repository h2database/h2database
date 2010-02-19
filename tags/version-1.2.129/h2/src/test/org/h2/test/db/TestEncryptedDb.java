/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
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
 * Test using an encrypted database.
 */
public class TestEncryptedDb extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws SQLException {
        if (config.memory || config.cipher != null) {
            return;
        }
        deleteDb("encrypted");
        Connection conn = getConnection("encrypted;CIPHER=AES", "sa", "123 123");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT)");
        stat.execute("CHECKPOINT");
        stat.execute("SET WRITE_DELAY 0");
        stat.execute("INSERT INTO TEST VALUES(1)");
        stat.execute("SHUTDOWN IMMEDIATELY");
        try {
            conn.close();
        } catch (SQLException e) {
            assertKnownException(e);
        }

        try {
            getConnection("encrypted;CIPHER=AES", "sa", "1234 1234");
            fail();
        } catch (SQLException e) {
            assertKnownException(e);
        }

        conn = getConnection("encrypted;CIPHER=AES", "sa", "123 123");
        stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());

        conn.close();
        deleteDb("encrypted");
    }

}
