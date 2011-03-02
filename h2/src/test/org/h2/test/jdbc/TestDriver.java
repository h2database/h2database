/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.h2.Driver;
import org.h2.test.TestBase;

/**
 * Tests the database driver.
 */
public class TestDriver extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        testSettingsAsProperties();
        testDriverObject();
    }

    private void testSettingsAsProperties() throws Exception {
        Properties props = new Properties();
        props.put("user", getUser());
        props.put("password", getPassword());
        props.put("max_compact_time", "1234");
        props.put("unknown", "1234");
        String url = getURL("driver", true);
        Connection conn = DriverManager.getConnection(url, props);
        ResultSet rs;
        rs = conn.createStatement().executeQuery(
                "select * from information_schema.settings where name='MAX_COMPACT_TIME'");
        rs.next();
        assertEquals(1234, rs.getInt(2));
        conn.close();
    }

    private void testDriverObject() throws Exception {
        Driver instance = Driver.load();
        assertTrue(DriverManager.getDriver("jdbc:h2:~/test") == instance);
        Driver.unload();
        try {
            java.sql.Driver d = DriverManager.getDriver("jdbc:h2:~/test");
            fail(d.toString());
        } catch (SQLException e) {
            // ignore
        }
        Driver.load();
        assertTrue(DriverManager.getDriver("jdbc:h2:~/test") == instance);
    }

}
