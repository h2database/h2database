/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import org.h2.test.TestBase;
import org.h2.engine.ConnectionInfo;

import java.util.Properties;

/**
 * Test the ConnectionInfo class.
 *
 * @author Kerry Sainsbury
 */
public class TestConnectionInfo extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        Properties info = new Properties();
        ConnectionInfo connectionInfo = new ConnectionInfo(
                "jdbc:h2:mem:testdb" +
                        ";LOG=2" +
                        ";ACCESS_MODE_DATA=rws" +
                        ";INIT=CREATE this...\\;INSERT that..." +
                        ";IFEXISTS=TRUE",
                info);

        assertEquals("jdbc:h2:mem:testdb", connectionInfo.getURL());

        assertEquals("2", connectionInfo.getProperty("LOG", ""));
        assertEquals("rws", connectionInfo.getProperty("ACCESS_MODE_DATA", ""));
        assertEquals("CREATE this...;INSERT that...", connectionInfo.getProperty("INIT", ""));
        assertEquals("TRUE", connectionInfo.getProperty("IFEXISTS", ""));
        assertEquals("undefined", connectionInfo.getProperty("CACHE_TYPE", "undefined"));
    }

}
