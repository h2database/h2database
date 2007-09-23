/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.h2.api.DatabaseEventListener;
import org.h2.test.TestBase;

public class TestDatabaseEventListener extends TestBase implements DatabaseEventListener {
    
    private boolean calledOpened, calledClosingDatabase;

    public void test() throws Exception {
        Properties p = new Properties();
        p.setProperty("user", "sa");
        p.setProperty("password", "sa");
        TestDatabaseEventListener l = new TestDatabaseEventListener();
        p.put("DATABASE_EVENT_LISTENER_OBJECT", l);
        org.h2.Driver.load();
        Connection conn = DriverManager.getConnection("jdbc:h2:mem:databaseEventListener", p);
        conn.close();
        check(l.calledOpened);
        check(l.calledClosingDatabase);
    }
    
    public void closingDatabase() {
        calledClosingDatabase = true;
    }

    public void diskSpaceIsLow(long stillAvailable) throws SQLException {
    }

    public void exceptionThrown(SQLException e, String sql) {
    }

    public void init(String url) {
    }

    public void opened() {
        calledOpened = true;
    }

    public void setProgress(int state, String name, int x, int max) {
    }

}
