/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.api.DatabaseEventListener;
import org.h2.test.TestBase;

public class TestListener extends TestBase implements DatabaseEventListener {
    
    private long last, start;
    
    public TestListener() {
        start = last = System.currentTimeMillis();
    }

    public void test() throws Exception {
        if(config.networked) {
            return;
        }
        deleteDb("listener");
        Connection conn;
        conn = getConnection("listener");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, 'Test' || SPACE(100))");
        int len = getSize(100, 100000);
        for(int i=0; i<len; i++) {
            prep.setInt(1, i);
            prep.execute();
        }
        crash(conn);
        
        conn = getConnection("listener;database_event_listener='" + getClass().getName() + "'");
        conn.close();
        
    }

    public void diskSpaceIsLow(long stillAvailable) throws SQLException {
        System.out.println("diskSpaceIsLow stillAvailable="+stillAvailable);
    }

    public void exceptionThrown(SQLException e, String sql) {
        TestBase.logError("exceptionThrown sql=" + sql, e);
    }

    public void setProgress(int state, String name, int current, int max) {
        long time = System.currentTimeMillis();
        if(time < last+1000) {
            return;
        }
        last = time;
        String stateName;
        switch(state) {
        case STATE_SCAN_FILE:
            stateName = "Scan " + name;
            break;
        case STATE_CREATE_INDEX:
            stateName = "Create Index " + name;
            break;
        case STATE_RECOVER:
            stateName = "Recover";
            break;
        default:
            TestBase.logError("unknown state: " + state, null);
            stateName = "? " + name;
        }
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
        }
        System.out.println("state: " + stateName + " " + (100*current/max) + " " + (time-start));
    }

    public void closingDatabase() {
    }

    public void init(String url) {
    }

}
