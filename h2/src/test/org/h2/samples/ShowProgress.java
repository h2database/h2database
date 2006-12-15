/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.api.DatabaseEventListener;
import org.h2.jdbc.JdbcConnection;

public class ShowProgress implements DatabaseEventListener {
    
    private long last, start;
    
    public ShowProgress() {
        start = last = System.currentTimeMillis();
    }

    public static void main(String[] args) throws Exception {
        new ShowProgress().test();
    }
    
    void test() throws Exception {
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection("jdbc:h2:test;LOG=2", "sa", "");
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, 'Test' || SPACE(100))");
        long time;
        time = System.currentTimeMillis();
        int len = 1000;
        for(int i=0; i<len; i++) {
            long last = System.currentTimeMillis();
            if(last > time+1000) {
                time = last;
                System.out.println("Inserting " + (100L*i/len) + "%");
            }
            prep.setInt(1, i);
            prep.execute();
        }
        boolean abnormalTermination = true;
        if(abnormalTermination) {
            ((JdbcConnection)conn).setPowerOffCount(1);
            try {
                stat.execute("INSERT INTO TEST VALUES(-1, 'Test' || SPACE(100))");
            } catch(SQLException e) {
            }
        } else {
            conn.close();
        }
        
        System.out.println("Open connection...");
        time = System.currentTimeMillis();
        conn = DriverManager.getConnection("jdbc:h2:test;LOG=2;DATABASE_EVENT_LISTENER='" + getClass().getName() + "'", "sa", "");
        time = System.currentTimeMillis() - time;
        System.out.println("Done after " + time + " ms");
        conn.close();
        
    }

    public void diskSpaceIsLow(long stillAvailable) throws SQLException {
        System.out.println("diskSpaceIsLow stillAvailable="+stillAvailable);
    }

    public void exceptionThrown(SQLException e) {
        e.printStackTrace();
    }

    public void setProgress(int state, String name, int current, int max) {
        long time = System.currentTimeMillis();
        if(time < last+5000) {
            return;
        }
        last = time;
        String stateName = "?";
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
        }
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
        }
        System.out.println("State: " + stateName + " " + (100*current/max) + "% (" + current+" of " + max + ") " + (time-start)+" ms");
    }

    public void closingDatabase() {
        System.out.println("Closing the database");
    }

    public void init(String url) {
        System.out.println("Initializing the event listener for database " + url);
    }

}
