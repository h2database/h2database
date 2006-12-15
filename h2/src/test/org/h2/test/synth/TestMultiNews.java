/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.sql.*;

public class TestMultiNews extends TestMultiThread {
    
    private static final String PREFIX_URL =  "http://feeds.wizbangblog.com/WizbangFullFeed?m=";
    
    int len = 10000;
    Connection conn;

    TestMultiNews(TestMulti base) throws SQLException {
        super(base);
        conn = base.getConnection();
    }

    void operation() throws SQLException {
        if(random.nextInt(10)==0) {
            conn.close();
            conn = base.getConnection();
        } else if(random.nextInt(10)==0) {
            if(random.nextBoolean()) {
                conn.commit();
            } else {
                conn.rollback();
            }
        } else if(random.nextInt(10)==0) {
            conn.setAutoCommit(random.nextBoolean());
        } else {
            if(random.nextBoolean()) {
                PreparedStatement prep;
                if(random.nextBoolean()) {
                    prep = conn.prepareStatement(
                        "SELECT * FROM NEWS WHERE FLINK = ?");
                } else {
                    prep = conn.prepareStatement(
                        "SELECT * FROM NEWS WHERE FVALUE = ?");
                }
                prep.setString(1, PREFIX_URL + random.nextInt(len));
                ResultSet rs = prep.executeQuery();
                if(!rs.next()) {
                    throw new SQLException("expected one row, got none");
                }
                if(rs.next()) {
                    throw new SQLException("expected one row, got more");
                }
            } else {
                PreparedStatement prep = conn.prepareStatement(
                        "UPDATE NEWS SET FSTATE = ? WHERE FID = ?");
                prep.setInt(1, random.nextInt(100));
                prep.setInt(2, random.nextInt(len));
                int count = prep.executeUpdate();
                if(count != 1) {
                    throw new SQLException("expected one row, got " + count);
                }
            }
        }
    }

    void begin() throws SQLException {
    }

    void end() throws SQLException {
        conn.close();
    }

    void finalTest() throws Exception {
    }

    void first() throws SQLException {
        Connection conn = base.getConnection();
        Statement stat = conn.createStatement();
        stat.execute(
                "CREATE TABLE TEST (ID IDENTITY, NAME VARCHAR)");
        stat.execute(
                "CREATE TABLE NEWS (FID NUMERIC(19) PRIMARY KEY, FCOMMENTS LONGVARCHAR, " +
                "FLINK VARCHAR(255), FSTATE INTEGER, FVALUE VARCHAR(255))");
        stat.execute(
        "CREATE INDEX IF NOT EXISTS NEWS_GUID_VALUE_INDEX ON NEWS(FVALUE)");
        stat.execute(
                "CREATE INDEX IF NOT EXISTS NEWS_LINK_INDEX ON NEWS(FLINK)");
        stat.execute(
                "CREATE INDEX IF NOT EXISTS NEWS_STATE_INDEX ON NEWS(FSTATE)");
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO NEWS (FID, FCOMMENTS, FLINK, FSTATE, FVALUE) VALUES " +
                "(?, ?, ?, ?, ?) ");
        PreparedStatement prep2 = conn.prepareStatement(
                "INSERT INTO TEST (NAME) VALUES (?)");
        for(int i=0; i<len; i++) {
            int x = random.nextInt(10) * 128;
            StringBuffer buff = new StringBuffer();
            while(buff.length() < x) {
                buff.append("Test ");
                buff.append(buff.length());
                buff.append(' ');
            }
            String comment = buff.toString();
            prep.setInt(1, i); // FID
            prep.setString(2, comment); // FCOMMENTS
            prep.setString(3, PREFIX_URL + i); // FLINK
            prep.setInt(4, 0); // FSTATE
            prep.setString(5, PREFIX_URL + i); // FVALUE
            prep.execute();
            prep2.setString(1, comment);
            prep2.execute();
        }
    }

}
