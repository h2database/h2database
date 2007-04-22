/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.cases;

import java.sql.*;
import java.util.*;
import java.text.*;

public class TestCalendar {
    static Connection conn;
    static Statement stat;

    public static void main(String[] args) throws Exception {
        // Firstly, demonstrate Java behaviour with 'illegal' times:

        // Change default timezone...same as setting the Windows timezone in Control Panel.
//        TimeZone.setDefault(TimeZone.getTimeZone("PST"));
        TimeZone.setDefault(TimeZone.getTimeZone("Australia/Melbourne"));

        // String s;
        // Timestamp t;
        //
        // System.out.println("Watch carefully: this is what Java does with 'illegal' local times:");
        //
        // s = "2006-10-29 01:30:00"; // Valid
        // t = Timestamp.valueOf(s);
        // System.out.println("Instantiated: " + s + " Local: " + t.toString());
        //
        // s = "2006-10-29 01:59:59"; // Valid
        // t = Timestamp.valueOf(s);
        // System.out.println("Instantiated: " + s + " Output: " + t.toString());
        //
        // s = "2006-10-29 02:00:00"; // Illegal time...does not 'exist'
        // t = Timestamp.valueOf(s);
        // System.out.println("Instantiated: " + s + " Output: " + t.toString());
        //
        // s = "2006-10-29 02:30:00"; // Illegal time...does not 'exist'
        // t = Timestamp.valueOf(s);
        // System.out.println("Instantiated: " + s + " Output: " + t.toString());
        //
        // s = "2006-10-29 02:59:59"; // Illegal time...does not 'exist'
        // t = Timestamp.valueOf(s);
        // System.out.println("Instantiated: " + s + " Output: " + t.toString());
        //
        // s = "2006-10-29 03:00:00"; // Valid again
        // t = Timestamp.valueOf(s);
        // System.out.println("Instantiated: " + s + " Output: " + t.toString());
        //
        // System.out.println();
        // System.out.println();

        // String url = "jdbc:pervasive://maximus:1583/FILD602";
        // String driver = "com.pervasive.jdbc.v2.Driver";
        // String uid = "Master"; String pwd = "master";
        // String createquery = "create table TT (RECID INTEGER, MYDATETIME DATETIME, PRIMARY KEY(RECID))";
        // String insertquery = "insert into TT values(1, '2006-10-29 02:00:00')";
        // String selectquery = "select MYDATETIME from TT";

//        String url = "jdbc:h2:mem:";
//        String driver = "org.h2.Driver";
//        String user = "sa";
//        String password = "sa";
        
        String url = "jdbc:h2:mem:";
        String driver = "org.h2.Driver";
        String user = "sa";
        String password = "sa";
        testDb(driver, url, user, password);
        
        testDb("org.postgresql.Driver", "jdbc:postgresql:jpox2", "sa", "sa");

        // String url = "jdbc:sybase:Tds:atlas:2638/toptier";
        // String driver = "com.sybase.jdbc3.jdbc.SybDriver";
        // String uid = "dba"; String pwd = "sql";
        // String createquery = "create table TT (RECID INTEGER, MYDATETIME DATETIME, PRIMARY KEY(RECID))";
        // String insertquery = "insert into TT values(1, '2006-10-29 02:00:00')";
        // String selectquery = "select MYDATETIME from TT";
    }

    private static void testDb(String driver, String url, String user, String password) throws Exception {
        Class.forName(driver);

        conn = DriverManager.getConnection(url, user, password);
        stat = conn.createStatement();

        String createquery = "create table TT (RECID INTEGER, MYDATETIME TIMESTAMP,  PRIMARY KEY(RECID))";
//        String insertquery = "insert into TT values(1, '2006-10-28T16:00:00')";
//        String insertquery = "insert into TT values(1, '2006-10-28T16:00:00+00:00')";
//        String insertquery = "insert into TT values(1, '2006-10-28T16:00:00')";
//        String insertquery = "insert into TT values(1, '2006-01-28T16:00:00')";
      String insertquery = "insert into TT values(1, '2006-01-20T16:00:00+11:00')";
//        String insertquery = "insert into TT values(1, '2006-10-28T16:00:00+10:00')";
//        String insertquery = "insert into TT values(1, '2006-10-28T16:00:00+09:00')";
        String selectquery = "select MYDATETIME from TT";

        try {
            stat.execute("DROP TABLE TT");
        } catch (SQLException e) {
            // ignore
        }
        stat.execute(createquery);
        stat.execute("delete from TT");
        stat.execute(insertquery);

        System.out.println("Database timestamp retrieval test.");

        ResultSet results = stat.executeQuery(selectquery);

        if (results != null) {
            Calendar c = Calendar.getInstance();
            DateFormat df;
            c.setTimeZone(TimeZone.getTimeZone("UTC"));

            while (results.next()) {
                // Firstly exercise the default behaviours
                // Because the value in the database is an 'illegal' local time in Melbourne
                // we cannot expect Java to output it correctly. Java 1.5 adds an hour to it to make it a 'legal'
                // time.

                Timestamp t = results.getTimestamp(1);
                System.out.println("Interpret db value as default tz: " + t.toString());
                df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"); // DateFormat.getDateTimeInstance();
                System.out.println("Interpret db value as default: " + df.format(t) + "   <<<Should display as UTC, not local");

                // Second test: instruct the jdbc driver to
                // interpret the time as a UTC time, in which case it IS legal.
                // It should be output as the correct UTC value (i.e. same as what's in the database)

                t = results.getTimestamp(1, c);
//                df.setCalendar(c);
                System.out.println("Interpret db value as UTC: " + df.format(t) + "   <<<Should display as UTC, not local");

                df.setCalendar(c);
                System.out.println("Interpret db value as UTC: " + df.format(t) + "   <<<Should display as UTC, not local");

            }
        }
        stat.close();
        conn.close();

    }
}
