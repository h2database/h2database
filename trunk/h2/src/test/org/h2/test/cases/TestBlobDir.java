/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.cases;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.tools.DeleteDbFiles;

/*
1) the trace has one function "Enabling the Trace Option at Runtime by
Manually Creating a File". how could I disable this function? I don't
want the user could use one file to trace my database info...
*/

/*

D:\data\h2\bin>java -Xrunhprof:cpu=samples,depth=8 org.h2.test.TestAll
Java: 1.5.0_10-b03, Java HotSpot(TM) Client VM, Sun Microsystems Inc.
Env: Windows XP, x86, 5.1, Service Pack 2, \ ; \r\n CH de  Cp1252
time to connect: 984ms
h2.lobFilesInDirectories: true
h2.lobFilesPerDirectory: 256
writing 1000: 5688ms
writing 2000: 4062ms
writing 3000: 5766ms
writing 4000: 3531ms
writing 5000: 3860ms
writing 6000: 8734ms
writing 7000: 3703ms
writing 8000: 3906ms
writing 9000: 11250ms
writing 10000: 3813ms
time to complete writing: 54313ms
Dumping CPU usage by sampling running threads ... done.

h2.lobFilesInDirectories: false
time to complete writing: 49828ms
time to complete reading: 2953ms
time to connect: 984ms

h2.lobFilesInDirectories: true
h2.lobFilesPerDirectory: 256
time to complete writing: 98687ms
time to complete reading: 2625ms
time to connect: 47ms

FIXED:
h2.lobFilesPerDirectory: 256
time to complete writing: 45656ms

h2.lobFilesPerDirectory: 1024
time to complete writing: 158204ms

h2.lobFilesInDirectories: false
time to complete writing: 17187ms

h2.lobFilesInDirectories: true
h2.lobFilesPerDirectory: 16
writing 1000: 5610ms
writing 2000: 1984ms
writing 3000: 3000ms
writing 4000: 6844ms
writing 5000: 7734ms
writing 6000: 11578ms
writing 7000: 6407ms
writing 8000: 6812ms
writing 9000: 15344ms
writing 10000: 7375ms
time to complete writing: 72688ms
time to complete writing: 74578ms
time to complete reading: 2734ms
time to connect: 47ms
*/
public class TestBlobDir {

    // Start h2 start parameters:
    // -Dh2.lobFilesInDirectories=true -Dh2.lobFilesPerDirectory=16

    public static void main(String[] args) throws Exception {
        System.setProperty("h2.lobFilesInDirectories", "true");
        System.setProperty("h2.lobFilesPerDirectory", "256");
        TestBlobDir blobtest = new TestBlobDir();
        DeleteDbFiles.execute(".", "testabc", true);
        Connection conn = blobtest.getConnection();
        long count;
        count = 10000;
        blobtest.printParameters(conn);
        blobtest.insertBlobs(conn, 1, count, 500);
//        blobtest.testBlobs(conn, 1, count);
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // test time to build up a new connection
//        conn = blobtest.getConnection();
//        try {
//            conn.close();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
    }

    public Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
//            String url = "jdbc:h2:tcp://localhost:9092/test123456789012345678901234567890";
//            String url = "jdbc:h2:test123456789012345678901234567890";
            String url = "jdbc:h2:testabc";
            Driver driver = DriverManager.getDriver(url);
            driver.toString();
            long start = System.currentTimeMillis();
            conn = DriverManager.getConnection(url, "sa", "");
            System.out.println("time to connect: " + (System.currentTimeMillis() - start + "ms"));

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return conn;
    }

    public void printParameters(Connection conn) {
        String sqlStmt = "SELECT * FROM INFORMATION_SCHEMA.SETTINGS WHERE NAME LIKE 'h2.lob%';";
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sqlStmt);
            while (rs.next()) {
                System.out.print(rs.getString("name") + ": ");
                System.out.println(rs.getString("value"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertBlobs(Connection conn, long from, long to, int blobLength) {
        byte[] byteblob = new byte[blobLength];
        for (int i = 0; i < byteblob.length; i++) {
            byteblob[i] = 'b';
        }
        // System.out.println("Inserting blobs of length " + blobLength + " from " + from + " to " + to);
        String sqlStmt = "INSERT INTO blobtable" + " (count, blobtest) VALUES (?1, ?2)";
        long startComulative = -1;
        PreparedStatement prepStmt = null;
        try {
            conn.createStatement().execute("DROP TABLE blobtable IF EXISTS");
            conn.createStatement().execute("CREATE TABLE blobtable(count INT4, blobtest BLOB, PRIMARY KEY (count))");
            prepStmt = conn.prepareStatement(sqlStmt);
            startComulative = System.currentTimeMillis();
            long start = System.currentTimeMillis();
            for (long i = 1; i <= to; i++) {
                prepStmt.setLong(1, i);
                InputStream blob = new ByteArrayInputStream(byteblob);
                prepStmt.setBinaryStream(2, blob, -1);
                prepStmt.execute();
                if (i % 1000 == 0) {
                    System.out.println("writing " + i + ": " + (System.currentTimeMillis() - start + "ms"));
                    start = System.currentTimeMillis();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("time to complete writing: " + (System.currentTimeMillis() - startComulative) + "ms");
        if (prepStmt != null) {
            try {
                prepStmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void testBlobs(Connection conn, long from, long to) {
        // System.out.println("Reading blobs" + " from " + from + " to " + to);
        ResultSet rs = null;
        byte[] data;
        BufferedInputStream imageInputStream = null;
        String sqlStmt = "SELECT * FROM blobtable where count=?1";
        long startComulative = -1;
        PreparedStatement prepStmt = null;
        try {
            prepStmt = conn.prepareStatement(sqlStmt);
            // long start = System.currentTimeMillis();
            startComulative = System.currentTimeMillis();
            for (long i = 1; i <= to; i++) {
                prepStmt.setLong(1, i);
                rs = prepStmt.executeQuery();
                if (rs.next()) {
                    int size = (int) rs.getBlob("blobtest").length();
                    imageInputStream = new BufferedInputStream(rs.getBinaryStream("blobtest"));
                    try {
                        DataInputStream in = new DataInputStream(imageInputStream);
                        data = new byte[size];
                        in.readFully(data);
                        in.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    System.out.println("error, no data");
                }
//                if (i % 1000 == 0) {
//                    System.out.println("reading " + i + ": " + (System.currentTimeMillis() - start + "ms"));
//                    start = System.currentTimeMillis();
//                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("time to complete reading: " + (System.currentTimeMillis() - startComulative) + "ms");
        return;
    }

}
