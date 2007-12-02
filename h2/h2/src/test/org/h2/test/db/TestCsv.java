/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.File;
import java.io.FileReader;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import org.h2.test.TestBase;
import org.h2.tools.Csv;
import org.h2.util.IOUtils;
import org.h2.util.StringUtils;

public class TestCsv extends TestBase {

    public void test() throws Exception {
        testEmptyFieldDelimiter();
        testFieldDelimiter();
        testAsTable();
        testWriteRead();
        testRead();
        testPipe();
    }
    
    private void testEmptyFieldDelimiter() throws Exception {
        File f = new File(baseDir + "/test.csv");
        f.delete();
        Connection conn = getConnection("csv");
        Statement stat = conn.createStatement();
        stat.execute("call csvwrite('"+baseDir+"/test.csv', 'select 1 id, ''Hello'' name', null, '|', '')");
        FileReader reader = new FileReader(baseDir + "/test.csv");
        String text = IOUtils.readStringAndClose(reader, -1).trim();
        text = StringUtils.replaceAll(text, "\r", "");
        text = StringUtils.replaceAll(text, "\n", " ");
        check("ID|NAME 1|Hello", text);
        ResultSet rs = stat.executeQuery("select * from csvread('" + baseDir + "/test.csv', null, null, '|', '')");
        ResultSetMetaData meta = rs.getMetaData();
        check(meta.getColumnCount(), 2);
        check(meta.getColumnLabel(1), "ID");
        check(meta.getColumnLabel(2), "NAME");
        check(rs.next());
        check(rs.getString(1), "1");
        check(rs.getString(2), "Hello");
        checkFalse(rs.next());
        conn.close();
    }

    private void testFieldDelimiter() throws Exception {
        File f = new File(baseDir + "/test.csv");
        f.delete();
        RandomAccessFile file = new RandomAccessFile(f, "rw");
        file.write("'A'; 'B'\n\'It\\'s nice\'; '\nHello\\*\n'".getBytes());
        file.close();
        Connection conn = getConnection("csv");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select * from csvread('" + baseDir + "/test.csv', null, null, ';', '''', '\\')");
        ResultSetMetaData meta = rs.getMetaData();
        check(meta.getColumnCount(), 2);
        check(meta.getColumnLabel(1), "A");
        check(meta.getColumnLabel(2), "B");
        check(rs.next());
        check(rs.getString(1), "It's nice");
        check(rs.getString(2), "\nHello*\n");
        checkFalse(rs.next());
        stat.execute("call csvwrite('" + baseDir + "/test2.csv', 'select * from csvread(''" + baseDir + "/test.csv'', null, null, '';'', '''''''', ''\\'')', null, '+', '*', '#')");
        rs = stat.executeQuery("select * from csvread('" + baseDir + "/test2.csv', null, null, '+', '*', '#')");
        meta = rs.getMetaData();
        check(meta.getColumnCount(), 2);
        check(meta.getColumnLabel(1), "A");
        check(meta.getColumnLabel(2), "B");
        check(rs.next());
        check(rs.getString(1), "It's nice");
        check(rs.getString(2), "\nHello*\n");
        checkFalse(rs.next());
        conn.close();
    }

    private void testPipe() throws Exception {
        deleteDb("csv");
        Connection conn = getConnection("csv");
        Statement stat = conn.createStatement();
        stat.execute("call csvwrite('" + baseDir + "/test.csv', 'select 1 id, ''Hello'' name', 'utf-8', '|')");
        ResultSet rs = stat.executeQuery("select * from csvread('" + baseDir + "/test.csv', null, 'utf-8', '|')");
        check(rs.next());
        check(rs.getInt(1), 1);
        check(rs.getString(2), "Hello");
        checkFalse(rs.next());
        new File(baseDir + "/test.csv").delete();

        // PreparedStatement prep = conn.prepareStatement("select * from
        // csvread(?, null, ?, ?)");
        // prep.setString(1, BASE_DIR+"/test.csv");
        // prep.setString(2, "utf-8");
        // prep.setString(3, "|");
        // rs = prep.executeQuery();

        conn.close();
    }

    private void testAsTable() throws Exception {
        deleteDb("csv");
        Connection conn = getConnection("csv");
        Statement stat = conn.createStatement();
        stat.execute("call csvwrite('" + baseDir + "/test.csv', 'select 1 id, ''Hello'' name')");
        ResultSet rs = stat.executeQuery("select name from csvread('" + baseDir + "/test.csv')");
        check(rs.next());
        check(rs.getString(1), "Hello");
        checkFalse(rs.next());
        rs = stat.executeQuery("call csvread('" + baseDir + "/test.csv')");
        check(rs.next());
        check(rs.getInt(1), 1);
        check(rs.getString(2), "Hello");
        checkFalse(rs.next());
        new File(baseDir + "/test.csv").delete();
        conn.close();
    }

    public void testRead() throws Exception {
        File f = new File(baseDir + "/test.csv");
        f.delete();
        RandomAccessFile file = new RandomAccessFile(f, "rw");
        file.write("a,b,c,d\n201,-2,0,18\n, \"abc\"\"\" ,,\"\"\n 1 ,2 , 3, 4 \n5, 6, 7, 8".getBytes());
        file.close();
        ResultSet rs = Csv.getInstance().read(baseDir + "/test.csv", null, "UTF8");
        ResultSetMetaData meta = rs.getMetaData();
        check(meta.getColumnCount(), 4);
        check(meta.getColumnLabel(1), "a");
        check(meta.getColumnLabel(2), "b");
        check(meta.getColumnLabel(3), "c");
        check(meta.getColumnLabel(4), "d");
        check(rs.next());
        check(rs.getString(1), "201");
        check(rs.getString(2), "-2");
        check(rs.getString(3), "0");
        check(rs.getString(4), "18");
        check(rs.next());
        check(rs.getString(1), null);
        check(rs.getString(2), "abc\"");
        check(rs.getString(3), null);
        check(rs.getString(4), "");
        check(rs.next());
        check(rs.getString(1), "1");
        check(rs.getString(2), "2");
        check(rs.getString(3), "3");
        check(rs.getString(4), "4");
        check(rs.next());
        check(rs.getString(1), "5");
        check(rs.getString(2), "6");
        check(rs.getString(3), "7");
        check(rs.getString(4), "8");
        checkFalse(rs.next());

        // a,b,c,d
        // 201,-2,0,18
        // 201,2,0,18
        // 201,2,0,18
        // 201,2,0,18
        // 201,2,0,18
        // 201,2,0,18
    }

    public void testWriteRead() throws Exception {

        deleteDb("csv");

        Connection conn = getConnection("csv");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID IDENTITY, NAME VARCHAR)");
        int len = 100;
        for (int i = 0; i < len; i++) {
            stat.execute("INSERT INTO TEST(NAME) VALUES('Ruebezahl')");
        }
        Csv.getInstance().write(conn, baseDir + "/testRW.csv", "SELECT * FROM TEST", "UTF8");
        ResultSet rs = Csv.getInstance().read(baseDir + "/testRW.csv", null, "UTF8");
        // stat.execute("CREATE ALIAS CSVREAD FOR \"org.h2.tools.Csv.read\"");
        ResultSetMetaData meta = rs.getMetaData();
        check(2, meta.getColumnCount());
        for (int i = 0; i < len; i++) {
            rs.next();
            check(rs.getString("ID"), "" + (i + 1));
            check(rs.getString("NAME"), "Ruebezahl");
        }
        checkFalse(rs.next());
        rs.close();
        conn.close();

    }

}
