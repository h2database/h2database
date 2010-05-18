/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;
import org.h2.store.fs.FileObject;
import org.h2.store.fs.FileSystem;
import org.h2.test.TestBase;
import org.h2.tools.Csv;
import org.h2.util.IOUtils;
import org.h2.util.New;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * CSVREAD and CSVWRITE tests.
 *
 * @author Thomas Mueller
 * @author Sylvain Cuaz (testNull)
 *
 */
public class TestCsv extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        test.config.traceTest = true;
        test.test();
    }

    public void test() throws Exception {
        testPseudoBom();
        testWriteRead();
        testColumnNames();
        testSpaceSeparated();
        testNull();
        testRandomData();
        testEmptyFieldDelimiter();
        testFieldDelimiter();
        testAsTable();
        testRead();
        testPipe();
        deleteDb("csv");
    }

    private void testPseudoBom() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // UTF-8 "BOM" / marker
        out.write(Utils.convertStringToBytes("ef" + "bb" + "bf"));
        out.write("\"ID\", \"NAME\"\n1, Hello".getBytes("UTF-8"));
        byte[] buff = out.toByteArray();
        Reader r = new InputStreamReader(new ByteArrayInputStream(buff), "UTF-8");
        ResultSet rs = Csv.getInstance().read(r, null);
        assertEquals("ID", rs.getMetaData().getColumnLabel(1));
        assertEquals("NAME", rs.getMetaData().getColumnLabel(2));
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());
    }

    private void testColumnNames() throws Exception {
        ResultSet rs;
        rs = Csv.getInstance().read(new StringReader("Id,First Name,2x,_x2\n1,2,3"), null);
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("First Name", rs.getMetaData().getColumnName(2));
        assertEquals("2x", rs.getMetaData().getColumnName(3));
        assertEquals("_X2", rs.getMetaData().getColumnName(4));
    }

    private void testSpaceSeparated() throws SQLException {
        deleteDb("csv");
        File f = new File(getBaseDir() + "/testSpace.csv");
        IOUtils.delete(f.getAbsolutePath());

        Connection conn = getConnection("csv");
        Statement stat = conn.createStatement();
        stat.execute("create temporary table test (a int, b int, c int)");
        stat.execute("insert into test values(1,2,3)");
        stat.execute("insert into test values(4,null,5)");
        stat.execute("call csvwrite('"+getBaseDir()+"/test.tsv','select * from test',null,' ')");
        ResultSet rs1 = stat.executeQuery("select * from test");
        assertResultSetOrdered(rs1, new String[][]{new String[]{"1", "2", "3"}, new String[]{"4", null, "5"}});
        ResultSet rs2 = stat.executeQuery("select * from csvread('"+getBaseDir()+"/test.tsv',null,null,' ')");
        assertResultSetOrdered(rs2, new String[][]{new String[]{"1", "2", "3"}, new String[]{"4", null, "5"}});
        conn.close();
        IOUtils.delete(f.getAbsolutePath());
        IOUtils.delete(getBaseDir() + "/test.tsv");
    }

    /**
     * Test custom NULL string.
     */
    private void testNull() throws Exception {
        deleteDb("csv");

        String fileName = getBaseDir() + "/testNull.csv";
        FileSystem fs = FileSystem.getInstance(fileName);
        fs.delete(fileName);

        FileObject file = fs.openFileObject(fileName, "rw");
        String csvContent = "\"A\",\"B\",\"C\",\"D\"\n\\N,\"\",\"\\N\",";
        byte[] b = csvContent.getBytes("UTF-8");
        file.write(b, 0, b.length);
        file.close();
        Csv csv = Csv.getInstance();
        csv.setNullString("\\N");
        ResultSet rs = csv.read(file.getName(), null, "UTF8");
        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(4, meta.getColumnCount());
        assertEquals("A", meta.getColumnLabel(1));
        assertEquals("B", meta.getColumnLabel(2));
        assertEquals("C", meta.getColumnLabel(3));
        assertEquals("D", meta.getColumnLabel(4));
        assertTrue(rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals("", rs.getString(2));
        // null is never quoted
        assertEquals("\\N", rs.getString(3));
        // an empty string is always parsed as null
        assertEquals(null, rs.getString(4));
        assertFalse(rs.next());

        Connection conn = getConnection("csv");
        Statement stat = conn.createStatement();
        stat.execute("call csvwrite('" + file.getName() + "', 'select NULL as a, '''' as b, ''\\N'' as c, NULL as d', 'UTF8', ',', '\"', NULL, '\\N', '\n')");
        InputStreamReader reader = new InputStreamReader(fs.openFileInputStream(fileName));
        // on read, an empty string is treated like null,
        // but on write a null is always written with the nullString
        String data = IOUtils.readStringAndClose(reader, -1);
        assertEquals(csvContent + "\\N", data.trim());
        conn.close();

        fs.delete(fileName);
    }

    private void testRandomData() throws SQLException {
        deleteDb("csv");
        Connection conn = getConnection("csv");
        Statement stat = conn.createStatement();
        stat.execute("drop table if exists test");
        stat.execute("create table test(a varchar, b varchar)");
        int len = getSize(1000, 10000);
        PreparedStatement prep = conn.prepareStatement("insert into test values(?, ?)");
        ArrayList<String[]> list = New.arrayList();
        Random random = new Random(1);
        for (int i = 0; i < len; i++) {
            String a = randomData(random), b = randomData(random);
            prep.setString(1, a);
            prep.setString(2, b);
            list.add(new String[]{a, b});
            prep.execute();
        }
        stat.execute("CALL CSVWRITE('" + getBaseDir() + "/test.csv', 'SELECT * FROM test', 'UTF-8', '|', '#')");
        Csv csv = Csv.getInstance();
        csv.setFieldSeparatorRead('|');
        csv.setFieldDelimiter('#');
        ResultSet rs = csv.read(getBaseDir() + "/test.csv", null, "UTF-8");
        for (int i = 0; i < len; i++) {
            assertTrue(rs.next());
            String[] pair = list.get(i);
            assertEquals(pair[0], rs.getString(1));
            assertEquals(pair[1], rs.getString(2));
        }
        assertFalse(rs.next());
        conn.close();
        IOUtils.delete(getBaseDir() + "/test.csv");
    }

    private String randomData(Random random) {
        if (random.nextInt(10) == 1) {
            return null;
        }
        int len = random.nextInt(5);
        StringBuilder buff = new StringBuilder();
        String chars = "\\\'\",\r\n\t ;.-123456|#";
        for (int i = 0; i < len; i++) {
            buff.append(chars.charAt(random.nextInt(chars.length())));
        }
        return buff.toString();
    }

    private void testEmptyFieldDelimiter() throws Exception {
        String fileName = getBaseDir() + "/test.csv";
        IOUtils.delete(fileName);
        Connection conn = getConnection("csv");
        Statement stat = conn.createStatement();
        stat.execute("call csvwrite('"+fileName+"', 'select 1 id, ''Hello'' name', null, '|', '', null, null, chr(10))");
        InputStreamReader reader = new InputStreamReader(IOUtils.openFileInputStream(fileName));
        String text = IOUtils.readStringAndClose(reader, -1).trim();
        text = StringUtils.replaceAll(text, "\n", " ");
        assertEquals("ID|NAME 1|Hello", text);
        ResultSet rs = stat.executeQuery("select * from csvread('" + fileName + "', null, null, '|', '')");
        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(2, meta.getColumnCount());
        assertEquals("ID", meta.getColumnLabel(1));
        assertEquals("NAME", meta.getColumnLabel(2));
        assertTrue(rs.next());
        assertEquals("1", rs.getString(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());
        conn.close();
        IOUtils.delete(fileName);
    }

    private void testFieldDelimiter() throws Exception {
        String fileName = getBaseDir() + "/test.csv";
        String fileName2 = getBaseDir() + "/test2.csv";
        FileSystem fs = FileSystem.getInstance(fileName);
        fs.delete(fileName);
        FileObject file = fs.openFileObject(fileName, "rw");
        byte[] b = "'A'; 'B'\n\'It\\'s nice\'; '\nHello\\*\n'".getBytes();
        file.write(b, 0, b.length);
        file.close();
        Connection conn = getConnection("csv");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select * from csvread('" + fileName + "', null, null, ';', '''', '\\')");
        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(2, meta.getColumnCount());
        assertEquals("A", meta.getColumnLabel(1));
        assertEquals("B", meta.getColumnLabel(2));
        assertTrue(rs.next());
        assertEquals("It's nice", rs.getString(1));
        assertEquals("\nHello*\n", rs.getString(2));
        assertFalse(rs.next());
        stat.execute("call csvwrite('" + fileName2 + "', 'select * from csvread(''" + fileName + "'', null, null, '';'', '''''''', ''\\'')', null, '+', '*', '#')");
        rs = stat.executeQuery("select * from csvread('" + fileName2 + "', null, null, '+', '*', '#')");
        meta = rs.getMetaData();
        assertEquals(2, meta.getColumnCount());
        assertEquals("A", meta.getColumnLabel(1));
        assertEquals("B", meta.getColumnLabel(2));
        assertTrue(rs.next());
        assertEquals("It's nice", rs.getString(1));
        assertEquals("\nHello*\n", rs.getString(2));
        assertFalse(rs.next());
        conn.close();
        fs.delete(fileName);
        fs.delete(fileName2);
    }

    private void testPipe() throws SQLException {
        deleteDb("csv");
        Connection conn = getConnection("csv");
        Statement stat = conn.createStatement();
        stat.execute("call csvwrite('" + getBaseDir() + "/test.csv', 'select 1 id, ''Hello'' name', 'utf-8', '|')");
        ResultSet rs = stat.executeQuery("select * from csvread('" + getBaseDir() + "/test.csv', null, 'utf-8', '|')");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());
        new File(getBaseDir() + "/test.csv").delete();

        // PreparedStatement prep = conn.prepareStatement("select * from
        // csvread(?, null, ?, ?)");
        // prep.setString(1, BASE_DIR+"/test.csv");
        // prep.setString(2, "utf-8");
        // prep.setString(3, "|");
        // rs = prep.executeQuery();

        conn.close();
        IOUtils.delete(getBaseDir() + "/test.csv");
    }

    private void testAsTable() throws SQLException {
        deleteDb("csv");
        Connection conn = getConnection("csv");
        Statement stat = conn.createStatement();
        stat.execute("call csvwrite('" + getBaseDir() + "/test.csv', 'select 1 id, ''Hello'' name')");
        ResultSet rs = stat.executeQuery("select name from csvread('" + getBaseDir() + "/test.csv')");
        assertTrue(rs.next());
        assertEquals("Hello", rs.getString(1));
        assertFalse(rs.next());
        rs = stat.executeQuery("call csvread('" + getBaseDir() + "/test.csv')");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());
        new File(getBaseDir() + "/test.csv").delete();
        conn.close();
    }

    private void testRead() throws Exception {
        String fileName = getBaseDir() + "/test.csv";
        FileSystem fs = FileSystem.getInstance(fileName);
        fs.delete(fileName);
        FileObject file = fs.openFileObject(fileName, "rw");
        byte[] b = "a,b,c,d\n201,-2,0,18\n, \"abc\"\"\" ,,\"\"\n 1 ,2 , 3, 4 \n5, 6, 7, 8".getBytes();
        file.write(b, 0, b.length);
        file.close();
        ResultSet rs = Csv.getInstance().read(fileName, null, "UTF8");
        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(4, meta.getColumnCount());
        assertEquals("A", meta.getColumnLabel(1));
        assertEquals("B", meta.getColumnLabel(2));
        assertEquals("C", meta.getColumnLabel(3));
        assertEquals("D", meta.getColumnLabel(4));
        assertTrue(rs.next());
        assertEquals("201", rs.getString(1));
        assertEquals("-2", rs.getString(2));
        assertEquals("0", rs.getString(3));
        assertEquals("18", rs.getString(4));
        assertTrue(rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals("abc\"", rs.getString(2));
        assertEquals(null, rs.getString(3));
        assertEquals("", rs.getString(4));
        assertTrue(rs.next());
        assertEquals("1", rs.getString(1));
        assertEquals("2", rs.getString(2));
        assertEquals("3", rs.getString(3));
        assertEquals("4", rs.getString(4));
        assertTrue(rs.next());
        assertEquals("5", rs.getString(1));
        assertEquals("6", rs.getString(2));
        assertEquals("7", rs.getString(3));
        assertEquals("8", rs.getString(4));
        assertFalse(rs.next());

        // a,b,c,d
        // 201,-2,0,18
        // 201,2,0,18
        // 201,2,0,18
        // 201,2,0,18
        // 201,2,0,18
        // 201,2,0,18
        fs.delete(fileName);
    }

    private void testWriteRead() throws SQLException {
        deleteDb("csv");
        Connection conn = getConnection("csv");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID IDENTITY, NAME VARCHAR)");
        // int len = 100000;
        int len = 100;
        for (int i = 0; i < len; i++) {
            stat.execute("INSERT INTO TEST(NAME) VALUES('Ruebezahl')");
        }
        long time;
        time = System.currentTimeMillis();
        Csv.getInstance().write(conn, getBaseDir() + "/testRW.csv", "SELECT X ID, 'Ruebezahl' NAME FROM SYSTEM_RANGE(1, " + len + ")", "UTF8");
        trace("write: " + (System.currentTimeMillis() - time));
        ResultSet rs;
        time = System.currentTimeMillis();
        for (int i = 0; i < 30; i++) {
            rs = Csv.getInstance().read(getBaseDir() + "/testRW.csv", null, "UTF8");
            while (rs.next()) {
                // ignore
            }
        }
        trace("read: " + (System.currentTimeMillis() - time));
        rs = Csv.getInstance().read(getBaseDir() + "/testRW.csv", null, "UTF8");
        // stat.execute("CREATE ALIAS CSVREAD FOR \"org.h2.tools.Csv.read\"");
        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(2, meta.getColumnCount());
        for (int i = 0; i < len; i++) {
            rs.next();
            assertEquals("" + (i + 1), rs.getString("ID"));
            assertEquals("Ruebezahl", rs.getString("NAME"));
        }
        assertFalse(rs.next());
        rs.close();
        conn.close();
        IOUtils.delete(getBaseDir() + "/testRW.csv");
    }

}
