/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html). 
 * Initial Developer: H2 Group 
 */
package org.h2.test.db;

import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;

import org.h2.test.TestBase;
import org.h2.util.IOUtils;
import org.h2.util.StringUtils;

/**
 * @author Thomas
 */

public class TestLob extends TestBase {

    public void test() throws Exception {
        if(config.memory) {
            return;
        }
        testLobCopy();
        testLobHibernate();
        testLobCopy(false);
        testLobCopy(true);
        testLobCompression(false);
        testLobCompression(true);
        testManyLobs();
        testClob();
        testUpdateLob();
        testLobReconnect();
        testLob(false);
        testLob(true);
        testJavaObject();
    }
    
    private void testLobCopy() throws Exception {
        deleteDb("lob");
        Connection conn = reconnect(null);             
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int, data clob)");
        stat.execute("insert into test values(1, space(1000));");
        stat.execute("insert into test values(2, space(1000));");
        stat.execute("create table test2(id int, data clob);");
        stat.execute("insert into test2 select * from test;");
        stat.execute("drop table test;");
        stat.execute("select * from test2;");
        stat.execute("update test2 set id=id;");
        stat.execute("select * from test2;");
        conn.close();
    }
    
    private void testLobHibernate() throws Exception {
        deleteDb("lob");
        Connection conn0 = reconnect(null);     

        conn0.getAutoCommit();
        conn0.setAutoCommit(false);
        DatabaseMetaData dbMeta0 = 
        conn0.getMetaData();
        dbMeta0.getDatabaseProductName();
        dbMeta0.getDatabaseMajorVersion();
        dbMeta0.getDatabaseProductVersion();
        dbMeta0.getDriverName();
        dbMeta0.getDriverVersion();
        dbMeta0.supportsResultSetType(1004);
        dbMeta0.supportsBatchUpdates();
        dbMeta0.dataDefinitionCausesTransactionCommit();
        dbMeta0.dataDefinitionIgnoredInTransactions();
        dbMeta0.supportsGetGeneratedKeys();
        conn0.getAutoCommit();
        conn0.getAutoCommit();
        conn0.commit();
        conn0.setAutoCommit(true);
        Statement stat0 = 
        conn0.createStatement();
        stat0.executeUpdate("drop table CLOB_ENTITY if exists");
        stat0.getWarnings();
        stat0.executeUpdate("create table CLOB_ENTITY (ID bigint not null, SER_DATA clob, CLOB_DATA clob, primary key (ID))");
        stat0.getWarnings();
        stat0.close();
        conn0.getWarnings();
        conn0.clearWarnings();
        conn0.setAutoCommit(false);
        conn0.getAutoCommit();
        conn0.getAutoCommit();
        PreparedStatement prep0 = 
        conn0.prepareStatement("select max(ID) from CLOB_ENTITY");
        ResultSet rs0 = 
        prep0.executeQuery();
        rs0.next();
        rs0.getLong(1);
        rs0.wasNull();
        rs0.close();
        prep0.close();
        conn0.getAutoCommit();
        PreparedStatement prep1 = 
        conn0.prepareStatement("insert into CLOB_ENTITY (SER_DATA, CLOB_DATA, ID) values (?, ?, ?)");
        prep1.setNull(1, 2005);
        StringBuffer buff = new StringBuffer(10000);
        for(int i=0; i<10000; i++) {
            buff.append((char)('0' + (i%10)));
        }
        Reader x = new StringReader(buff.toString());
        prep1.setCharacterStream(2, x, 10000);
        prep1.setLong(3, 1);
        prep1.addBatch();
        prep1.executeBatch();
        prep1.close();
        conn0.getAutoCommit();
        conn0.getAutoCommit();
        conn0.commit();
        conn0.isClosed();
        conn0.getWarnings();
        conn0.clearWarnings();
        conn0.getAutoCommit();
        conn0.getAutoCommit();
        PreparedStatement prep2 = 
        conn0.prepareStatement("select clobholdin0_.ID as ID0_0_, clobholdin0_.SER_DATA as SER2_0_0_, clobholdin0_.CLOB_DATA as CLOB3_0_0_ from CLOB_ENTITY clobholdin0_ where clobholdin0_.ID=?");
        prep2.setLong(1, 1);
        ResultSet rs1 = 
        prep2.executeQuery();
        rs1.next();
        rs1.getCharacterStream("SER2_0_0_");
        Clob clob0 = 
        rs1.getClob("CLOB3_0_0_");
        rs1.wasNull();
        rs1.next();
        rs1.close();
        prep2.getMaxRows();
        prep2.getQueryTimeout();
        prep2.close();
        conn0.getAutoCommit();
        Reader r = clob0.getCharacterStream();
        for(int i=0; i<10000; i++) {
            int ch = r.read();
            if(ch != ('0' + (i%10))) {
                error("expected "+ (char)('0' + (i%10)) + " got: " + ch + " (" + (char)ch + ")");
            }
        }
        int ch = r.read();
        if(ch != -1) {
            error("expected -1 got: " + ch );
        }
        conn0.close();
    }

    private void testLobCopy(boolean compress) throws Exception {
        deleteDb("lob");
        Connection conn;
        conn = reconnect(null);     
        Statement stat = conn.createStatement();
        if(compress) {
            conn.createStatement().execute("SET COMPRESS_LOB LZF");
        } else {
            conn.createStatement().execute("SET COMPRESS_LOB NO");
        }        
        conn = reconnect(conn);
        stat = conn.createStatement();
        ResultSet rs;
        rs = stat.executeQuery("select value from information_schema.settings where NAME='COMPRESS_LOB'");
        rs.next();
        check(rs.getString(1), compress ? "LZF" : "NO");
        checkFalse(rs.next());
        stat.execute("create table test(text clob)");
        stat.execute("create table test2(text clob)");
        StringBuffer buff = new StringBuffer();
        for(int i=0; i<1000; i++) {
            buff.append(' ');
        }
        String spaces = buff.toString();
        stat.execute("insert into test values('"+spaces+"')");
        stat.execute("insert into test2 select * from test");
        rs = stat.executeQuery("select * from test2");
        rs.next();
        check(rs.getString(1), spaces);
        stat.execute("drop table test");
        rs = stat.executeQuery("select * from test2");
        rs.next();
        check(rs.getString(1), spaces);
        stat.execute("alter table test2 add column id int before text");
        rs = stat.executeQuery("select * from test2");
        rs.next();
        check(rs.getString("text"), spaces);
        conn.close();
    }
    
    private void testLobCompression(boolean compress) throws Exception {
        deleteDb("lob");
        Connection conn;
        conn = reconnect(null);       
        if(compress) {
            conn.createStatement().execute("SET COMPRESS_LOB LZF");
        } else {
            conn.createStatement().execute("SET COMPRESS_LOB NO");
        }
        conn.createStatement().execute("CREATE TABLE TEST(ID INT PRIMARY KEY, C CLOB)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?)");
        long time = System.currentTimeMillis();
        int len = getSize(10, 40);
        if(config.networked && config.big) {
            len = 5;
        }
        StringBuffer buff = new StringBuffer();
        for(int i=0; i<100; i++) {
            buff.append(StringUtils.xmlNode("content", null, "This is a test " + i));
        }
        String xml = buff.toString();
        for(int i=0; i<len; i++) {
            prep.setInt(1, i);
            prep.setString(2, xml + i);
            prep.execute();
        }
        for(int i=0; i<len; i++) {
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM TEST");
            while(rs.next()) {
                if(i==0) {
                    check(xml + rs.getInt(1), rs.getString(2));
                } else {
                    Reader r = rs.getCharacterStream(2);
                    String result = IOUtils.readStringAndClose(r, -1);
                    check(xml + rs.getInt(1), result);
                }
            }
        }
        time = System.currentTimeMillis() - time;
        // System.out.println("time: " + time +" compress: " + compress);
        conn.close();
    }
    
    private void testManyLobs() throws Exception {
        deleteDb("lob");
        Connection conn;
        conn = reconnect(null);        
        conn.createStatement().execute("CREATE TABLE TEST(ID INT PRIMARY KEY, B BLOB, C CLOB)");
        int len = getSize(10, 2000);
        if(config.networked) {
            len = 100;
        }
        
        int start = 1, increment = 19;
        
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(ID, B, C) VALUES(?, ?, ?)");
        for(int i=start; i<len; i+=increment) {
            int l = i;
            prep.setInt(1, i);
            prep.setBinaryStream(2, getRandomStream(l, i), -1);
            prep.setCharacterStream(3, getRandomReader(l, i), -1);
            prep.execute();
        }
        
        conn = reconnect(conn);
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM TEST ORDER BY ID");
        while(rs.next()) {
            int i = rs.getInt("ID");
            Blob b = rs.getBlob("B");
            Clob c = rs.getClob("C");
            int l = i;
            check(b.length(), l);
            check(c.length(), l);
            checkStream(b.getBinaryStream(), getRandomStream(l, i), -1);
            checkReader(c.getCharacterStream(), getRandomReader(l, i), -1);
        }
        
        prep = conn.prepareStatement("UPDATE TEST SET B=?, C=? WHERE ID=?");
        for(int i=start; i<len; i+=increment) {
            int l = i;
            prep.setBinaryStream(1, getRandomStream(l, -i), -1);
            prep.setCharacterStream(2, getRandomReader(l, -i), -1);
            prep.setInt(3, i);
            prep.execute();
        }

        conn = reconnect(conn);
        rs = conn.createStatement().executeQuery("SELECT * FROM TEST ORDER BY ID");
        while(rs.next()) {
            int i = rs.getInt("ID");
            Blob b = rs.getBlob("B");
            Clob c = rs.getClob("C");
            int l = i;
            check(b.length(), l);
            check(c.length(), l);
            checkStream(b.getBinaryStream(), getRandomStream(l, -i), -1);
            checkReader(c.getCharacterStream(), getRandomReader(l, -i), -1);
        }
        
        conn.close();
    }
    
    private void testClob() throws Exception {
        deleteDb("lob");
        Connection conn;
        conn = reconnect(null);        
        conn.createStatement().execute("CREATE TABLE TEST(ID IDENTITY, C CLOB)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(C) VALUES(?)");
        prep.setCharacterStream(1, new CharArrayReader("Bohlen".toCharArray()), "Bohlen".length());
        prep.execute();
        prep.setCharacterStream(1, new CharArrayReader("B\u00f6hlen".toCharArray()), "B\u00f6hlen".length());
        prep.execute();
        prep.setCharacterStream(1, getRandomReader(501, 1), -1);
        prep.execute();
        prep.setCharacterStream(1, getRandomReader(1501, 2), 401);
        prep.execute();
        conn = reconnect(conn);        
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM TEST ORDER BY ID");
        rs.next();
        check("Bohlen", rs.getString("C"));
        checkReader(new CharArrayReader("Bohlen".toCharArray()), rs.getCharacterStream("C"), -1);
        rs.next();
        checkReader(new CharArrayReader("B\u00f6hlen".toCharArray()), rs.getCharacterStream("C"), -1);
        rs.next();
        checkReader(getRandomReader(501, 1), rs.getCharacterStream("C"), -1);
        Clob clob = rs.getClob("C");
        checkReader(getRandomReader(501, 1), clob.getCharacterStream(), -1);
        check(clob.length(), 501);
        rs.next();
        checkReader(getRandomReader(401, 2), rs.getCharacterStream("C"), -1);
        checkReader(getRandomReader(1500, 2), rs.getCharacterStream("C"), 401);
        clob = rs.getClob("C");
        checkReader(getRandomReader(1501, 2), clob.getCharacterStream(), 401);
        checkReader(getRandomReader(401, 2), clob.getCharacterStream(), 401);
        check(clob.length(), 401);
        checkFalse(rs.next());
        conn.close();
    }
    
    private Connection reconnect(Connection conn) throws Exception {
        long time = System.currentTimeMillis();
        if(conn != null) {
            conn.close();
        }
        conn = getConnection("lob");
        trace("re-connect="+(System.currentTimeMillis()-time));        
        return conn;
    }
    
    
    void testUpdateLob() throws Exception {
        deleteDb("lob");
        Connection conn;
        conn = reconnect(null);

        PreparedStatement prep = conn.prepareStatement(
                "CREATE TABLE IF NOT EXISTS p( id int primary key, rawbyte BLOB ); ");
        prep.execute();
        prep.close();

        prep = conn.prepareStatement("INSERT INTO p(id) VALUES(?);");
        for (int i = 0; i < 10; i++) {
            prep.setInt(1, i);
            prep.execute();
        }
        prep.close();

        prep = conn.prepareStatement("UPDATE p set rawbyte=? WHERE id=?");
        for (int i = 0; i < 8; i++) {
            prep.setBinaryStream(1, getRandomStream(10000, i), 0);
            prep.setInt(2, i);
            prep.execute();
        }
        prep.close();
        conn.commit();

        conn = reconnect(conn);

        conn.setAutoCommit(true);
        prep = conn.prepareStatement("UPDATE p set rawbyte=? WHERE id=?");
        for (int i = 8; i < 10; i++) {
            prep.setBinaryStream(1, getRandomStream(10000, i), 0);
            prep.setInt(2, i);
            prep.execute();
        }
        prep.close();

        prep = conn.prepareStatement("SELECT * from p");
        ResultSet rs = prep.executeQuery();
        while (rs.next()) {
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                rs.getMetaData().getColumnName(i);
                rs.getString(i);
            }
        }
        conn.close();
    }
    
    void testLobReconnect() throws Exception {
        deleteDb("lob");
        Connection conn = reconnect(null);
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, TEXT CLOB)");
        PreparedStatement prep;
        prep = conn.prepareStatement("INSERT INTO TEST VALUES(1, ?)");
        String s = new String(getRandomChars(10000, 1));
        byte[] data = s.getBytes("UTF-8");
        prep.setBinaryStream(1, new ByteArrayInputStream(data), 0);
        prep.execute();
       
        conn = reconnect(conn);
        stat = conn.createStatement();        
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST WHERE ID=1");
        rs.next();
        checkStream(new ByteArrayInputStream(data), rs.getBinaryStream("TEXT"), -1);
        
        
        prep = conn.prepareStatement("UPDATE TEST SET TEXT = ?");
        s = new String(getRandomChars(10201, 1));
        prep.setBinaryStream(1, new ByteArrayInputStream(data), 0);
        prep.execute();

        conn = reconnect(conn);
        stat = conn.createStatement();        
        rs = stat.executeQuery("SELECT * FROM TEST WHERE ID=1");
        rs.next();
        checkStream(new ByteArrayInputStream(data), rs.getBinaryStream("TEXT"), -1);

        stat.execute("DROP TABLE IF EXISTS TEST");        
        conn.close();
    }
    
    void testLob(boolean clob) throws Exception {
        deleteDb("lob");
        Connection conn = reconnect(null);
        conn = reconnect(conn);
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        PreparedStatement prep;
        ResultSet rs;
        long time;
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, VALUE "+(clob?"CLOB":"BLOB")+")");
        
        int len = getSize(1, 1000);
        if(config.networked && config.big) {
            len = 100;
        }
        
        time = System.currentTimeMillis();
        prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?)");
        for(int i=0; i<len; i+=(i+i+1)) {
            prep.setInt(1, i);
            int size = i*i;            
            if(clob) {
                prep.setCharacterStream(2, getRandomReader(size, i), 0);
            } else {
                prep.setBinaryStream(2, getRandomStream(size, i), 0);
            }
            prep.execute();
        }
        trace("insert="+(System.currentTimeMillis()-time));        
        traceMemory();
        conn = reconnect(conn);
        
        time = System.currentTimeMillis();
        prep  = conn.prepareStatement("SELECT ID, VALUE FROM TEST");
        rs = prep.executeQuery();
        while(rs.next()) {
            int id = rs.getInt("ID");
            int size = id*id;
            if(clob) {
                Reader rt = rs.getCharacterStream(2);
                checkReader(rt, getRandomReader(size, id), -1);
                checkReader((Reader)rs.getObject(2), getRandomReader(size, id), -1);
            } else {
                InputStream in = rs.getBinaryStream(2);
                checkStream(in, getRandomStream(size, id), -1);
                checkStream((InputStream)rs.getObject(2), getRandomStream(size, id), -1);
            }
        }
        trace("select="+(System.currentTimeMillis()-time));
        traceMemory();
        
        conn = reconnect(conn);
        
        time = System.currentTimeMillis();
        prep  = conn.prepareStatement("DELETE FROM TEST WHERE ID=?");
        for(int i=0; i<len; i++) {
            prep.setInt(1, i);
            prep.executeUpdate();
        }
        trace("delete="+(System.currentTimeMillis()-time));
        traceMemory();
        conn = reconnect(conn);
        
        conn.setAutoCommit(false);
        prep = conn.prepareStatement(
            "INSERT INTO TEST VALUES(1, ?)");
        if(clob) {
            prep.setCharacterStream(1, getRandomReader(0, 0), 0);
        } else {
            prep.setBinaryStream(1, getRandomStream(0, 0), 0);
        }
        prep.execute();
        conn.rollback();
        prep.execute();
        conn.commit();
        
        conn.createStatement().execute("DELETE FROM TEST WHERE ID=1");
        conn.rollback();
        conn.createStatement().execute("DELETE FROM TEST WHERE ID=1");
        conn.commit();
        
        conn.createStatement().execute("DROP TABLE TEST");
        conn.close();
    }

    void testJavaObject() throws Exception {
        deleteDb("lob");
        Connection conn = getConnection("lob");
        conn.createStatement().execute("CREATE TABLE TEST(ID INT PRIMARY KEY, DATA OTHER)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(1, ?)");
        prep.setObject(1, new TestLobObject("abc"));
        prep.execute();
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM TEST");
        rs.next();
        Object oa = rs.getObject(2);
        TestLobObject a = (TestLobObject)oa;
        Object ob = rs.getObject("DATA");
        TestLobObject b = (TestLobObject)ob;
        check(a.data, "abc");
        check(b.data, "abc");
        checkFalse(rs.next());
        conn.close();
    }

    private void checkStream(InputStream a, InputStream b, int len) throws Exception {
        // this doesn't actually read anything - just tests reading 0 bytes
        a.read(new byte[0]);
        b.read(new byte[0]);
        a.read(new byte[10], 3, 0);
        b.read(new byte[10], 0, 0);
        
        for(int i=0; len<0 || i<len; i++) {
            int ca = a.read();
            a.read(new byte[0]);
            int cb = b.read();
            check(ca, cb);
            if(ca==-1) {
                break;
            }
        }
        a.read(new byte[10], 3, 0);
        b.read(new byte[10], 0, 0);
        a.read(new byte[0]);
        b.read(new byte[0]);
        a.close();
        b.close();
    }
    
    private void checkReader(Reader a, Reader b, int len) throws Exception {
        for(int i=0; len<0 || i<len; i++) {
            int ca = a.read();
            int cb = b.read();
            check(ca, cb);
            if(ca==-1) {
                break;
            }
        }
        a.close();
        b.close();
    }
    
    private Reader getRandomReader(int len, int seed) {
        return new CharArrayReader(getRandomChars(len, seed));
    }
    
    private char[] getRandomChars(int len, int seed) {
        Random random = new Random(seed);        
        char[] buff = new char[len];
        for(int i=0; i<len; i++) {
            char ch;
            do {
                ch = (char)random.nextInt(Character.MAX_VALUE);
                // UTF8: String.getBytes("UTF-8") only returns 1 byte for 0xd800-0xdfff
            } while(ch >= 0xd800 && ch <= 0xdfff);
            buff[i] = ch;
        }
        return buff;
    }
    
    private InputStream getRandomStream(int len, int seed) {
        Random random = new Random(seed);
        byte[] buff = new byte[len];
        random.nextBytes(buff);
        return new ByteArrayInputStream(buff);
    }
    
}
