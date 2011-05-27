/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Random;

import org.h2.engine.Constants;
import org.h2.store.FileLister;
import org.h2.test.TestBase;
import org.h2.test.trace.Player;
import org.h2.tools.Backup;
import org.h2.tools.ChangePassword;
import org.h2.tools.ConvertTraceFile;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.Recover;
import org.h2.tools.Restore;
import org.h2.tools.RunScript;
import org.h2.tools.Script;
import org.h2.tools.Server;
import org.h2.util.FileUtils;
import org.h2.util.Resources;

/**
 * Tests the database tools.
 */
public class TestTools extends TestBase {

    private Server server;

    public void test() throws Exception {
        if (config.networked) {
            return;
        }
        deleteDb("utils");
        testScriptRunscriptLob();
        deleteDb("utils");
        testServerMain();
        testRemove();
        testConvertTraceFile();
        testManagementDb();
        testResourceGenerator();
        testChangePassword();
        testServer();
        testScriptRunscript();
        testBackupRestore();
        testRecover();
    }

    private void testServerMain() throws Exception {
        String result;
        Connection conn;
        org.h2.Driver.load();

        result = runServer(new String[]{"-?"}, 1);
        check(result.indexOf("Starts H2 Servers") >= 0);
        check(result.indexOf("Unknown option") < 0);

        result = runServer(new String[]{"-xy"}, 1);
        check(result.indexOf("Starts H2 Servers") >= 0);
        check(result.indexOf("Unsupported option") >= 0);
        result = runServer(new String[]{"-tcp", "-tcpPort", "9001", "-tcpPassword", "abc"}, 0);
        check(result.indexOf("tcp://") >= 0);
        check(result.indexOf(":9001") >= 0);
        check(result.indexOf("only local") >= 0);
        check(result.indexOf("Starts H2 Servers") < 0);
        conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9001/mem:", "sa", "sa");
        conn.close();
        result = runServer(new String[]{"-tcpShutdown", "tcp://localhost:9001", "-tcpPassword", "abc", "-tcpShutdownForce"}, 0);
        check(result.indexOf("Shutting down") >= 0);

        result = runServer(new String[]{"-tcp", "-tcpAllowOthers", "-tcpPort", "9001", "-tcpPassword", "abcdef", "-tcpSSL"}, 0);
        check(result.indexOf("ssl://") >= 0);
        check(result.indexOf(":9001") >= 0);
        check(result.indexOf("others can") >= 0);
        check(result.indexOf("Starts H2 Servers") < 0);
        conn = DriverManager.getConnection("jdbc:h2:ssl://localhost:9001/mem:", "sa", "sa");
        conn.close();

        result = runServer(new String[]{"-tcpShutdown", "ssl://localhost:9001", "-tcpPassword", "abcdef"}, 0);
        check(result.indexOf("Shutting down") >= 0);
        try {
            conn = DriverManager.getConnection("jdbc:h2:ssl://localhost:9001/mem:", "sa", "sa");
            error();
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }

        result = runServer(new String[]{
                "-web", "-webPort", "9002", "-webAllowOthers", "-webSSL", 
                "-pg", "-pgAllowOthers", "-pgPort", "9003",
                "-ftp", "-ftpPort", "9004", "-ftpDir", ".", "-ftpRead", "guest", "-ftpWrite", "sa", "-ftpWritePassword", "sa", "-ftpTask", 
                "-tcp", "-tcpAllowOthers", "-tcpPort", "9005", "-tcpPassword", "abc"}, 0);
        Server stop = server;
        check(result.indexOf("https://") >= 0);
        check(result.indexOf(":9002") >= 0);
        check(result.indexOf("pg://") >= 0);
        check(result.indexOf(":9003") >= 0);
        check(result.indexOf("others can") >= 0);
        check(result.indexOf("only local") < 0);
        check(result.indexOf("ftp://") >= 0);
        check(result.indexOf(":9004") >= 0);
        check(result.indexOf("tcp://") >= 0);
        check(result.indexOf(":9005") >= 0);

        result = runServer(new String[]{"-tcpShutdown", "tcp://localhost:9005", "-tcpPassword", "abc", "-tcpShutdownForce"}, 0);
        check(result.indexOf("Shutting down") >= 0);
        stop.shutdown();
        try {
            conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9005/mem:", "sa", "sa");
            error();
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }
    }

    private String runServer(String[] args, int exitCode) throws Exception {
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(buff);
        server = new Server();
        int gotCode = server.run(args, ps);
        check(exitCode, gotCode);
        ps.flush();
        String s = new String(buff.toByteArray());
        return s;
    }

    private void testConvertTraceFile() throws Exception {
        deleteDb("toolsConvertTraceFile");
        Class.forName("org.h2.Driver");
        String url = "jdbc:h2:" + baseDir + "/toolsConvertTraceFile";
        Connection conn = DriverManager.getConnection(url + ";TRACE_LEVEL_FILE=3", "sa", "sa");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar, amount decimal)");
        PreparedStatement prep = conn.prepareStatement("insert into test values(?, ?, ?)");
        prep.setInt(1, 1);
        prep.setString(2, "Hello");
        prep.setBigDecimal(3, new BigDecimal("10.20"));
        prep.executeUpdate();
        stat.execute("create table test2(id int primary key, a real, b double, c bigint, " +
                "d smallint, e boolean, f binary, g date, h time, i timestamp)");
        prep = conn.prepareStatement("insert into test2 values(1, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        prep.setFloat(1, Float.MIN_VALUE);
        prep.setDouble(2, Double.MIN_VALUE);
        prep.setLong(3, Long.MIN_VALUE);
        prep.setShort(4, Short.MIN_VALUE);
        prep.setBoolean(5, false);
        prep.setBytes(6, new byte[] { (byte) 10, (byte) 20 });
        prep.setDate(7, java.sql.Date.valueOf("2007-12-31"));
        prep.setTime(8, java.sql.Time.valueOf("23:59:59"));
        prep.setTimestamp(9, java.sql.Timestamp.valueOf("2007-12-31 23:59:59"));
        prep.execute();
        conn.close();

        ConvertTraceFile.main(new String[]{"-traceFile", baseDir + "/toolsConvertTraceFile.trace.db", "-javaClass", baseDir + "/Test", "-script", baseDir + "/test.sql"});
        new File(baseDir + "/Test.java").delete();

        File trace = new File(baseDir + "/toolsConvertTraceFile.trace.db");
        check(trace.exists());
        File newTrace = new File(baseDir + "/test.trace.db");
        newTrace.delete();
        check(trace.renameTo(newTrace));
        deleteDb("toolsConvertTraceFile");
        Player.main(new String[]{baseDir + "/test.trace.db"});
        testTraceFile(url);

        deleteDb("toolsConvertTraceFile");
        RunScript.main(new String[]{"-url", url, "-user", "sa", "-script", baseDir + "/test.sql"});
        testTraceFile(url);
    }

    private void testTraceFile(String url) throws Exception {
        Connection conn;
        Recover.main(new String[]{"-removePassword", "-dir", baseDir, "-db", "toolsConvertTraceFile"});
        conn = DriverManager.getConnection(url, "sa", "");
        Statement stat = conn.createStatement();
        ResultSet rs;
        rs = stat.executeQuery("select * from test");
        rs.next();
        check(1, rs.getInt(1));
        check("Hello", rs.getString(2));
        check("10.20", rs.getBigDecimal(3).toString());
        checkFalse(rs.next());
        rs = stat.executeQuery("select * from test2");
        rs.next();
        check(Float.MIN_VALUE, rs.getFloat("a"));
        check(Double.MIN_VALUE, rs.getDouble("b"));
        check(Long.MIN_VALUE, rs.getLong("c"));
        check(Short.MIN_VALUE, rs.getShort("d"));
        check(!rs.getBoolean("e"));
        check(new byte[] { (byte) 10, (byte) 20 }, rs.getBytes("f"));
        check("2007-12-31", rs.getString("g"));
        check("23:59:59", rs.getString("h"));
        check("2007-12-31 23:59:59.0", rs.getString("i"));
        checkFalse(rs.next());
        conn.close();
    }

    private void testRemove() throws Exception {
        deleteDb("toolsRemove");
        Class.forName("org.h2.Driver");
        String url = "jdbc:h2:" + baseDir + "/toolsRemove";
        Connection conn = DriverManager.getConnection(url, "sa", "sa");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("insert into test values(1, 'Hello')");
        conn.close();
        ArrayList list = FileLister.getDatabaseFiles(baseDir, "toolsRemove", true);
        for (int i = 0; i < list.size(); i++) {
            String fileName = (String) list.get(i);
            if (fileName.endsWith(Constants.SUFFIX_LOG_FILE)) {
                FileUtils.delete(fileName);
            }
        }
        Recover.main(new String[]{"-dir", baseDir, "-db", "toolsRemove", "-removePassword"});
        conn = DriverManager.getConnection(url, "sa", "");
        stat = conn.createStatement();
        ResultSet rs;
        rs = stat.executeQuery("select * from test");
        rs.next();
        check(1, rs.getInt(1));
        check("Hello", rs.getString(2));
        conn.close();
    }

    private void testRecover() throws Exception {
        deleteDb("toolsRecover");
        Class.forName("org.h2.Driver");
        String url = "jdbc:h2:" + baseDir + "/toolsRecover";
        Connection conn = DriverManager.getConnection(url, "sa", "sa");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar, b blob, c clob)");
        stat.execute("create table \"test 2\"(id int primary key, name varchar)");
        stat.execute("comment on table test is ';-)'");
        stat.execute("insert into test values(1, 'Hello', SECURE_RAND(2000), space(2000))");
        ResultSet rs;
        rs = stat.executeQuery("select * from test");
        rs.next();
        byte[] b1 = rs.getBytes(3);
        String s1 = rs.getString(4);

        conn.close();
        Recover.main(new String[]{"-dir", baseDir, "-db", "toolsRecover"});
        deleteDb("toolsRecover");
        conn = DriverManager.getConnection(url, "another", "another");
        stat = conn.createStatement();
        stat.execute("runscript from '" + baseDir + "/toolsRecover.data.sql'");
        rs = stat.executeQuery("select * from \"test 2\"");
        checkFalse(rs.next());
        rs = stat.executeQuery("select * from test");
        rs.next();
        check(1, rs.getInt(1));
        check("Hello", rs.getString(2));
        byte[] b2 = rs.getBytes(3);
        String s2 = rs.getString(4);
        check(2000, b2.length);
        check(2000, s2.length());
        check(b1, b2);
        check(s1, s2);
        checkFalse(rs.next());
        conn.close();
    }

    private void testManagementDb() throws Exception {
        int count = getSize(2, 10);
        for (int i = 0; i < count; i++) {
            Server server = Server.createTcpServer(new String[] {"-tcpPort", "9192"}).start();
            server.stop();
            server = Server.createTcpServer(new String[] { "-tcpPassword", "abc", "-tcpPort", "9192" }).start();
            server.stop();
        }
    }

    private void testScriptRunscriptLob() throws Exception {
        Class.forName("org.h2.Driver");
        String url = "jdbc:h2:" + baseDir + "/utils";
        String user = "sa", password = "abc";
        String fileName = baseDir + "/b2.sql";
        Connection conn = DriverManager.getConnection(url, user, password);
        conn.createStatement().execute("CREATE TABLE TEST(ID INT PRIMARY KEY, BDATA BLOB, CDATA CLOB)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?, ?)");

        prep.setInt(1, 1);
        prep.setNull(2, Types.BLOB);
        prep.setNull(3, Types.CLOB);
        prep.execute();

        prep.setInt(1, 2);
        prep.setString(2, "face");
        prep.setString(3, "face");
        prep.execute();

        Random random = new Random(1);
        prep.setInt(1, 3);
        byte[] large = new byte[getSize(10 * 1024, 100 * 1024)];
        random.nextBytes(large);
        prep.setBytes(2, large);
        String largeText = new String(large, "ISO-8859-1");
        prep.setString(3, largeText);
        prep.execute();

        for (int i = 0; i < 2; i++) {
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM TEST ORDER BY ID");
            rs.next();
            check(1, rs.getInt(1));
            check(rs.getString(2) == null);
            check(rs.getString(3) == null);
            rs.next();
            check(2, rs.getInt(1));
            check("face", rs.getString(2));
            check("face", rs.getString(3));
            rs.next();
            check(3, rs.getInt(1));
            check(large, rs.getBytes(2));
            check(largeText, rs.getString(3));
            checkFalse(rs.next());

            conn.close();
            Script.main(new String[] { "-url", url, "-user", user, "-password", password, "-script", fileName});
            DeleteDbFiles.main(new String[] { "-dir", baseDir, "-db", "utils", "-quiet" });
            RunScript.main(new String[] { "-url", url, "-user", user, "-password", password, "-script", fileName});
            conn = DriverManager.getConnection("jdbc:h2:" + baseDir + "/utils", "sa", "abc");
        }
        conn.close();

    }

    private void testScriptRunscript() throws Exception {
        Class.forName("org.h2.Driver");
        String url = "jdbc:h2:" + baseDir + "/utils";
        String user = "sa", password = "abc";
        String fileName = baseDir + "/b2.sql";
        Connection conn = DriverManager.getConnection(url, user, password);
        conn.createStatement().execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        conn.createStatement().execute("INSERT INTO TEST VALUES(1, 'Hello')");
        conn.close();
        Script.main(new String[] { "-url", url, "-user", user, "-password", password, "-script", fileName, "-options",
                "nodata", "compression", "lzf", "cipher", "xtea", "password", "'123'" });
        DeleteDbFiles.main(new String[] { "-dir", baseDir, "-db", "utils", "-quiet" });
        RunScript.main(new String[] { "-url", url, "-user", user, "-password", password, "-script", fileName,
                "-options", "compression", "lzf", "cipher", "xtea", "password", "'123'" });
        conn = DriverManager.getConnection("jdbc:h2:" + baseDir + "/utils", "sa", "abc");
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM TEST");
        checkFalse(rs.next());
        conn.close();
    }

    private void testBackupRestore() throws Exception {
        Class.forName("org.h2.Driver");
        String url = "jdbc:h2:" + baseDir + "/utils";
        String user = "sa", password = "abc";
        String fileName = baseDir + "/b2.zip";
        DeleteDbFiles.main(new String[] { "-dir", baseDir, "-db", "utils", "-quiet" });
        Connection conn = DriverManager.getConnection(url, user, password);
        conn.createStatement().execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        conn.createStatement().execute("INSERT INTO TEST VALUES(1, 'Hello')");
        conn.close();
        Backup.main(new String[] { "-file", fileName, "-dir", baseDir, "-db", "utils", "-quiet" });
        DeleteDbFiles.main(new String[] { "-dir", baseDir, "-db", "utils", "-quiet" });
        Restore.main(new String[] { "-file", fileName, "-dir", baseDir, "-db", "utils", "-quiet" });
        conn = DriverManager.getConnection("jdbc:h2:" + baseDir + "/utils", "sa", "abc");
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM TEST");
        check(rs.next());
        checkFalse(rs.next());
        conn.close();
        DeleteDbFiles.main(new String[] { "-dir", baseDir, "-db", "utils", "-quiet" });
    }

    private void testResourceGenerator() throws Exception {
        Resources.main(new String[] { "." });
    }

    private void testChangePassword() throws Exception {
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection("jdbc:h2:" + baseDir + "/utils;CIPHER=XTEA;STORAGE=TEXT", "sa",
                "abc 123");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        conn.close();
        String[] args = new String[] { "-dir", baseDir, "-db", "utils", "-cipher", "XTEA", "-decrypt", "abc", "-quiet" };
        ChangePassword.main(args);
        args = new String[] { "-dir", baseDir, "-db", "utils", "-cipher", "AES", "-encrypt", "def", "-quiet" };
        ChangePassword.main(args);
        conn = DriverManager.getConnection("jdbc:h2:" + baseDir + "/utils;CIPHER=AES", "sa", "def 123");
        stat = conn.createStatement();
        stat.execute("SELECT * FROM TEST");
        conn.close();
        args = new String[] { "-dir", baseDir, "-db", "utils", "-quiet" };
        DeleteDbFiles.main(args);
    }

    private void testServer() throws Exception {
        Connection conn;
        deleteDb("test");
        Server server = Server.createTcpServer(new String[] { "-baseDir", baseDir, "-tcpPort", "9192" }).start();
        conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9192/test", "sa", "");
        conn.close();
        server.stop();
        server = Server.createTcpServer(
                new String[] { "-ifExists", "-tcpPassword", "abc", "-baseDir", baseDir, "-tcpPort", "9192" }).start();
        try {
            conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9192/test2", "sa", "");
            error("should not be able to create new db");
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }
        conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9192/test", "sa", "");
        conn.close();
        try {
            Server.shutdownTcpServer("tcp://localhost:9192", "", true);
            error("shouldn't work and should throw an exception");
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }
        conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9192/test", "sa", "");
        // conn.close();
        Server.shutdownTcpServer("tcp://localhost:9192", "abc", true);
        // check that the database is closed
        deleteDb("test");
        try {
            conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9192/test", "sa", "");
            error("server must have been closed");
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }
    }

}
