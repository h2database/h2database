/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import org.h2.dev.fs.FileSystemCrypt;
import org.h2.store.fs.FileObject;
import org.h2.store.fs.FileSystem;
import org.h2.store.fs.FileSystemMemory;
import org.h2.test.TestBase;
import org.h2.tools.Backup;
import org.h2.tools.DeleteDbFiles;
import org.h2.util.IOUtils;

/**
 * Tests various file system.
 */
public class TestFileSystem extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        // test.config.traceTest = true;
        test.test();
    }

    public void test() throws Exception {
        FileSystemCrypt.register();
        // DebugFileSystem.register().setTrace(true);
        // testFileSystem("crypt:aes:x:" + getBaseDir() + "/fs");

        testSplitDatabaseInZip();
        testDatabaseInMemFileSys();
        testDatabaseInJar();
        // set default part size to 1 << 10
        String f = "split:10:" + getBaseDir() + "/fs";
        FileSystem.getInstance(f).getCanonicalPath(f);
        testFileSystem(getBaseDir() + "/fs");
        testFileSystem(FileSystemMemory.PREFIX);
        FileSystemDatabase fs = FileSystemDatabase.register("jdbc:h2:mem:fs");
        // testFileSystem("jdbc:h2:mem:fs;TRACE_LEVEL_FILE=3");
        testFileSystem("jdbc:h2:mem:fs");
        testFileSystem(FileSystemMemory.PREFIX_LZF);
        testUserHome();
        fs.unregister();
        try {
            FileSystemCrypt.register();
            testFileSystem("crypt:aes:x:" + getBaseDir() + "/fs");
            testFileSystem("nio:" + getBaseDir() + "/fs");
            testFileSystem("nioMapped:" + getBaseDir() + "/fs");
            if (!config.splitFileSystem) {
                testFileSystem("split:" + getBaseDir() + "/fs");
                testFileSystem("split:nioMapped:" + getBaseDir() + "/fs");
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } catch (Error e) {
            e.printStackTrace();
            throw e;
        } finally {
            IOUtils.delete(getBaseDir() + "/fs");
        }
    }

    private void testSplitDatabaseInZip() throws SQLException {
        String dir = getBaseDir() + "/fs";
        IOUtils.deleteRecursive(dir, false);
        Connection conn;
        Statement stat;
        conn = DriverManager.getConnection("jdbc:h2:split:18:"+dir+"/test");
        stat = conn.createStatement();
        stat.execute(
                "create table test(id int primary key, name varchar) " +
                "as select x, space(10000) from system_range(1, 100)");
        stat.execute("shutdown defrag");
        conn.close();
        Backup.execute(dir + "/test.zip", dir, "", true);
        DeleteDbFiles.execute("split:" + dir, "test", true);
        conn = DriverManager.getConnection(
                "jdbc:h2:split:zip:"+dir+"/test.zip!/test");
        conn.createStatement().execute("select * from test where id=1");
        conn.close();
        IOUtils.deleteRecursive(dir, false);
    }

    private void testDatabaseInMemFileSys() throws SQLException {
        org.h2.Driver.load();
        deleteDb("fsMem");
        String url = "jdbc:h2:" + getBaseDir() + "/fsMem";
        Connection conn = DriverManager.getConnection(url, "sa", "sa");
        conn.createStatement().execute("CREATE TABLE TEST AS SELECT * FROM DUAL");
        conn.createStatement().execute("BACKUP TO '" + getBaseDir() + "/fsMem.zip'");
        conn.close();
        org.h2.tools.Restore.main("-file", getBaseDir() + "/fsMem.zip", "-dir", "memFS:");
        conn = DriverManager.getConnection("jdbc:h2:memFS:fsMem", "sa", "sa");
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM TEST");
        rs.close();
        conn.close();
        deleteDb("fsMem");
        IOUtils.delete(getBaseDir() + "/fsMem.zip");
    }

    private void testDatabaseInJar() throws SQLException {
        if (getBaseDir().indexOf(':') > 0) {
            return;
        }
        if (config.networked) {
            return;
        }
        org.h2.Driver.load();
        String url = "jdbc:h2:" + getBaseDir() + "/fsJar";
        Connection conn = DriverManager.getConnection(url, "sa", "sa");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar, b blob, c clob)");
        stat.execute("insert into test values(1, 'Hello', SECURE_RAND(2000), space(2000))");
        ResultSet rs;
        rs = stat.executeQuery("select * from test");
        rs.next();
        byte[] b1 = rs.getBytes(3);
        String s1 = rs.getString(4);
        conn.close();
        conn = DriverManager.getConnection(url, "sa", "sa");
        stat = conn.createStatement();
        stat.execute("backup to '" + getBaseDir() + "/fsJar.zip'");
        conn.close();

        deleteDb("fsJar");
        FileSystem fs = FileSystem.getInstance("zip:" + getBaseDir() + "/fsJar.zip");
        for (String f : fs.listFiles("zip:" + getBaseDir() + "/fsJar.zip")) {
            assertTrue(fs.isAbsolute(f));
            assertTrue(!fs.isDirectory(f));
            assertTrue(fs.length(f) > 0);
            assertTrue(f.endsWith(fs.getFileName(f)));
        }
        String urlJar = "jdbc:h2:zip:" + getBaseDir() + "/fsJar.zip!/fsJar";
        conn = DriverManager.getConnection(urlJar, "sa", "sa");
        stat = conn.createStatement();
        rs = stat.executeQuery("select * from test");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        byte[] b2 = rs.getBytes(3);
        String s2 = rs.getString(4);
        assertEquals(2000, b2.length);
        assertEquals(2000, s2.length());
        assertEquals(b1, b2);
        assertEquals(s1, s2);
        assertFalse(rs.next());
        conn.close();
        IOUtils.delete(getBaseDir() + "/fsJar.zip");
    }

    private void testUserHome() {
        FileSystem fs = FileSystem.getInstance("~/test");
        String fileName = fs.getCanonicalPath("~/test");
        String userDir = System.getProperty("user.home");
        assertTrue(fileName.startsWith(userDir));
    }

    private void testFileSystem(String fsBase) throws Exception {
        testSimple(fsBase);
        testTempFile(fsBase);
        testRandomAccess(fsBase);
    }

    private void testSimple(String fsBase) throws Exception {
        FileSystem fs = FileSystem.getInstance(fsBase);
        long time = System.currentTimeMillis();
        for (String s : fs.listFiles(fsBase)) {
            fs.delete(s);
        }
        fs.createDirs(fsBase + "/test/x");
        fs.delete(fsBase + "/test");
        fs.delete(fsBase + "/test2");
        assertTrue(fs.createNewFile(fsBase + "/test"));
        assertTrue(fs.canWrite(fsBase + "/test"));
        FileObject fo = fs.openFileObject(fsBase + "/test", "rw");
        byte[] buffer = new byte[10000];
        Random random = new Random(1);
        random.nextBytes(buffer);
        fo.write(buffer, 0, 10000);
        assertEquals(10000, fo.length());
        fo.seek(20000);
        assertEquals(20000, fo.getFilePointer());
        try {
            fo.readFully(buffer, 0, 1);
            fail();
        } catch (EOFException e) {
            // expected
        }
        assertEquals(fsBase + "/test", fo.getName().replace('\\', '/'));
        assertEquals("test", fs.getFileName(fo.getName()));
        assertEquals(fsBase, fs.getParent(fo.getName()).replace('\\', '/'));
        fo.tryLock();
        fo.releaseLock();
        assertEquals(10000, fo.length());
        fo.close();
        assertEquals(10000, fs.length(fsBase + "/test"));
        fo = fs.openFileObject(fsBase + "/test", "r");
        byte[] test = new byte[10000];
        fo.readFully(test, 0, 10000);
        assertEquals(buffer, test);
        try {
            fo.write(test, 0, 10);
            fail();
        } catch (IOException e) {
            // expected
        }
        try {
            fo.setFileLength(10);
            fail();
        } catch (IOException e) {
            // expected
        }
        fo.close();
        long lastMod = fs.getLastModified(fsBase + "/test");
        if (lastMod < time - 1999) {
            // at most 2 seconds difference
            assertEquals(time, lastMod);
        }
        assertEquals(10000, fs.length(fsBase + "/test"));
        String[] list = fs.listFiles(fsBase);
        assertEquals(1, list.length);
        assertTrue(list[0].endsWith("test"));
        fs.copy(fsBase + "/test", fsBase + "/test3");
        fs.rename(fsBase + "/test3", fsBase + "/test2");
        assertTrue(!fs.exists(fsBase + "/test3"));
        assertTrue(fs.exists(fsBase + "/test2"));
        assertEquals(10000, fs.length(fsBase + "/test2"));
        byte[] buffer2 = new byte[10000];
        InputStream in = fs.openFileInputStream(fsBase + "/test2");
        int pos = 0;
        while (true) {
            int l = in.read(buffer2, pos, Math.min(10000 - pos, 1000));
            if (l <= 0) {
                break;
            }
            pos += l;
        }
        in.close();
        assertEquals(10000, pos);
        assertEquals(buffer, buffer2);

        assertTrue(fs.tryDelete(fsBase + "/test2"));
        fs.delete(fsBase + "/test");
        if (fsBase.indexOf(FileSystemMemory.PREFIX) < 0 && fsBase.indexOf(FileSystemMemory.PREFIX_LZF) < 0) {
            fs.createDirs(fsBase + "/testDir/test");
            assertTrue(fs.isDirectory(fsBase + "/testDir"));
            if (!fsBase.startsWith("jdbc:")) {
                fs.deleteRecursive(fsBase + "/testDir", false);
                assertTrue(!fs.exists(fsBase + "/testDir"));
            }
        }
    }

    private void testRandomAccess(String fsBase) throws Exception {
        testRandomAccess(fsBase, 1);
    }

    private void testRandomAccess(String fsBase, int seed) throws Exception {
        StringBuilder buff = new StringBuilder();
        FileSystem fs = FileSystem.getInstance(fsBase);
        String s = fs.createTempFile(fsBase + "/tmp", ".tmp", false, false);
        File file = new File(TestBase.BASE_TEST_DIR + "/tmp");
        file.getParentFile().mkdirs();
        file.delete();
        RandomAccessFile ra = new RandomAccessFile(file, "rw");
        fs.delete(s);
        FileObject f = fs.openFileObject(s, "rw");
        try {
            f.readFully(new byte[1], 0, 1);
            fail();
        } catch (EOFException e) {
            // expected
        }
        f.sync();
        Random random = new Random(seed);
        int size = getSize(100, 500);
        try {
            for (int i = 0; i < size; i++) {
                trace("op " + i);
                int pos = random.nextInt(10000);
                switch(random.nextInt(7)) {
                case 0: {
                    pos = (int) Math.min(pos, ra.length());
                    trace("seek " + pos);
                    buff.append("seek " + pos + "\n");
                    f.seek(pos);
                    ra.seek(pos);
                    break;
                }
                case 1: {
                    byte[] buffer = new byte[random.nextInt(1000)];
                    random.nextBytes(buffer);
                    trace("write " + buffer.length);
                    buff.append("write " + buffer.length + "\n");
                    f.write(buffer, 0, buffer.length);
                    ra.write(buffer, 0, buffer.length);
                    break;
                }
                case 2: {
                    trace("setLength " + pos);
                    f.setFileLength(pos);
                    ra.setLength(pos);
                    if (ra.getFilePointer() > pos) {
                        f.seek(0);
                        ra.seek(0);
                    }
                    buff.append("setLength " + pos + "\n");
                    break;
                }
                case 3: {
                    int len = random.nextInt(1000);
                    len = (int) Math.min(len, ra.length() - ra.getFilePointer());
                    byte[] b1 = new byte[len];
                    byte[] b2 = new byte[len];
                    trace("readFully " + len);
                    ra.readFully(b1, 0, len);
                    f.readFully(b2, 0, len);
                    buff.append("readFully " + len + "\n");
                    assertEquals(b1, b2);
                    break;
                }
                case 4: {
                    trace("getFilePointer");
                    buff.append("getFilePointer\n");
                    assertEquals(ra.getFilePointer(), f.getFilePointer());
                    break;
                }
                case 5: {
                    trace("length " + ra.length());
                    buff.append("length " + ra.length() + "\n");
                    assertEquals(ra.length(), f.length());
                    break;
                }
                case 6: {
                    trace("reopen");
                    buff.append("reopen\n");
                    f.close();
                    ra.close();
                    ra = new RandomAccessFile(file, "rw");
                    f = fs.openFileObject(s, "rw");
                    assertEquals(ra.length(), f.length());
                    break;
                }
                default:
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
            fail("Exception: " + e + "\n"+ buff.toString());
        } finally {
            f.close();
            ra.close();
            file.delete();
            fs.delete(s);
        }
    }

    private void testTempFile(String fsBase) throws Exception {
        int len = 10000;
        FileSystem fs = FileSystem.getInstance(fsBase);
        String s = fs.createTempFile(fsBase + "/tmp", ".tmp", false, false);
        OutputStream out = fs.openFileOutputStream(s, false);
        byte[] buffer = new byte[len];
        out.write(buffer);
        out.close();
        out = fs.openFileOutputStream(s, true);
        out.write(1);
        out.close();
        InputStream in = fs.openFileInputStream(s);
        for (int i = 0; i < len; i++) {
            assertEquals(0, in.read());
        }
        assertEquals(1, in.read());
        assertEquals(-1, in.read());
        in.close();
        out.close();
        fs.delete(s);
    }

}
