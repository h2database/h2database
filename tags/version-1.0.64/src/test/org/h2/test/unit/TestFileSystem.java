/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.EOFException;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;

import org.h2.store.fs.FileObject;
import org.h2.store.fs.FileSystem;
import org.h2.test.TestBase;

public class TestFileSystem extends TestBase {

    public void test() throws Exception {
        testDatabaseInJar();
        testFileSystem(baseDir + "/fs");
        testFileSystem(FileSystem.MEMORY_PREFIX);
        // testFileSystem("jdbc:h2:mem:fs;TRACE_LEVEL_FILE=3");
        testFileSystem("jdbc:h2:mem:fs");
        testFileSystem(FileSystem.MEMORY_PREFIX_LZF);
        testUserHome();
    }

    private void testDatabaseInJar() throws Exception {
        if (config.networked) {
            return;
        }
        Class.forName("org.h2.Driver");
        String url = "jdbc:h2:" + baseDir + "/fsJar";
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
        stat.execute("backup to '" + baseDir + "/fsJar.zip'");
        conn.close();

        deleteDb(baseDir + "/fsJar");
        FileSystem fs = FileSystem.getInstance("zip:" + baseDir + "/fsJar.zip");
        String[] files = fs.listFiles("zip:" + baseDir + "/fsJar.zip");
        for (int i = 0; i < files.length; i++) {
            String f = files[i];
            check(fs.isAbsolute(f));
            check(!fs.isDirectory(f));
            check(fs.length(f) > 0);
            check(f.endsWith(fs.getFileName(f)));
        }
        String urlJar = "jdbc:h2:zip:" + baseDir + "/fsJar.zip!/fsJar";
        conn = DriverManager.getConnection(urlJar, "sa", "sa");
        stat = conn.createStatement();
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

    private void testUserHome() throws Exception {
        FileSystem fs = FileSystem.getInstance("~/test");
        String fileName = fs.getAbsolutePath("~/test");
        String userDir = System.getProperty("user.home");
        check(fileName.startsWith(userDir));
    }

    private void testFileSystem(String fsBase) throws Exception {
        testSimple(fsBase);
        testTempFile(fsBase);
        testRandomAccess(fsBase);
    }

    private void testSimple(String fsBase) throws Exception {
        FileSystem fs = FileSystem.getInstance(fsBase);
        long time = System.currentTimeMillis();
        String[] list = fs.listFiles(fsBase);
        for (int i = 0; i < list.length; i++) {
            fs.delete(list[i]);
        }
        fs.mkdirs(fsBase + "/test");
        fs.delete(fsBase + "/test");
        fs.delete(fsBase + "/test2");
        check(fs.createNewFile(fsBase + "/test"));
        check(fs.canWrite(fsBase + "/test"));
        FileObject fo = fs.openFileObject(fsBase + "/test", "rw");
        byte[] buffer = new byte[10000];
        Random random = new Random(1);
        random.nextBytes(buffer);
        fo.write(buffer, 0, 10000);
        fo.close();
        check(fs.getLastModified(fsBase + "/test") >= time);
        check(fs.length(fsBase + "/test"), 10000);
        list = fs.listFiles(fsBase);
        check(list.length, 1);
        check(list[0].endsWith("test"));

        fs.copy(fsBase + "/test", fsBase + "/test3");
        fs.rename(fsBase + "/test3", fsBase + "/test2");
        check(!fs.exists(fsBase + "/test3"));
        check(fs.exists(fsBase + "/test2"));
        check(fs.length(fsBase + "/test2"), 10000);
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
        check(pos, 10000);
        check(buffer2, buffer);

        check(fs.tryDelete(fsBase + "/test2"));
        fs.delete(fsBase + "/test");

        if (!fsBase.startsWith(FileSystem.MEMORY_PREFIX) && !fsBase.startsWith(FileSystem.MEMORY_PREFIX_LZF)) {
            fs.createDirs(fsBase + "/testDir/test");
            check(fs.isDirectory(fsBase + "/testDir"));
            if (!fsBase.startsWith(FileSystem.DB_PREFIX)) {
                fs.deleteRecursive("/testDir");
                check(!fs.exists("/testDir"));
            }
        }
    }

    private void testRandomAccess(String fsBase) throws Exception {
        FileSystem fs = FileSystem.getInstance(fsBase);
        String s = fs.createTempFile(fsBase + "/temp", ".tmp", false, false);
        File file = new File(baseDir + "/temp");
        file.delete();
        RandomAccessFile ra = new RandomAccessFile(file, "rw");
        fs.delete(s);
        FileObject f = fs.openFileObject(s, "rw");
        try {
            f.readFully(new byte[1], 0, 1);
            error("Unexpected success");
        } catch (EOFException e) {
            // expected
        }
        f.sync();
        Random random = new Random(1);
        int size = getSize(100, 500);
        for (int i = 0; i < size; i++) {
            int pos = random.nextInt(10000);
            switch(random.nextInt(7)) {
            case 0: {
                pos = (int) Math.min(pos, ra.length());
                trace("seek " + pos);
                f.seek(pos);
                ra.seek(pos);
                break;
            }
            case 1: {
                byte[] buffer = new byte[random.nextInt(1000)];
                random.nextBytes(buffer);
                trace("write " + buffer.length);
                f.write(buffer, 0, buffer.length);
                ra.write(buffer, 0, buffer.length);
                break;
            }
            case 2: {
                f.setLength(pos);
                ra.setLength(pos);
                if (ra.getFilePointer() > pos) {
                    f.seek(0);
                    ra.seek(0);
                }
                trace("setLength " + pos);
                break;
            }
            case 3: {
                int len = random.nextInt(1000);
                len = (int) Math.min(len, ra.length() - ra.getFilePointer());
                byte[] b1 = new byte[len];
                byte[] b2 = new byte[len];
                f.readFully(b1, 0, len);
                ra.readFully(b2, 0, len);
                trace("readFully " + len);
                check(b1, b2);
                break;
            }
            case 4: {
                trace("getFilePointer");
                check(f.getFilePointer(), ra.getFilePointer());
                break;
            }
            case 5: {
                trace("length " + ra.length());
                check(f.length(), ra.length());
                break;
            }
            case 6: {
                trace("reopen");
                f.close();
                ra.close();
                ra = new RandomAccessFile(file, "rw");
                f = fs.openFileObject(s, "rw");
                check(f.length(), ra.length());
                break;
            }
            }
        }
        f.close();
        ra.close();
    }

    private void testTempFile(String fsBase) throws Exception {
        FileSystem fs = FileSystem.getInstance(fsBase);
        String s = fs.createTempFile(fsBase + "/temp", ".tmp", false, false);
        OutputStream out = fs.openFileOutputStream(s, false);
        byte[] buffer = new byte[10000];
        out.write(buffer);
        out.close();
        out = fs.openFileOutputStream(s, true);
        out.write(1);
        out.close();
        InputStream in = fs.openFileInputStream(s);
        for (int i = 0; i < 10000; i++) {
            check(in.read(), 0);
        }
        check(in.read(), 1);
        check(in.read(), -1);
        in.close();
        out.close();
    }


}
