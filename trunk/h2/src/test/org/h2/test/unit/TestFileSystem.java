/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.Random;

import org.h2.store.fs.FileObject;
import org.h2.store.fs.FileSystem;
import org.h2.store.fs.FileSystemMemory;
import org.h2.test.TestBase;

public class TestFileSystem extends TestBase {

    public void test() throws Exception {
        testFileSystem(FileSystemMemory.MEMORY_PREFIX);
        testFileSystem(baseDir + "/fs");
        testFileSystem(FileSystemMemory.MEMORY_PREFIX_LZF);
        int test;
//        testFileSystem("jdbc:h2:mem:fs;TRACE_LEVEL_FILE=0/");
    }

    private void testFileSystem(String fsBase) throws Exception {
        testTempFile(fsBase);
        testRandomAccess(fsBase);
    }
    
    private void testRandomAccess(String fsBase) throws Exception {
        FileSystem fs = FileSystem.getInstance(fsBase);
        String s = fs.createTempFile(fsBase + "/temp", ".tmp", false, false);
        File file = new File(baseDir + "/temp");
        file.delete();
        RandomAccessFile ra = new RandomAccessFile(file, "rw");
        fs.delete(s);
        FileObject f = fs.openFileObject(s, "rw");
        Random random = new Random(1);
        for (int i = 0; i < 200; i++) {
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
    }
    

}
