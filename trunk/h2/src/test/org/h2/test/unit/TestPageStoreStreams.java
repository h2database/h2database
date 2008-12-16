/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.Database;
import org.h2.index.Page;
import org.h2.store.PageInputStream;
import org.h2.store.PageOutputStream;
import org.h2.store.PageStore;
import org.h2.test.TestBase;

/**
 * Test page store input and output streams.
 */
public class TestPageStoreStreams extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        testFuzz();
//        for (int i = 0; i < 4; i++) {
//            testPerformance(true);
//            testPerformance(false);
//        }
    }

    private void testPerformance(boolean file) throws Exception {
        String name = "mem:pageStoreStreams";
        ConnectionInfo ci = new ConnectionInfo(name);
        Database db = new Database(name, ci, null);
        String fileName = getTestDir("/pageStoreStreams");
        new File(fileName).delete();
        File f = new File(fileName + ".dat");
        f.delete();
        PageStore store = new PageStore(db, fileName, "rw", 8192);
        store.setPageSize(8 * 1024);
        byte[] buff = new byte[100];
        store.open();
        int head = store.allocatePage();
        OutputStream out;
        InputStream in;
        long start = System.currentTimeMillis();
        if (file) {
            out = new BufferedOutputStream(new FileOutputStream(f), 4 * 1024);
        } else {
            out = new PageOutputStream(store, 0, head, Page.TYPE_LOG);
        }
        for (int i = 0; i < 1000000; i++) {
            out.write(buff);
        }
        out.close();
        if (file) {
            in = new BufferedInputStream(new FileInputStream(f), 4 * 1024);
        } else {
            in = new PageInputStream(store, 0, head, Page.TYPE_LOG);
        }
        while (true) {
            int len = in.read(buff);
            if (len < 0) {
                break;
            }
        }
        in.close();
        System.out.println((file ? "file" : "pageStore") +
                " " + (System.currentTimeMillis() - start));
        store.close();
        db.shutdownImmediately();
        new File(fileName).delete();
        f.delete();
    }

    private void testFuzz() throws Exception {
        String name = "mem:pageStoreStreams";
        ConnectionInfo ci = new ConnectionInfo(name);
        Database db = new Database(name, ci, null);
        String fileName = getTestDir("/pageStoreStreams");
        new File(fileName).delete();
        PageStore store = new PageStore(db, fileName, "rw", 8192);
        store.open();
        Random random = new Random(1);
        for (int i = 0; i < 10000; i += 1000) {
            int len = i == 0 ? 0 : random.nextInt(i);
            byte[] data = new byte[len];
            random.nextBytes(data);
            int head = store.allocatePage();
            PageOutputStream out = new PageOutputStream(store, 0, head, Page.TYPE_LOG);
            for (int p = 0; p < len;) {
                int l = len == 0 ? 0 : Math.min(len - p, random.nextInt(len / 10));
                out.write(data, p, l);
                p += l;
            }
            out.close();
            PageInputStream in = new PageInputStream(store, 0, head, Page.TYPE_LOG);
            byte[] data2 = new byte[len];
            for (int off = 0;;) {
                int l = random.nextInt(1 + len / 10) + 1;
                l = in.read(data2, off, l);
                if (l < 0) {
                    break;
                }
                off += l;
            }
            in.close();
            assertEquals(data, data2);
        }
        store.close();
        db.shutdownImmediately();
        new File(fileName).delete();
    }

}
