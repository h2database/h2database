/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
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
import java.sql.SQLException;
import java.util.Random;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.Database;
import org.h2.index.Page;
import org.h2.store.PageInputStream;
import org.h2.store.PageOutputStream;
import org.h2.store.PageStore;
import org.h2.test.TestBase;
import org.h2.util.IntArray;

/**
 * Test the page store.
 */
public class TestPageStore extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        testAllocateFree();
        testStreamFuzz();
        testStreamPerformance(false, 1000);
        // testPerformance(true, 1000000);
        // testPerformance(false, 1000000);
    }

    private void testAllocateFree() throws SQLException {
        String fileName = getTestDir("/pageStore");
        new File(fileName).delete();
        File f = new File(fileName + ".dat");
        f.delete();
        Database db = getDatabase();
        PageStore store = new PageStore(db, fileName, "rw", 8192);
        store.setPageSize(1024);
        store.open();
        IntArray list = new IntArray();
        int size = 270;
        for (int i = 0; i < size; i++) {
            int id = store.allocatePage();
            list.add(id);
        }
        for (int i = 0; i < size; i++) {
            int id = list.get(i);
            store.freePage(id);
        }
        for (int i = 0; i < size; i++) {
            int id = store.allocatePage();
            int expected = list.get(list.size() - 1 - i);
            assertEquals(expected, id);
        }
        store.close();
        db.shutdownImmediately();
        new File(fileName).delete();
        f.delete();
    }

    private Database getDatabase() throws SQLException {
        String name = "mem:pageStore";
        ConnectionInfo ci = new ConnectionInfo(name);
        return new Database(name, ci, null);
    }

    private void testStreamPerformance(boolean file, int count) throws Exception {
        String fileName = getTestDir("/pageStore");
        new File(fileName).delete();
        File f = new File(fileName + ".dat");
        f.delete();
        Database db = getDatabase();
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
            out = new PageOutputStream(store, 0, head, Page.TYPE_LOG, false);
        }
        for (int i = 0; i < count; i++) {
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
        println((file ? "file" : "pageStore") +
                " " + (System.currentTimeMillis() - start));
        store.close();
        db.shutdownImmediately();
        new File(fileName).delete();
        f.delete();
    }

    private void testStreamFuzz() throws Exception {
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
            PageOutputStream out = new PageOutputStream(store, 0, head, Page.TYPE_LOG, false);
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
