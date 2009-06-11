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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import org.h2.constant.SysProperties;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.Database;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.Page;
import org.h2.index.PageScanIndex;
import org.h2.result.Row;
import org.h2.schema.Schema;
import org.h2.store.PageInputStream;
import org.h2.store.PageOutputStream;
import org.h2.store.PageStore;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableData;
import org.h2.test.TestBase;
import org.h2.util.IntArray;
import org.h2.util.ObjectArray;
import org.h2.value.Value;
import org.h2.value.ValueInt;

/**
 * Test the page store.
 */
public class TestPageStore extends TestBase {

    private Database db;
    private Schema schema;
    private TableData table;
    private Index index;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        System.setProperty("h2.pageStore", "true");
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        testFuzzOperations();
        testScanIndex();
        // testBtreeIndex();
        // testAllocateFree();
        // testStreamFuzz();
        // testStreamPerformance(false, 1000);
    }

    private void testFuzzOperations() throws SQLException {
        int best = Integer.MAX_VALUE;
        for (int i = 0; i < 10; i++) {
            int x = testFuzzOperationsSeed(i, 10);
            if (x >= 0 && x < best) {
                best = x;
                fail("op:" + x + " seed:" + i);
            }
        }
    }

    private int testFuzzOperationsSeed(int seed, int len) throws SQLException {
        deleteDb("test");
        Connection conn = getConnection("test");
        Statement stat = conn.createStatement();
        log("DROP TABLE IF EXISTS TEST;");
        stat.execute("DROP TABLE IF EXISTS TEST");
        log("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR DEFAULT 'Hello World');");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR DEFAULT 'Hello World')");
        Set<Integer> rows = new TreeSet<Integer>();
        Random random = new Random(seed);
        for (int i = 0; i < len; i++) {
            int op = random.nextInt(3);
            Integer x = new Integer(random.nextInt(100));
            switch(op) {
            case 0:
                if (!rows.contains(x)) {
                    log("insert into test(id) values(" + x + ");");
                    stat.execute("INSERT INTO TEST(ID) VALUES("+ x + ");");
                    rows.add(x);
                }
                break;
            case 1:
                if (rows.contains(x)) {
                    log("delete from test where id=" + x + ";");
                    stat.execute("DELETE FROM TEST WHERE ID=" + x);
                    rows.remove(x);
                }
                break;
            case 2:
                conn.close();
                conn = getConnection("test");
                stat = conn.createStatement();
                ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
                log("--reconnect");
                for (int test : rows) {
                    if (!rs.next()) {
                        log("error: expected next");
                        conn.close();
                        return i;
                    }
                    int y = rs.getInt(1);
                    // System.out.println(" " + x);
                    if (y != test) {
                        log("error: " + y + " <> " + test);
                        conn.close();
                        return i;
                    }
                }
                if (rs.next()) {
                    log("error: unexpected next");
                    conn.close();
                    return i;
                }
            }
        }
        conn.close();
        return -1;
    }

    private void log(String m) {
        trace("   " + m);
    }

    private void testBtreeIndex() throws SQLException {
        if (!SysProperties.PAGE_STORE) {
            return;
        }
        deleteDb("pageStore");
        String fileName = getTestDir("/pageStore");
        new File(fileName).delete();
        File f = new File(fileName + ".dat");
        f.delete();
        db = getDatabase();
        PageStore store = new PageStore(db, fileName, "rw", 8192);
        store.setPageSize(1024);
        store.open();
        openBtreeIndex();
        Row row;
        for (int i = 10; i < 100; i += 10) {
            row = table.getTemplateRow();
            row.setValue(0, ValueInt.get(i));
            row.setPos(i);
            index.add(db.getSystemSession(), row);
        }
        row = table.getTemplateRow();
        row.setValue(0, ValueInt.get(60));
        row.setPos(60);
        index.remove(db.getSystemSession(), row);
        row = table.getTemplateRow();
        row.setValue(0, ValueInt.get(60));
        row.setPos(60);
        index.add(db.getSystemSession(), row);
        store.checkpoint();
        store.close();
        store = new PageStore(db, fileName, "rw", 8192);
        store.open();
        openBtreeIndex();
        Cursor cursor = index.find(db.getSystemSession(), null, null);
        for (int i = 10; i < 100; i += 10) {
            assertTrue(cursor.next());
            Row r = cursor.get();
            assertEquals(i, r.getValue(0).getInt());
        }
        assertFalse(cursor.next());
        store.close();
        db.shutdownImmediately();
    }

    private void testScanIndex() throws SQLException {
        if (!SysProperties.PAGE_STORE) {
            return;
        }
        deleteDb("pageStore");
        String fileName = getTestDir("/pageStore");
        new File(fileName).delete();
        File f = new File(fileName + ".dat");
        f.delete();
        db = getDatabase();
        PageStore store = new PageStore(db, fileName, "rw", 8192);
        store.setPageSize(1024);
        store.open();
        openScanIndex();
        Row row;
        for (int i = 10; i < 100; i += 10) {
            row = table.getTemplateRow();
            row.setValue(0, ValueInt.get(i));
            row.setPos(i);
            index.add(db.getSystemSession(), row);
        }
        row = table.getTemplateRow();
        row.setValue(0, ValueInt.get(60));
        row.setPos(60);
        index.remove(db.getSystemSession(), row);
        row = table.getTemplateRow();
        row.setValue(0, ValueInt.get(60));
        row.setPos(60);
        index.add(db.getSystemSession(), row);
        store.checkpoint();
        store.close();
        store = new PageStore(db, fileName, "rw", 8192);
        store.open();
        openScanIndex();
        Cursor cursor = index.find(db.getSystemSession(), null, null);
        for (int i = 10; i < 100; i += 10) {
            assertTrue(cursor.next());
            Row r = cursor.get();
            assertEquals(i, r.getValue(0).getInt());
        }
        assertFalse(cursor.next());
        store.close();
        db.shutdownImmediately();
    }

    private Database getDatabase() throws SQLException {
        String name = getTestDir("/pageStore");
        ConnectionInfo ci = new ConnectionInfo(name);
        return new Database(name, ci, null);
    }

    private void openScanIndex() throws SQLException {
        ObjectArray cols = ObjectArray.newInstance();
        cols.add(new Column("ID", Value.INT));
        schema = new Schema(db, 0, "", null, true);
        table = new TableData(schema, "PAGE_INDEX", 1, cols, true, true, false, 100, null);
        index = (PageScanIndex) table.getScanIndex(
                db.getSystemSession());
    }

    private void openBtreeIndex() throws SQLException {
        ObjectArray cols = ObjectArray.newInstance();
        cols.add(new Column("ID", Value.INT));
        schema = new Schema(db, 0, "", null, true);
        int id = db.allocateObjectId(true, true);
        table = new TableData(schema, "BTREE_INDEX", id, cols, true, true, false, 100, null);
        id = db.allocateObjectId(true, true);
        table.addIndex(db.getSystemSession(), "BTREE", id,
                IndexColumn.wrap(table.getColumns()),
                IndexType.createNonUnique(true),
                Index.EMPTY_HEAD, "");
        index = (PageScanIndex) table.getScanIndex(
                db.getSystemSession());
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
            store.freePage(id, false, null);
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
            out = new PageOutputStream(store, 0);
        }
        for (int i = 0; i < count; i++) {
            out.write(buff);
        }
        out.close();
        if (file) {
            in = new BufferedInputStream(new FileInputStream(f), 4 * 1024);
        } else {
            in = new PageInputStream(store, 0);
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
            PageOutputStream out = new PageOutputStream(store, 0);
            for (int p = 0; p < len;) {
                int l = len == 0 ? 0 : Math.min(len - p, random.nextInt(len / 10));
                out.write(data, p, l);
                p += l;
            }
            out.close();
            PageInputStream in = new PageInputStream(store, 0);
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
