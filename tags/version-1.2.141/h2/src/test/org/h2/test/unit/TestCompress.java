/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;
import org.h2.compress.CompressLZF;
import org.h2.compress.Compressor;
import org.h2.constant.SysProperties;
import org.h2.store.fs.FileSystem;
import org.h2.test.TestBase;
import org.h2.tools.CompressTool;
import org.h2.util.New;

/**
 * Data compression tests.
 */
public class TestCompress extends TestBase {

    private boolean testPerformance;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        if (testPerformance) {
            testDatabase();
            System.exit(0);
            return;
        }
        testMultiThreaded();
        if (config.big) {
            for (int i = 0; i < 100; i++) {
                test(i);
            }
            for (int i = 100; i < 10000; i += i + i + 1) {
                test(i);
            }
        } else {
            test(0);
            test(1);
            test(7);
            test(50);
            test(200);
        }
        test(4000000);
        testVariableEnd();
    }

    private void testMultiThreaded() throws Exception {
        Thread[] threads = new Thread[3];
        final boolean[] stop = { false };
        final Exception[] ex = { null };
        for (int i = 0; i < threads.length; i++) {
            Thread t = new Thread() {
                public void run() {
                    CompressTool tool = CompressTool.getInstance();
                    byte[] buff = new byte[1024];
                    Random r = new Random();
                    while (!stop[0]) {
                        r.nextBytes(buff);
                        try {
                            byte[] test = tool.expand(tool.compress(buff, "LZF"));
                            assertEquals(buff, test);
                        } catch (Exception e) {
                            ex[0] = e;
                        }
                    }
                }
            };
            threads[i] = t;
            t.start();
        }
        try {
            Thread.sleep(1000);
            stop[0] = true;
            for (Thread t : threads) {
                t.join();
            }
        } catch (InterruptedException e) {
            // ignore
        }
        if (ex[0] != null) {
            throw ex[0];
        }
    }

    private void testVariableEnd() throws Exception {
        CompressTool utils = CompressTool.getInstance();
        StringBuilder buff = new StringBuilder();
        for (int i = 0; i < 90; i++) {
            buff.append('0');
        }
        String prefix = buff.toString();
        for (int i = 0; i < 100; i++) {
            buff = new StringBuilder(prefix);
            for (int j = 0; j < i; j++) {
                buff.append((char) ('1' + j));
            }
            String test = buff.toString();
            byte[] in = test.getBytes();
            assertEquals(in, utils.expand(utils.compress(in, "LZF")));
        }
    }

    private void testDatabase() throws Exception {
        deleteDb("memFS:compress");
        Connection conn = getConnection("memFS:compress");
        Statement stat = conn.createStatement();
        ResultSet rs;
        rs = stat.executeQuery("select table_name from information_schema.tables");
        Statement stat2 = conn.createStatement();
        while (rs.next()) {
            String table = rs.getString(1);
            if (!"COLLATIONS".equals(table)) {
                stat2.execute("create table " + table + " as select * from information_schema." + table);
            }
        }
        conn.close();
        Compressor compress = new CompressLZF();
        int pageSize = SysProperties.PAGE_SIZE;
        byte[] buff = new byte[pageSize];
        byte[] test = new byte[2 * pageSize];
        compress.compress(buff, pageSize, test, 0);
        for (int j = 0; j < 4; j++) {
            long time = System.currentTimeMillis();
            for (int i = 0; i < 1000; i++) {
                InputStream in = FileSystem.getInstance("memFS:").openFileInputStream("memFS:compress.h2.db");
                int total = 0;
                while (true) {
                    int len = in.read(buff);
                    if (len < 0) {
                        break;
                    }
                    total += compress.compress(buff, pageSize, test, 0);
                }
                in.close();
            }
            System.out.println("compress: " + (System.currentTimeMillis() - time) + " ms");
        }

        for (int j = 0; j < 4; j++) {
            ArrayList<byte[]> comp = New.arrayList();
            InputStream in = FileSystem.getInstance("memFS:").openFileInputStream("memFS:compress.h2.db");
            while (true) {
                int len = in.read(buff);
                if (len < 0) {
                    break;
                }
                int b = compress.compress(buff, pageSize, test, 0);
                byte[] data = new byte[b];
                System.arraycopy(test, 0, data, 0, b);
                comp.add(data);
            }
            in.close();
            byte[] result = new byte[pageSize];
            long time = System.currentTimeMillis();
            for (int i = 0; i < 1000; i++) {
                for (int k = 0; k < comp.size(); k++) {
                    byte[] data = comp.get(k);
                    compress.expand(data, 0, data.length, result, 0, pageSize);
                }
            }
            System.out.println("expand: " + (System.currentTimeMillis() - time) + " ms");
        }
    }

    private void test(int len) {
        Random r = new Random(len);
        for (int pattern = 0; pattern < 4; pattern++) {
            byte[] buff = new byte[len];
            switch (pattern) {
            case 0:
                // leave empty
                break;
            case 1: {
                r.nextBytes(buff);
                break;
            }
            case 2: {
                for (int x = 0; x < len; x++) {
                    buff[x] = (byte) (x & 10);
                }
                break;
            }
            case 3: {
                for (int x = 0; x < len; x++) {
                    buff[x] = (byte) (x / 10);
                }
                break;
            }
            default:
            }
            if (r.nextInt(2) < 1) {
                for (int x = 0; x < len; x++) {
                    if (r.nextInt(20) < 1) {
                        buff[x] = (byte) (r.nextInt(255));
                    }
                }
            }
            CompressTool utils = CompressTool.getInstance();
            for (String a : new String[] { "LZF", "No", "Deflate" }) {
                long time = System.currentTimeMillis();
                byte[] out = utils.compress(buff, a);
                byte[] test = utils.expand(out);
                if (testPerformance) {
                    System.out.println("p:" + pattern + " len: " + out.length + " time: " + (System.currentTimeMillis() - time) + " " + a);
                }
                assertEquals(buff.length, test.length);
                assertEquals(buff, test);
            }
        }
    }

}
