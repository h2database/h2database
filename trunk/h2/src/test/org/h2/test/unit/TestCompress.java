/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import org.h2.compress.CompressLZF;
import org.h2.compress.Compressor;
import org.h2.engine.Constants;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.tools.CompressTool;
import org.h2.util.IOUtils;
import org.h2.util.New;
import org.h2.util.Task;

/**
 * Data compression tests.
 */
public class TestCompress extends TestBase {

    private boolean testPerformance;
    private byte[] buff = new byte[10];

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
        testVariableSizeInt();
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

    private void testVariableSizeInt() {
        assertEquals(1, CompressTool.getVariableIntLength(0));
        assertEquals(2, CompressTool.getVariableIntLength(0x80));
        assertEquals(3, CompressTool.getVariableIntLength(0x4000));
        assertEquals(4, CompressTool.getVariableIntLength(0x200000));
        assertEquals(5, CompressTool.getVariableIntLength(0x10000000));
        assertEquals(5, CompressTool.getVariableIntLength(-1));
        for (int x = 0; x < 0x20000; x++) {
            testVar(x);
            testVar(Integer.MIN_VALUE + x);
            testVar(Integer.MAX_VALUE - x);
            testVar(0x200000 + x - 100);
            testVar(0x10000000 + x - 100);
        }
    }

    private void testVar(int x) {
        int len = CompressTool.getVariableIntLength(x);
        int l2 = CompressTool.writeVariableInt(buff, 0, x);
        assertEquals(len, l2);
        int x2 = CompressTool.readVariableInt(buff, 0);
        assertEquals(x2, x);
    }

    private void testMultiThreaded() throws Exception {
        Task[] tasks = new Task[3];
        for (int i = 0; i < tasks.length; i++) {
            Task t = new Task() {
                public void call() {
                    CompressTool tool = CompressTool.getInstance();
                    byte[] buff = new byte[1024];
                    Random r = new Random();
                    while (!stop) {
                        r.nextBytes(buff);
                        byte[] test = tool.expand(tool.compress(buff, "LZF"));
                        assertEquals(buff, test);
                    }
                }
            };
            tasks[i] = t;
            t.execute();
        }
        Thread.sleep(1000);
        for (Task t : tasks) {
            t.get();
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
        int pageSize = Constants.DEFAULT_PAGE_SIZE;
        byte[] buff = new byte[pageSize];
        byte[] test = new byte[2 * pageSize];
        compress.compress(buff, pageSize, test, 0);
        for (int j = 0; j < 4; j++) {
            long time = System.currentTimeMillis();
            for (int i = 0; i < 1000; i++) {
                InputStream in = FileUtils.newInputStream("memFS:compress.h2.db");
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
            InputStream in = FileUtils.newInputStream("memFS:compress.h2.db");
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

    private void test(int len) throws IOException {
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
            // level 9 is highest, strategy 2 is huffman only
            for (String a : new String[] { "LZF", "No", "Deflate", "Deflate level 9 strategy 2" }) {
                long time = System.currentTimeMillis();
                byte[] out = utils.compress(buff, a);
                byte[] test = utils.expand(out);
                if (testPerformance) {
                    System.out.println("p:" + pattern + " len: " + out.length + " time: " + (System.currentTimeMillis() - time) + " " + a);
                }
                assertEquals(buff.length, test.length);
                assertEquals(buff, test);
                Arrays.fill(test, (byte) 0);
                CompressTool.expand(out, test, 0);
                assertEquals(buff, test);
            }
            for (String a : new String[] { null, "LZF", "DEFLATE", "ZIP", "GZIP" }) {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                OutputStream out2 = CompressTool.wrapOutputStream(out, a, "test");
                IOUtils.copy(new ByteArrayInputStream(buff), out2);
                out2.close();
                InputStream in = new ByteArrayInputStream(out.toByteArray());
                in = CompressTool.wrapInputStream(in, a, "test");
                out.reset();
                IOUtils.copy(in, out);
                assertEquals(buff, out.toByteArray());
            }
        }
    }

}
