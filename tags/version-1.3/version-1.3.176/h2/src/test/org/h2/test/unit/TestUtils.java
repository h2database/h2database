/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Random;
import org.h2.test.TestBase;
import org.h2.util.IOUtils;
import org.h2.util.Utils;

/**
 * Tests reflection utilities.
 */
public class TestUtils extends TestBase {

    /**
     * Dummy field
     */
    public final String testField = "abc";

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        testIOUtils();
        testSortTopN();
        testSortTopNRandom();
        testWriteReadLong();
        testGetNonPrimitiveClass();
        testGetNonPrimitiveClass();
        testGetNonPrimitiveClass();
        testReflectionUtils();
    }

    private void testIOUtils() throws IOException {
        for (int i = 0; i < 20; i++) {
            byte[] data = new byte[i];
            InputStream in = new ByteArrayInputStream(data);
            byte[] buffer = new byte[i];
            assertEquals(0, IOUtils.readFully(in, buffer, -2));
            assertEquals(0, IOUtils.readFully(in, buffer, -1));
            assertEquals(0, IOUtils.readFully(in, buffer, 0));
            for (int j = 1, off = 0;; j += 1) {
                int read = Math.max(0, Math.min(i - off, j));
                int l = IOUtils.readFully(in, buffer, j);
                assertEquals(read, l);
                off += l;
                if (l == 0) {
                    break;
                }
            }
            assertEquals(0, IOUtils.readFully(in, buffer, 1));
        }
        for (int i = 0; i < 10; i++) {
            char[] data = new char[i];
            Reader in = new StringReader(new String(data));
            char[] buffer = new char[i];
            assertEquals(0, IOUtils.readFully(in, buffer, -2));
            assertEquals(0, IOUtils.readFully(in, buffer, -1));
            assertEquals(0, IOUtils.readFully(in, buffer, 0));
            for (int j = 1, off = 0;; j += 1) {
                int read = Math.max(0, Math.min(i - off, j));
                int l = IOUtils.readFully(in, buffer, j);
                assertEquals(read, l);
                off += l;
                if (l == 0) {
                    break;
                }
            }
            assertEquals(0, IOUtils.readFully(in, buffer, 1));
        }
    }

    private void testWriteReadLong() {
        byte[] buff = new byte[8];
        for (long x : new long[]{Long.MIN_VALUE, Long.MAX_VALUE, 0, 1, -1,
                Integer.MIN_VALUE, Integer.MAX_VALUE}) {
            Utils.writeLong(buff, 0, x);
            long y = Utils.readLong(buff, 0);
            assertEquals(x, y);
        }
        Random r = new Random(1);
        for (int i = 0; i < 1000; i++) {
            long x = r.nextLong();
            Utils.writeLong(buff, 0, x);
            long y = Utils.readLong(buff, 0);
            assertEquals(x, y);
        }
    }

    private void testSortTopN() {
        Comparator<Integer> comp = new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1.compareTo(o2);
            }
        };
        Integer[] arr = new Integer[] {};
        Utils.sortTopN(arr, 0, 5, comp);

        arr = new Integer[] { 1 };
        Utils.sortTopN(arr, 0, 5, comp);

        arr = new Integer[] { 3, 5, 1, 4, 2 };
        Utils.sortTopN(arr, 0, 2, comp);
        assertEquals(arr[0].intValue(), 1);
        assertEquals(arr[1].intValue(), 2);
    }

    private void testSortTopNRandom() {
        Random rnd = new Random();
        Comparator<Integer> comp = new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1.compareTo(o2);
            }
        };
        for (int z = 0; z < 10000; z++) {
            Integer[] arr = new Integer[1 + rnd.nextInt(500)];
            for (int i = 0; i < arr.length; i++) {
                arr[i] = rnd.nextInt(50);
            }
            Integer[] arr2 = Arrays.copyOf(arr, arr.length);
            int offset = rnd.nextInt(arr.length);
            int limit = rnd.nextInt(arr.length);
            Utils.sortTopN(arr, offset, limit, comp);
            Arrays.sort(arr2, comp);
            for (int i = offset, end = Math.min(offset + limit, arr.length); i < end; i++) {
                if (!arr[i].equals(arr2[i])) {
                    fail(offset + " " + end + "\n" + Arrays.toString(arr) +
                            "\n" + Arrays.toString(arr2));
                }
            }
        }
    }

    private void testGetNonPrimitiveClass() {
        testGetNonPrimitiveClass(BigInteger.class, BigInteger.class);
        testGetNonPrimitiveClass(Boolean.class, boolean.class);
        testGetNonPrimitiveClass(Byte.class, byte.class);
        testGetNonPrimitiveClass(Character.class, char.class);
        testGetNonPrimitiveClass(Byte.class, byte.class);
        testGetNonPrimitiveClass(Double.class, double.class);
        testGetNonPrimitiveClass(Float.class, float.class);
        testGetNonPrimitiveClass(Integer.class, int.class);
        testGetNonPrimitiveClass(Long.class, long.class);
        testGetNonPrimitiveClass(Short.class, short.class);
        testGetNonPrimitiveClass(Void.class, void.class);
    }

    private void testGetNonPrimitiveClass(Class<?> expected, Class<?> p) {
        assertEquals(expected.getName(), Utils.getNonPrimitiveClass(p).getName());
    }

    private void testReflectionUtils() throws Exception {
        // Static method call
        long currentTimeMillis1 = System.currentTimeMillis();
        long currentTimeMillis2 = (Long) Utils.callStaticMethod(
                "java.lang.System.currentTimeMillis");
        assertTrue(currentTimeMillis1 <= currentTimeMillis2);
        // New Instance
        Object instance = Utils.newInstance("java.lang.StringBuilder");
        // New Instance with int parameter
        instance = Utils.newInstance("java.lang.StringBuilder", 10);
        // StringBuilder.append or length don't work on JDK 5 due to
        // http://bugs.sun.com/view_bug.do?bug_id=4283544
        instance = Utils.newInstance("java.lang.Integer", 10);
        // Instance methods
        long x = (Long) Utils.callMethod(instance, "longValue");
        assertEquals(10, x);
        // Static fields
        String pathSeparator = (String) Utils
                .getStaticField("java.io.File.pathSeparator");
        assertEquals(File.pathSeparator, pathSeparator);
        // Instance fields
        String test = (String) Utils.getField(this, "testField");
        assertEquals(this.testField, test);
        // Class present?
        assertFalse(Utils.isClassPresent("abc"));
        assertTrue(Utils.isClassPresent(getClass().getName()));
        Utils.callStaticMethod("java.lang.String.valueOf", "a");
        Utils.callStaticMethod("java.awt.AWTKeyStroke.getAWTKeyStroke",
                'x', java.awt.event.InputEvent.SHIFT_DOWN_MASK);
        // Common comparable superclass
        assertFalse(Utils.haveCommonComparableSuperclass(
                Integer.class,
                Long.class));
        assertTrue(Utils.haveCommonComparableSuperclass(
                Integer.class,
                Integer.class));
        assertTrue(Utils.haveCommonComparableSuperclass(
                Timestamp.class,
                Date.class));
        assertFalse(Utils.haveCommonComparableSuperclass(
                ArrayList.class,
                Long.class));
        assertFalse(Utils.haveCommonComparableSuperclass(
                Integer.class,
                ArrayList.class));
    }

}
