/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Comparator;
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
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws Exception {
        testIOUtils();
        testSortTopN();
        testSortTopNRandom();
        testGetNonPrimitiveClass();
        testGetNonPrimitiveClass();
        testGetNonPrimitiveClass();
        testReflectionUtils();
        testParseBoolean();
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

    private void testSortTopN() {
        Comparator<Integer> comp = Comparator.naturalOrder();
        Integer[] arr = new Integer[] {};
        Utils.sortTopN(arr, 0, 0, comp);

        arr = new Integer[] { 1 };
        Utils.sortTopN(arr, 0, 1, comp);

        arr = new Integer[] { 3, 5, 1, 4, 2 };
        Utils.sortTopN(arr, 0, 2, comp);
        assertEquals(arr[0].intValue(), 1);
        assertEquals(arr[1].intValue(), 2);
    }

    private void testSortTopNRandom() {
        Random rnd = new Random();
        Comparator<Integer> comp = Comparator.naturalOrder();
        for (int z = 0; z < 10000; z++) {
            int length = 1 + rnd.nextInt(500);
            Integer[] arr = new Integer[length];
            for (int i = 0; i < length; i++) {
                arr[i] = rnd.nextInt(50);
            }
            Integer[] arr2 = Arrays.copyOf(arr, length);
            int offset = rnd.nextInt(length);
            int limit = rnd.nextInt(length - offset + 1);
            Utils.sortTopN(arr, offset, offset + limit, comp);
            Arrays.sort(arr2, comp);
            for (int i = offset, end = offset + limit; i < end; i++) {
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
        long currentTimeNanos1 = System.nanoTime();
        long currentTimeNanos2 = (Long) Utils.callStaticMethod(
                "java.lang.System.nanoTime");
        assertTrue(currentTimeNanos1 <= currentTimeNanos2);
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
        // Instance fields
        Utils.callStaticMethod("java.lang.String.valueOf", "a");
        Utils.callStaticMethod("java.awt.AWTKeyStroke.getAWTKeyStroke",
                'x', java.awt.event.InputEvent.SHIFT_DOWN_MASK);
    }

    private void testParseBooleanCheckFalse(String value) {
        assertFalse(Utils.parseBoolean(value, false, false));
        assertFalse(Utils.parseBoolean(value, false, true));
        assertFalse(Utils.parseBoolean(value, true, false));
        assertFalse(Utils.parseBoolean(value, true, true));
    }

    private void testParseBooleanCheckTrue(String value) {
        assertTrue(Utils.parseBoolean(value, false, false));
        assertTrue(Utils.parseBoolean(value, false, true));
        assertTrue(Utils.parseBoolean(value, true, false));
        assertTrue(Utils.parseBoolean(value, true, true));
    }

    private void testParseBoolean() {
        // Test for default value in case of null
        assertFalse(Utils.parseBoolean(null, false, false));
        assertFalse(Utils.parseBoolean(null, false, true));
        assertTrue(Utils.parseBoolean(null, true, false));
        assertTrue(Utils.parseBoolean(null, true, true));
        // Test assorted valid strings
        testParseBooleanCheckFalse("0");
        testParseBooleanCheckFalse("f");
        testParseBooleanCheckFalse("F");
        testParseBooleanCheckFalse("n");
        testParseBooleanCheckFalse("N");
        testParseBooleanCheckFalse("no");
        testParseBooleanCheckFalse("No");
        testParseBooleanCheckFalse("NO");
        testParseBooleanCheckFalse("false");
        testParseBooleanCheckFalse("False");
        testParseBooleanCheckFalse("FALSE");
        testParseBooleanCheckTrue("1");
        testParseBooleanCheckTrue("t");
        testParseBooleanCheckTrue("T");
        testParseBooleanCheckTrue("y");
        testParseBooleanCheckTrue("Y");
        testParseBooleanCheckTrue("yes");
        testParseBooleanCheckTrue("Yes");
        testParseBooleanCheckTrue("YES");
        testParseBooleanCheckTrue("true");
        testParseBooleanCheckTrue("True");
        testParseBooleanCheckTrue("TRUE");
        // Test other values
        assertFalse(Utils.parseBoolean("BAD", false, false));
        assertTrue(Utils.parseBoolean("BAD", true, false));
        assertThrows(IllegalArgumentException.class, () -> Utils.parseBoolean("BAD", false, true));
        assertThrows(IllegalArgumentException.class, () -> Utils.parseBoolean("BAD", true, true));
    }

}
