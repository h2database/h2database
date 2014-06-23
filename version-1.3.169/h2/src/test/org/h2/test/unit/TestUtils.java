/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.File;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import org.h2.test.TestBase;
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

    public void test() throws Exception {
        testWriteReadLong();
        testGetNonPrimitiveClass();
        testGetNonPrimitiveClass();
        testGetNonPrimitiveClass();
        testReflectionUtils();
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

    private void testGetNonPrimitiveClass() throws Exception {
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
        long currentTimeMillis2 = (Long) Utils.callStaticMethod("java.lang.System.currentTimeMillis");
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
        String pathSeparator = (String) Utils.getStaticField("java.io.File.pathSeparator");
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
        assertFalse(Utils.haveCommonComparableSuperclass(Integer.class, Long.class));
        assertTrue(Utils.haveCommonComparableSuperclass(Integer.class, Integer.class));
        assertTrue(Utils.haveCommonComparableSuperclass(Timestamp.class, Date.class));
        assertFalse(Utils.haveCommonComparableSuperclass(ArrayList.class, Long.class));
        assertFalse(Utils.haveCommonComparableSuperclass(Integer.class, ArrayList.class));
    }

}
