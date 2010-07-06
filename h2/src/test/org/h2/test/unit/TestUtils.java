/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.File;
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
        // Static method call
        long currentTimeMillis1 = System.currentTimeMillis();
        long currentTimeMillis2 = (Long) Utils.callStaticMethod("java.lang.System.currentTimeMillis");
        assertTrue(currentTimeMillis1 <= currentTimeMillis2);
        // New Instance
        Object instance = Utils.newInstance("java.lang.StringBuilder");
        // New Instance with int parameter
        instance = Utils.newInstance("java.lang.StringBuilder", 10);
        // Instance methods
        instance = Utils.newInstance("java.lang.StringBuilder", "abc");
        // Instance methods
        int length = (Integer) Utils.callMethod(instance, "length");
        assertEquals(3, length);
        // Static fields
        String pathSeparator = (String) Utils.getStaticField("java.io.File.pathSeparator");
        assertEquals(File.pathSeparator, pathSeparator);
        // Instance fields
        String testField = (String) Utils.getField(this, "testField");
        assertEquals(this.testField, testField);
        // Class present?
        assertFalse(Utils.isClassPresent("abc"));
        assertTrue(Utils.isClassPresent(getClass().getName()));
        Utils.callStaticMethod("java.lang.String.valueOf", "a");
        Utils.callStaticMethod("java.awt.AWTKeyStroke.getAWTKeyStroke",
                'x', java.awt.event.InputEvent.SHIFT_DOWN_MASK);
    }

}
