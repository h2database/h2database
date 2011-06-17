/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.SQLException;

import org.h2.test.TestBase;
import org.h2.util.MathUtils;

/**
 * Tests math utility methods.
 */
public class TestMathUtils extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws SQLException {
        try {
            MathUtils.factorial(-1);
            fail();
        } catch (IllegalArgumentException e) {
            // ignore
        }
        assertEquals("1", MathUtils.factorial(0).toString());
        assertEquals("1", MathUtils.factorial(1).toString());
        assertEquals("2", MathUtils.factorial(2).toString());
        assertEquals("6", MathUtils.factorial(3).toString());
        assertEquals("3628800", MathUtils.factorial(10).toString());
        assertEquals("2432902008176640000", MathUtils.factorial(20).toString());
    }
}
