/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.math.BigInteger;
import org.h2.test.TestBase;

/**
 * Tests math utility methods.
 */
public class TestMathUtils extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() {
        testFactorial();
    }

    private void testFactorial() {
        try {
            factorial(-1);
            fail();
        } catch (IllegalArgumentException e) {
            // ignore
        }
        assertEquals("1", factorial(0).toString());
        assertEquals("1", factorial(1).toString());
        assertEquals("2", factorial(2).toString());
        assertEquals("6", factorial(3).toString());
        assertEquals("3628800", factorial(10).toString());
        assertEquals("2432902008176640000", factorial(20).toString());
    }

    /**
     * Calculate the factorial (n!) of a number.
     * This implementation uses a naive multiplication loop, and
     * is very slow for large n.
     * For n = 1000, it takes about 10 ms.
     * For n = 8000, it takes about 800 ms.
     *
     * @param n the number
     * @return the factorial of n
     */
    public static BigInteger factorial(int n) {
        if (n < 0) {
            throw new IllegalArgumentException(n + "<0");
        } else if (n < 2) {
            return BigInteger.ONE;
        }
        BigInteger x = new BigInteger("" + n);
        BigInteger result = x;
        for (int i = n - 1; i >= 2; i--) {
            x = x.subtract(BigInteger.ONE);
            result = result.multiply(x);
        }
        return result;
    }

}
