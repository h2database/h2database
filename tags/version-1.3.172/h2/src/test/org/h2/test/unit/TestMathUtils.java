/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.math.BigInteger;
import java.util.Random;
import org.h2.test.TestBase;
import org.h2.test.utils.AssertThrows;
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
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() {
        testRandom();
        testReverse();
        testFactorial();
    }

    private void testRandom() {
        int bits = 0;
        for (int i = 0; i < 1000; i++) {
            bits |= 1 << MathUtils.randomInt(8);
        }
        assertEquals(255, bits);
        bits = 0;
        for (int i = 0; i < 1000; i++) {
            bits |= 1 << MathUtils.secureRandomInt(8);
        }
        assertEquals(255, bits);
        bits = 0;
        for (int i = 0; i < 1000; i++) {
            bits |= 1 << (MathUtils.secureRandomLong() & 7);
        }
        assertEquals(255, bits);
        // just verify the method doesn't throw an exception
        byte[] data = MathUtils.generateAlternativeSeed();
        assertTrue(data.length > 10);
    }

    private void testReverse() {
        assertEquals(Integer.reverse(0), Integer.reverse(0));
        assertEquals(Integer.reverse(Integer.MAX_VALUE), Integer.reverse(Integer.MAX_VALUE));
        assertEquals(Integer.reverse(Integer.MIN_VALUE), Integer.reverse(Integer.MIN_VALUE));
        assertEquals(Long.reverse(0), Long.reverse(0L));
        assertEquals(Long.reverse(Long.MAX_VALUE), Long.reverse(Long.MAX_VALUE));
        assertEquals(Long.reverse(Long.MIN_VALUE), Long.reverse(Long.MIN_VALUE));
        for (int i = Integer.MIN_VALUE; i < 0; i += 1019) {
            int x = Integer.reverse(i);
            assertEquals(Integer.reverse(i), x);
        }
        for (int i = 0; i > 0; i += 1019) {
            int x = Integer.reverse(i);
            assertEquals(Integer.reverse(i), x);
        }
        for (long i = Long.MIN_VALUE; i < 0; i += 1018764321251L) {
            long x = Long.reverse(i);
            assertEquals(Long.reverse(i), x);
        }
        for (long i = 0; i > 0; i += 1018764321251L) {
            long x = Long.reverse(i);
            assertEquals(Long.reverse(i), x);
        }
        Random random = new Random(10);
        for (int i = 0; i < 1000000; i++) {
            long x = random.nextLong();
            long r = Long.reverse(x);
            assertEquals(Long.reverse(x), r);
            int y = random.nextInt();
            int s = Integer.reverse(y);
            assertEquals(Integer.reverse(y), s);
        }
    }

    private void testFactorial() {
        new AssertThrows(IllegalArgumentException.class) { @Override
        public void test() {
            factorial(-1);
        }};
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
