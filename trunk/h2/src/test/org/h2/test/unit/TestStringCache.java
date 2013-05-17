/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.util.Random;

import org.h2.test.TestBase;
import org.h2.util.StringUtils;

/**
 * Tests the string cache facility.
 */
public class TestStringCache extends TestBase {

    /**
     * Flag to indicate the test should stop.
     */
    volatile boolean stop;
    private final Random random = new Random(1);
    private final String[] some = { null, "", "ABC", "this is a medium sized string", "1", "2" };
    private boolean returnNew;
    private boolean useIntern;

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) {
        new TestStringCache().runBenchmark();
    }

    @Override
    public void test() throws InterruptedException {
        returnNew = true;
        StringUtils.clearCache();
        testSingleThread(getSize(5000, 20000));
        testMultiThreads();
        returnNew = false;
        StringUtils.clearCache();
        testSingleThread(getSize(5000, 20000));
        testMultiThreads();
    }

    private void runBenchmark() {
        returnNew = false;
        for (int i = 0; i < 6; i++) {
            useIntern = (i % 2) == 0;
            long time = System.currentTimeMillis();
            testSingleThread(100000);
            time = System.currentTimeMillis() - time;
            System.out.println(time + " ms (useIntern=" + useIntern + ")");
        }

    }

    private String randomString() {
        if (random.nextBoolean()) {
            String s = some[random.nextInt(some.length)];
            if (s != null && random.nextBoolean()) {
                s = new String(s);
            }
            return s;
        }
        int len = random.nextBoolean() ? random.nextInt(1000) : random.nextInt(10);
        StringBuilder buff = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            buff.append(random.nextInt(0xfff));
        }
        return buff.toString();
    }

    /**
     * Test one string operation using the string cache.
     */
    void testString() {
        String a = randomString();
        if (returnNew) {
            String b = StringUtils.fromCacheOrNew(a);
            try {
                assertEquals(a, b);
            } catch (Exception e) {
                TestBase.logError("error", e);
            }
            if (a != null && a == b && a.length() > 0) {
                throw new AssertionError("a=" + System.identityHashCode(a) + " b=" + System.identityHashCode(b));
            }
        } else {
            String b;
            if (useIntern) {
                b = a == null ? null : a.intern();
            } else {
                b = StringUtils.cache(a);
            }
            try {
                assertEquals(a, b);
            } catch (Exception e) {
                TestBase.logError("error", e);
            }
        }
    }

    private void testSingleThread(int len) {
        for (int i = 0; i < len; i++) {
            testString();
        }
    }

    private void testMultiThreads() throws InterruptedException {
        int threadCount = getSize(3, 100);
        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (!stop) {
                        testString();
                    }
                }
            });
            threads[i] = t;
        }
        for (int i = 0; i < threadCount; i++) {
            threads[i].start();
        }
        int wait = getSize(200, 2000);
        Thread.sleep(wait);
        stop = true;
        for (int i = 0; i < threadCount; i++) {
            threads[i].join();
        }
    }

}
