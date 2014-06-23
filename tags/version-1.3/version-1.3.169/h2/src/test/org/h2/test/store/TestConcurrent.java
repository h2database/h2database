/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.util.Map;
import java.util.Random;
import org.h2.dev.store.btree.MVMap;
import org.h2.dev.store.btree.MVStore;
import org.h2.test.TestBase;
import org.h2.util.Task;

/**
 * Tests concurrently accessing a tree map store.
 */
public class TestConcurrent extends TestMVStore {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws InterruptedException {
        testConcurrentWrite();
        testConcurrentRead();
    }

    /**
     * Test what happens on concurrent write. Concurrent write may corrupt the
     * map, so that keys and values may become null.
     */
    private void testConcurrentWrite() throws InterruptedException {
        final MVStore s = openStore(null);
        final MVMap<Integer, Integer> m = s.openMap("data", Integer.class, Integer.class);
        final int size = 20;
        final Random rand = new Random(1);
        Task task = new Task() {
            public void call() throws Exception {
                while (!stop) {
                    try {
                        if (rand.nextBoolean()) {
                            m.put(rand.nextInt(size), 1);
                        } else {
                            m.remove(rand.nextInt(size));
                        }
                        m.get(rand.nextInt(size));
                    } catch (NullPointerException e) {
                        // ignore
                    }
                }
            }
        };
        task.execute();
        Thread.sleep(1);
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 10; i++) {
                try {
                    if (rand.nextBoolean()) {
                        m.put(rand.nextInt(size), 2);
                    } else {
                        m.remove(rand.nextInt(size));
                    }
                    m.get(rand.nextInt(size));
                } catch (NullPointerException e) {
                    // ignore
                }
            }
            s.incrementVersion();
            Thread.sleep(1);
        }
        task.get();
        // verify the structure is still somewhat usable
        for (int x : m.keySet()) {
            try {
                m.get(x);
            } catch (NullPointerException e) {
                // ignore
            }
        }
        for (int i = 0; i < size; i++) {
            try {
                m.get(i);
            } catch (NullPointerException e) {
                // ignore
            }
        }
        s.close();
    }

    private void testConcurrentRead() throws InterruptedException {
        final MVStore s = openStore(null);
        final MVMap<Integer, Integer> m = s.openMap("data", Integer.class, Integer.class);
        final int size = 3;
        int x = (int) s.getCurrentVersion();
        for (int i = 0; i < size; i++) {
            m.put(i, x);
        }
        s.incrementVersion();
        Task task = new Task() {
            public void call() throws Exception {
                while (!stop) {
                    long v = s.getCurrentVersion() - 1;
                    Map<Integer, Integer> old = m.openVersion(v);
                    for (int i = 0; i < size; i++) {
                        Integer x = old.get(i);
                        if (x == null || (int) v != x) {
                            Map<Integer, Integer> old2 = m.openVersion(v);
                            throw new AssertionError(x + "<>" + v + " at " + i + " " + old2);
                        }
                    }
                }
            }
        };
        task.execute();
        Thread.sleep(1);
        for (int j = 0; j < 100; j++) {
            x = (int) s.getCurrentVersion();
            for (int i = 0; i < size; i++) {
                m.put(i, x);
            }
            s.incrementVersion();
            Thread.sleep(1);
        }
        task.get();
        s.close();
    }

}
