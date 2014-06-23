/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVMapConcurrent;
import org.h2.mvstore.MVStore;
import org.h2.store.fs.FileChannelInputStream;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.util.New;
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

    @Override
    public void test() throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        FileUtils.createDirectories(getBaseDir());
        FileUtils.deleteRecursive("memFS:", false);

        testConcurrentFree();
        testConcurrentStoreAndRemoveMap();
        testConcurrentStoreAndClose();
        testConcurrentOnlineBackup();
        testConcurrentMap();
        testConcurrentIterate();
        testConcurrentWrite();
        testConcurrentRead();
    }

    private void testConcurrentFree() throws InterruptedException {
        String fileName = "memFS:testConcurrentFree.h3";
        for (int test = 0; test < 10; test++) {
            FileUtils.delete(fileName);
            final MVStore s1 = new MVStore.Builder().
                    fileName(fileName).autoCommitDisabled().open();
            s1.setRetentionTime(0);
            final int count = 200;
            for (int i = 0; i < count; i++) {
                MVMap<Integer, Integer> m = s1.openMap("d" + i);
                m.put(1, 1);
                if (i % 2 == 0) {
                    s1.commit();
                }
            }
            s1.close();
            final MVStore s = new MVStore.Builder().
                    fileName(fileName).autoCommitDisabled().open();
            s.setRetentionTime(0);
            final ArrayList<MVMap<Integer, Integer>> list = New.arrayList();
            for (int i = 0; i < count; i++) {
                MVMap<Integer, Integer> m = s.openMap("d" + i);
                list.add(m);
            }

            final AtomicInteger counter = new AtomicInteger();
            Task task = new Task() {
                @Override
                public void call() throws Exception {
                    while (!stop) {
                        int x = counter.getAndIncrement();
                        if (x >= count) {
                            break;
                        }
                        MVMap<Integer, Integer> m = list.get(x);
                        m.clear();
                        s.removeMap(m);
                    }
                }
            };
            task.execute();
            Thread.sleep(1);
            while (true) {
                int x = counter.getAndIncrement();
                if (x >= count) {
                    break;
                }
                MVMap<Integer, Integer> m = list.get(x);
                m.clear();
                s.removeMap(m);
                if (x % 5 == 0) {
                    s.commit();
                }
            }
            task.get();
            s.commit();

            MVMap<String, String> meta = s.getMetaMap();
            int chunkCount = 0;
            for (String k : meta.keyList()) {
                if (k.startsWith("chunk.")) {
                    chunkCount++;
                }
            }
            assertEquals(1, chunkCount);
            s.close();
        }
        FileUtils.deleteRecursive("memFS:", false);
    }

    private void testConcurrentStoreAndRemoveMap() throws InterruptedException {
        String fileName = "memFS:testConcurrentStoreAndRemoveMap.h3";
        final MVStore s = openStore(fileName);
        int count = 200;
        for (int i = 0; i < count; i++) {
            MVMap<Integer, Integer> m = s.openMap("d" + i);
            m.put(1, 1);
        }
        final AtomicInteger counter = new AtomicInteger();
        Task task = new Task() {
            @Override
            public void call() throws Exception {
                while (!stop) {
                    counter.incrementAndGet();
                    s.commit();
                }
            }
        };
        task.execute();
        Thread.sleep(1);
        for (int i = 0; i < count || counter.get() < count; i++) {
            MVMap<Integer, Integer> m = s.openMap("d" + i);
            m.put(1, 10);
            s.removeMap(m);
            if (task.isFinished()) {
                break;
            }
        }
        task.get();
        s.close();
        FileUtils.deleteRecursive("memFS:", false);
    }

    private void testConcurrentStoreAndClose() throws InterruptedException {
        String fileName = "memFS:testConcurrentStoreAndClose";
        for (int i = 0; i < 10; i++) {
            FileUtils.delete(fileName);
            final MVStore s = openStore(fileName);
            final AtomicInteger counter = new AtomicInteger();
            Task task = new Task() {
                @Override
                public void call() throws Exception {
                    while (!stop) {
                        s.setStoreVersion(counter.incrementAndGet());
                        s.commit();
                    }
                }
            };
            task.execute();
            while (counter.get() < 5) {
                Thread.sleep(1);
            }
            try {
                s.close();
                // sometimes closing works, in which case
                // storing must fail at some point (not necessarily
                // immediately)
                for (int x = counter.get(), y = x; x <= y + 2; x++) {
                    Thread.sleep(1);
                }
                Exception e = task.getException();
                assertEquals(DataUtils.ERROR_CLOSED,
                        DataUtils.getErrorCode(e.getMessage()));
            } catch (IllegalStateException e) {
                // sometimes storing works, in which case
                // closing must fail
                assertEquals(DataUtils.ERROR_WRITING_FAILED,
                        DataUtils.getErrorCode(e.getMessage()));
                task.get();
            }
            s.close();
        }
        FileUtils.deleteRecursive("memFS:", false);
    }

    /**
     * Test the concurrent map implementation.
     */
    private void testConcurrentMap() throws InterruptedException {
        final MVStore s = openStore(null);
        final MVMap<Integer, Integer> m = s.openMap("data",
                new MVMapConcurrent.Builder<Integer, Integer>());
        final int size = 20;
        final Random rand = new Random(1);
        Task task = new Task() {
            @Override
            public void call() throws Exception {
                try {
                    while (!stop) {
                        if (rand.nextBoolean()) {
                            m.put(rand.nextInt(size), 1);
                        } else {
                            m.remove(rand.nextInt(size));
                        }
                        m.get(rand.nextInt(size));
                        m.firstKey();
                        m.lastKey();
                        m.ceilingKey(5);
                        m.floorKey(5);
                        m.higherKey(5);
                        m.lowerKey(5);
                        for (Iterator<Integer> it = m.keyIterator(null);
                                it.hasNext();) {
                            it.next();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        task.execute();
        Thread.sleep(1);
        for (int j = 0; j < 100; j++) {
            for (int i = 0; i < 100; i++) {
                if (rand.nextBoolean()) {
                    m.put(rand.nextInt(size), 2);
                } else {
                    m.remove(rand.nextInt(size));
                }
                m.get(rand.nextInt(size));
            }
            s.commit();
            Thread.sleep(1);
        }
        task.get();
        s.close();
    }

    private void testConcurrentOnlineBackup() throws Exception {
        String fileName = getBaseDir() + "/onlineBackup.h3";
        String fileNameRestore = getBaseDir() + "/onlineRestore.h3";
        final MVStore s = openStore(fileName);
        final MVMap<Integer, byte[]> map = s.openMap("test");
        final Random r = new Random();
        Task t = new Task() {
            @Override
            public void call() throws Exception {
                while (!stop) {
                    for (int i = 0; i < 10; i++) {
                        map.put(i, new byte[100 * r.nextInt(100)]);
                    }
                    s.commit();
                    map.clear();
                    s.commit();
                    long len = s.getFileStore().size();
                    if (len > 1024 * 1024) {
                        // slow down writing a lot
                        Thread.sleep(200);
                    } else if (len > 20 * 1024) {
                        // slow down writing
                        Thread.sleep(20);
                    }
                }
            }
        };
        t.execute();
        for (int i = 0; i < 10; i++) {
            // System.out.println("test " + i);
            s.setReuseSpace(false);
            byte[] buff = readFileSlowly(s.getFileStore().getFile(), s.getFileStore().size());
            s.setReuseSpace(true);
            FileOutputStream out = new FileOutputStream(fileNameRestore);
            out.write(buff);
            MVStore s2 = openStore(fileNameRestore);
            MVMap<Integer, byte[]> test = s2.openMap("test");
            for (Integer k : test.keySet()) {
                test.get(k);
            }
            s2.close();
            // let it compact
            Thread.sleep(10);
        }
        t.get();
        s.close();
    }

    private static byte[] readFileSlowly(FileChannel file, long length) throws Exception {
        file.position(0);
        InputStream in = new BufferedInputStream(new FileChannelInputStream(file, false));
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        for (int j = 0; j < length; j++) {
            int x = in.read();
            if (x < 0) {
                break;
            }
            buff.write(x);
        }
        in.close();
        return buff.toByteArray();
    }

    private void testConcurrentIterate() {
        MVStore s = new MVStore.Builder().pageSplitSize(3).open();
        s.setVersionsToKeep(100);
        final MVMap<Integer, Integer> map = s.openMap("test");
        final int len = 10;
        final Random r = new Random();
        Task t = new Task() {
            @Override
            public void call() throws Exception {
                while (!stop) {
                    int x = r.nextInt(len);
                    if (r.nextBoolean()) {
                        map.remove(x);
                    } else {
                        map.put(x, r.nextInt(100));
                    }
                }
            }
        };
        t.execute();
        for (int k = 0; k < 10000; k++) {
            Iterator<Integer> it = map.keyIterator(r.nextInt(len));
            long old = s.getCurrentVersion();
            s.commit();
            while (map.getVersion() == old) {
                Thread.yield();
            }
            while (it.hasNext()) {
                it.next();
            }
        }
        t.get();
        s.close();
    }


    /**
     * Test what happens on concurrent write. Concurrent write may corrupt the
     * map, so that keys and values may become null.
     */
    private void testConcurrentWrite() throws InterruptedException {
        final AtomicInteger detected = new AtomicInteger();
        final AtomicInteger notDetected = new AtomicInteger();
        for (int i = 0; i < 10; i++) {
            testConcurrentWrite(detected, notDetected);
        }
        // in most cases, it should be detected
        assertTrue(notDetected.get() * 10 <= detected.get());
    }

    private void testConcurrentWrite(final AtomicInteger detected,
            final AtomicInteger notDetected) throws InterruptedException {
        final MVStore s = openStore(null);
        final MVMap<Integer, Integer> m = s.openMap("data");
        final int size = 20;
        final Random rand = new Random(1);
        Task task = new Task() {
            @Override
            public void call() throws Exception {
                while (!stop) {
                    try {
                        if (rand.nextBoolean()) {
                            m.put(rand.nextInt(size), 1);
                        } else {
                            m.remove(rand.nextInt(size));
                        }
                        m.get(rand.nextInt(size));
                    } catch (ConcurrentModificationException e) {
                        detected.incrementAndGet();
                    } catch (NegativeArraySizeException e) {
                        notDetected.incrementAndGet();
                    } catch (ArrayIndexOutOfBoundsException e) {
                        notDetected.incrementAndGet();
                    } catch (IllegalArgumentException e) {
                        notDetected.incrementAndGet();
                    } catch (NullPointerException e) {
                        notDetected.incrementAndGet();
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
                } catch (ConcurrentModificationException e) {
                    detected.incrementAndGet();
                } catch (NegativeArraySizeException e) {
                    notDetected.incrementAndGet();
                } catch (ArrayIndexOutOfBoundsException e) {
                    notDetected.incrementAndGet();
                } catch (IllegalArgumentException e) {
                    notDetected.incrementAndGet();
                } catch (NullPointerException e) {
                    notDetected.incrementAndGet();
                }
            }
            s.commit();
            Thread.sleep(1);
        }
        task.get();
        s.close();
    }

    private void testConcurrentRead() throws InterruptedException {
        final MVStore s = openStore(null);
        final MVMap<Integer, Integer> m = s.openMap("data");
        final int size = 3;
        int x = (int) s.getCurrentVersion();
        for (int i = 0; i < size; i++) {
            m.put(i, x);
        }
        s.commit();
        Task task = new Task() {
            @Override
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
            s.commit();
            Thread.sleep(1);
        }
        task.get();
        s.close();
    }

}
