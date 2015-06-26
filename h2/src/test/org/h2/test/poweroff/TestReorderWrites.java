/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.poweroff;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;

import org.h2.mvstore.MVStore;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.test.utils.FilePathReorderWrites;

/**
 * Tests that the MVStore recovers from a power failure if the file system or
 * disk re-ordered the write operations.
 */
public class TestReorderWrites  extends TestBase {

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
        testFileSystem();
        // testMVStore();
    }

    private void testMVStore() {
        FilePathReorderWrites fs = FilePathReorderWrites.register();
        String fileName = "reorder:memFS:test.mv";
        Random r = new Random(1);
        for (int i = 0; i < 100; i++) {
            System.out.println(i + " tst --------------------------------");
            fs.setPowerOffCountdown(100, i);
            FileUtils.delete(fileName);
            MVStore store = new MVStore.Builder().
                    fileName(fileName).
                    autoCommitDisabled().open();
            // store.setRetentionTime(10);
            Map<Integer, byte[]> map = store.openMap("data");
            map.put(0, new byte[1]);
            store.commit();
         //   if (r.nextBoolean()) {
                store.getFileStore().sync();
            //}
            fs.setPowerOffCountdown(4 + r.nextInt(20), i);
            try {
                for (int j = 1; j < 100; j++) {
                    Map<Integer, Integer> newMap = store.openMap("d" + j);
                    newMap.put(j, j * 10);
                    int key = r.nextInt(10);
                    int len = 10 * r.nextInt(1000);
                    if (r.nextBoolean()) {
                        map.remove(key);
                    } else {
                        map.put(key, new byte[len]);
                    }
                    store.commit();
                }
                // write has to fail at some point
                fail();
            } catch (IllegalStateException e) {
                // expected
            }
            store.close();
            System.out.println("-------------------------------- test");
            fs.setPowerOffCountdown(100, 0);
            System.out.println("file size: " + FileUtils.size(fileName));
            store = new MVStore.Builder().
                    fileName(fileName).
                    autoCommitDisabled().open();
            map = store.openMap("data");
            assertEquals(1, map.get(0).length);
            for (int j = 0; j < 100; j++) {
                Map<Integer, Integer> newMap = store.openMap("d" + j);
                newMap.get(j);
            }
            // map.keySet();
            store.close();
        }
    }

    private void testFileSystem() throws IOException {
        FilePathReorderWrites fs = FilePathReorderWrites.register();
        String fileName = "reorder:memFS:test";
        ByteBuffer empty = ByteBuffer.allocate(1024);
        Random r = new Random(1);
        long minSize = Long.MAX_VALUE;
        long maxSize = 0;
        int minWritten = Integer.MAX_VALUE;
        int maxWritten = 0;
        for (int i = 0; i < 100; i++) {
            fs.setPowerOffCountdown(100, i);
            FileUtils.delete(fileName);
            FileChannel fc = FilePath.get(fileName).open("rw");
            for (int j = 0; j < 20; j++) {
                fc.write(empty, j * 1024);
                empty.flip();
            }
            fs.setPowerOffCountdown(4 + r.nextInt(20), i);
            int lastWritten = 0;
            int lastTruncated = 0;
            for (int j = 20; j >= 0; j--) {
                try {
                    byte[] bytes = new byte[1024];
                    Arrays.fill(bytes, (byte) j);
                    ByteBuffer data = ByteBuffer.wrap(bytes);
                    fc.write(data, 0);
                    lastWritten = j;
                } catch (IOException e) {
                    // expected
                    break;
                }
                try {
                    fc.truncate(j * 1024);
                    lastTruncated = j * 1024;
                } catch (IOException e) {
                    // expected
                    break;
                }
            }
            if (lastTruncated <= 0 || lastWritten <= 0) {
                fail();
            }
            fs.setPowerOffCountdown(100, 0);
            fc = FilePath.get(fileName).open("rw");
            ByteBuffer data = ByteBuffer.allocate(1024);
            fc.read(data, 0);
            data.flip();
            int got = data.get();
            long size = fc.size();
            minSize = Math.min(minSize, size);
            maxSize = Math.max(minSize, size);
            minWritten = Math.min(minWritten, got);
            maxWritten = Math.max(maxWritten, got);
        }
        assertTrue(minSize < maxSize);
        assertTrue(minWritten < maxWritten);
    }

}
