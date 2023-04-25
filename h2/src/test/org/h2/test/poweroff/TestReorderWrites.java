/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
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
import org.h2.mvstore.MVStoreException;
import org.h2.mvstore.MVStoreTool;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.test.utils.FilePathReorderWrites;

/**
 * Tests that the MVStore recovers from a power failure if the file system or
 * disk re-ordered the write operations.
 */
public class TestReorderWrites extends TestBase {

    private static final boolean LOG = false;

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
        testMVStore(false);
        testMVStore(true);
        testFileSystem(false);
        testFileSystem(true);
    }

    private void testMVStore(final boolean partialWrite) {
        // Add partial write test
        // @since 2019-07-31 little-pan
        println(String.format("testMVStore(): %s partial write", partialWrite? "Enable": "Disable"));
        FilePathReorderWrites.setPartialWrites(partialWrite);

        FilePathReorderWrites fs = FilePathReorderWrites.register();
        String fileName = "reorder:memFS:test.mv";
        try {
            for (int i = 0; i < (config.big ? 1000 : 100); i++) {
                log(i + " --------------------------------");
                // this test is not interested in power off failures during
                // initial creation
                fs.setPowerOffCountdown(0, 0);
                // release the static data this test generates
                FileUtils.delete("memFS:test.mv");
                FileUtils.delete("memFS:test.mv.copy");
                MVStore store = new MVStore.Builder().
                        fileName(fileName).
                        autoCommitDisabled().open();
                // store.setRetentionTime(10);
                Map<Integer, byte[]> map = store.openMap("data");
                map.put(-1, new byte[1]);
                store.commit();
                store.getFileStore().sync();
                Random r = new Random(i);
                int stop = 4 + r.nextInt(config.big ? 150 : 20);
                log("countdown start");
                fs.setPowerOffCountdown(stop, i);
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
                        log("op " + j + ": ");
                        store.commit();
                        switch (r.nextInt(10)) {
                        case 0:
                            log("op compact");
                            store.compact(100, 10 * 1024);
                            break;
                        case 1:
                        default:
                            log("op compactMoveChunks");
                            store.compactFile(1000);
                            log("op compactFile done");
                            break;
                        }
                    }
                    // write has to fail at some point
                    fail();
                } catch (MVStoreException e) {
                    log("stop " + e + ", cause: " + e.getCause());
                    // expected
                }
                try {
                    store.close();
                } catch (MVStoreException e) {
                    // expected
                    store.closeImmediately();
                }
                log("verify");
                fs.setPowerOffCountdown(100, 0);
                if (LOG) {
                    MVStoreTool.dump(fileName, true);
                }
                store = new MVStore.Builder().
                        fileName(fileName).
                        autoCommitDisabled().open();
                map = store.openMap("data");
                if (!map.containsKey(-1)) {
                    fail("key not found, size=" + map.size() + " i=" + i);
                } else {
                    assertEquals("i=" + i, 1, map.get(-1).length);
                }
                for (int j = 0; j < 100; j++) {
                    Map<Integer, Integer> newMap = store.openMap("d" + j);
                    newMap.get(j);
                }
                map.keySet();
                store.close();
            }
        } finally {
            // release the static data this test generates
            FileUtils.delete("memFS:test.mv");
            FileUtils.delete("memFS:test.mv.copy");
        }
    }

    private static void log(String message) {
        if (LOG) {
            System.out.println(message);
        }
    }

    private void testFileSystem(final boolean partialWrite) throws IOException {
        FilePathReorderWrites fs = FilePathReorderWrites.register();
        // *disable this for now, still bug(s) in our code*
        // Add partial write enable test
        // @since 2019-07-31 little-pan
        FilePathReorderWrites.setPartialWrites(partialWrite);
        println(String.format("testFileSystem(): %s partial write", partialWrite? "Enable": "Disable"));

        String fileName = "reorder:memFS:test";
        final ByteBuffer empty = ByteBuffer.allocate(1024);
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
        // release the static data this test generates
        FileUtils.delete(fileName);
    }

}
