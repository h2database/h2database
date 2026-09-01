/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.util.Iterator;
import java.util.Map;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;

/**
 * Test that an open cursor does not fail with "Chunk not found" while the
 * store is compacted during iteration.
 */
public class TestMVStoreChunkNotFound extends TestBase {

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
        String fileName = getBaseDir() + "/testChunkNotFound.h3";
        FileUtils.createDirectories(getBaseDir());
        FileUtils.delete(fileName);
        try {
            int items = 4_000;
            String payload = "x".repeat(50);
            println("writing " + items + " entries");
            try (MVStore store = openStore(fileName)) {
                MVMap<String, String> map = store.openMap("data");
                for (int i = 0; i < items; i++) {
                    map.put(Integer.toString(i), payload + i);
                    if (i % 1000 == 0) {
                        store.commit();
                    }
                }
                // overwrite every other key so every chunk is about half dead
                for (int i = 0; i < items; i += 2) {
                    map.put(Integer.toString(i), i + payload);
                    if (i % 1000 == 0) {
                        store.commit();
                    }
                }
                store.commit();
                println("wrote entries, fill rate: " + store.getFileStore().getChunksFillRate() + "%");
            }
            // reopen so iteration has to read pages from disk
            println("reopening store and iterating");
            try (MVStore store = openStore(fileName)) {
                MVMap<String, String> map = store.openMap("data");
                MVMap<String, String> side = store.openMap("side");
                int iterated = 0;
                Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
                while (it.hasNext()) {
                    it.next();
                    iterated++;
                    if (iterated % 1000 == 0) {
                        // advance the store version and compact
                        // while the cursor is open
                        side.put("tick", Integer.toString(iterated));
                        store.commit();
                        boolean compacted = store.compact(95, 16 * 1024 * 1024);
                        println("iterated=" + iterated + " compacted=" + compacted + " fill=" + store.getFileStore().getChunksFillRate() + "%");
                    }
                }
                println("done, iterated=" + iterated);
                assertEquals(items, iterated);
            }
        } finally {
            FileUtils.delete(fileName);
        }
    }

    private static MVStore openStore(String fileName) {
        MVStore store = new MVStore.Builder()
                .fileName(fileName)
                .open();
        store.setVersionsToKeep(0);
        store.setRetentionTime(0);
        return store;
    }
}

