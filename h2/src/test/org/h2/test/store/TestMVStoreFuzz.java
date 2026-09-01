/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.TreeMap;

import org.h2.mvstore.Cursor;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;

/**
 * Seed-driven fuzz test for the MVStore.
 * <p>
 * Each run derives everything (store configuration, operation mix, keys,
 * values) from a single seed, so any failure can be reproduced by pinning
 * {@link #PINNED_SEED}. The operation weights are re-rolled per seed
 * ("swarm testing"): some operations get weight zero in a given run, which
 * explores different interleavings of store features than a uniform mix
 * would.
 * <p>
 * A {@link TreeMap} shadow acts as the model oracle. Long-lived cursors are
 * kept open across commit/compact/reopen-free mutations and advanced lazily,
 * to catch lifecycle bugs such as "Chunk not found" where a chunk still
 * referenced by an open cursor is freed and overwritten.
 */
public class TestMVStoreFuzz extends TestBase {

    /**
     * Master seed for deriving per-run seeds. Change to explore a different
     * set of runs.
     */
    private static final long MASTER_SEED = 0;

    /**
     * Set to a non-null value to reproduce a single failing run.
     */
    private static final Long PINNED_SEED = null;

    /**
     * Print every operation; useful when reproducing a failure.
     */
    private static final boolean LOG = false;

    private static final int RUNS = 10;

    private static final int KEY_RANGE = 3000;

    // operation ids
    private static final int OP_PUT = 0;
    private static final int OP_PUT_BIG = 1;
    private static final int OP_REMOVE = 2;
    private static final int OP_RANGE_PUT = 3;
    private static final int OP_RANGE_REMOVE = 4;
    private static final int OP_CLEAR = 5;
    private static final int OP_COMMIT = 6;
    private static final int OP_ROLLBACK = 7;
    private static final int OP_COMPACT = 8;
    private static final int OP_COMPACT_FILE = 9;
    private static final int OP_REOPEN = 10;
    private static final int OP_OPEN_CURSOR = 11;
    private static final int OP_ADVANCE_CURSOR = 12;
    private static final int OP_AUX_CHURN = 13;
    private static final int OP_COUNT = 14;

    private static final String[] OP_NAMES = { "put", "putBig", "remove",
            "rangePut", "rangeRemove", "clear", "commit", "rollback",
            "compact", "compactFile", "reopen", "openCursor", "advanceCursor",
            "auxChurn" };

    private Random r;
    private int op;

    // per-run swarm configuration
    private int[] weights;
    private int weightSum;
    private boolean autoCommit;
    private int keysPerPage;
    private int autoCommitBufferKb;
    private int versionsToKeep;
    private int retentionTime;

    /**
     * A cursor kept open across other operations, together with the entries
     * it is expected to produce (snapshot of the shadow map at creation).
     */
    private static class OpenCursor {
        final Cursor<Integer, String> cursor;
        final Iterator<Map.Entry<Integer, String>> expected;
        final int createdAtOp;

        OpenCursor(Cursor<Integer, String> cursor,
                Iterator<Map.Entry<Integer, String>> expected, int createdAtOp) {
            this.cursor = cursor;
            this.expected = expected;
            this.createdAtOp = createdAtOp;
        }
    }

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
        String fileName = "memFS:" + getTestName();
        int opsPerRun = getSize(1000, 5000);
        Random seeds = new Random(MASTER_SEED);
        for (int run = 0; run < RUNS; run++) {
            long seed = PINNED_SEED != null ? PINNED_SEED : seeds.nextLong();
            try {
                fuzz(fileName, opsPerRun, seed);
            } catch (Throwable ex) {
                println("failed with seed:" + seed + " op:" + op + " ("
                        + OP_NAMES[pickedOp] + ") config:" + configString());
                throw ex;
            } finally {
                FileUtils.delete(fileName);
            }
            if (PINNED_SEED != null) {
                break;
            }
        }
    }

    private int pickedOp;

    private void fuzz(String fileName, int opsPerRun, long seed) {
        r = new Random(seed);
        rollConfig();
        MVStore s = openStore(fileName);
        MVMap<Integer, String> m = s.openMap("data");
        TreeMap<Integer, String> shadow = new TreeMap<>();
        TreeMap<Integer, String> committedShadow = new TreeMap<>();
        Deque<OpenCursor> cursors = new ArrayDeque<>();
        for (op = 0; op < opsPerRun; op++) {
            pickedOp = pickOp();
            int k = r.nextInt(KEY_RANGE);
            if (LOG) {
                println("op " + op + ": " + OP_NAMES[pickedOp] + " k=" + k);
            }
            switch (pickedOp) {
            case OP_PUT: {
                String v = value(k, 10 + r.nextInt(40));
                m.put(k, v);
                shadow.put(k, v);
                break;
            }
            case OP_PUT_BIG: {
                // large values force overflow / multi-page storage
                String v = value(k, 1024 + r.nextInt(8192));
                m.put(k, v);
                shadow.put(k, v);
                break;
            }
            case OP_REMOVE:
                m.remove(k);
                shadow.remove(k);
                break;
            case OP_RANGE_PUT: {
                int n = 1 + r.nextInt(2 * keysPerPage);
                int step = r.nextBoolean() ? 1 : -1;
                for (int i = 0; i < n; i++, k += step) {
                    String v = value(k, 10 + r.nextInt(40));
                    m.put(k, v);
                    shadow.put(k, v);
                }
                break;
            }
            case OP_RANGE_REMOVE: {
                int n = 1 + r.nextInt(2 * keysPerPage);
                int step = r.nextBoolean() ? 1 : -1;
                for (int i = 0; i < n; i++, k += step) {
                    m.remove(k);
                    shadow.remove(k);
                }
                break;
            }
            case OP_CLEAR:
                m.clear();
                shadow.clear();
                break;
            case OP_COMMIT:
                s.commit();
                committedShadow = new TreeMap<>(shadow);
                break;
            case OP_ROLLBACK:
                // only reachable with auto-commit disabled, so the last
                // committed version is exactly what we tracked
                s.rollback();
                shadow = new TreeMap<>(committedShadow);
                // pages written for the discarded version may be freed;
                // cursors over them are not expected to survive
                cursors.clear();
                break;
            case OP_COMPACT:
                s.compact(50 + r.nextInt(50), 1024 << r.nextInt(12));
                break;
            case OP_COMPACT_FILE:
                s.commit();
                committedShadow = new TreeMap<>(shadow);
                s.compactFile(200);
                break;
            case OP_REOPEN:
                cursors.clear();
                s.commit();
                committedShadow = new TreeMap<>(shadow);
                s.close();
                s = openStore(fileName);
                m = s.openMap("data");
                break;
            case OP_OPEN_CURSOR: {
                if (cursors.size() >= 8) {
                    cursors.removeFirst();
                }
                Integer from = r.nextBoolean() ? null : k;
                Cursor<Integer, String> c = m.cursor(from);
                ArrayList<Map.Entry<Integer, String>> snapshot = new ArrayList<>(
                        (from == null ? shadow : shadow.tailMap(from)).entrySet());
                cursors.addLast(new OpenCursor(c, snapshot.iterator(), op));
                break;
            }
            case OP_ADVANCE_CURSOR:
                advanceCursor(cursors);
                break;
            case OP_AUX_CHURN: {
                // churn a second map so chunks contain a mix of live and
                // dead pages from different maps
                MVMap<Integer, byte[]> aux = s.openMap("aux");
                int n = 1 + r.nextInt(50);
                for (int i = 0; i < n; i++) {
                    int ak = r.nextInt(KEY_RANGE);
                    if (r.nextBoolean()) {
                        byte[] junk = new byte[r.nextInt(512)];
                        r.nextBytes(junk);
                        aux.put(ak, junk);
                    } else {
                        aux.remove(ak);
                    }
                }
                if (r.nextInt(20) == 0) {
                    s.removeMap(aux);
                }
                break;
            }
            default:
                throw new IllegalStateException("op " + pickedOp);
            }
            // spot checks after every operation
            assertEquals("get(" + k + ")", shadow.get(k), m.get(k));
            assertEquals("size", shadow.size(), m.size());
            if (op % 200 == 199) {
                fullVerify(shadow, m);
            }
        }
        // drain all cursors and do a final full check
        while (!cursors.isEmpty()) {
            drainCursor(cursors.removeFirst());
        }
        fullVerify(shadow, m);
        s.close();
    }

    private void rollConfig() {
        autoCommit = r.nextBoolean();
        keysPerPage = 4 + r.nextInt(28);
        autoCommitBufferKb = r.nextBoolean() ? 4 : 1024;
        versionsToKeep = new int[] { 0, 0, 5, 20 }[r.nextInt(4)];
        retentionTime = new int[] { 0, 0, 1000, 45000 }[r.nextInt(4)];
        // swarm testing: re-roll operation weights per seed, deliberately
        // dropping some operations entirely (feature omission)
        weights = new int[OP_COUNT];
        weightSum = 0;
        for (int i = 0; i < OP_COUNT; i++) {
            weights[i] = r.nextInt(3) == 0 ? 0 : 1 + r.nextInt(8);
        }
        // always mutate, otherwise the run is a no-op
        weights[OP_PUT] = Math.max(weights[OP_PUT], 4);
        // rare, destroys all accumulated state
        weights[OP_CLEAR] = Math.min(weights[OP_CLEAR], 1);
        weights[OP_REOPEN] = Math.min(weights[OP_REOPEN], 1);
        weights[OP_COMPACT_FILE] = Math.min(weights[OP_COMPACT_FILE], 1);
        if (autoCommit) {
            // with background commits the last committed version is not
            // under our control, so the rollback oracle would be wrong
            weights[OP_ROLLBACK] = 0;
        }
        for (int w : weights) {
            weightSum += w;
        }
    }

    private int pickOp() {
        int x = r.nextInt(weightSum);
        for (int i = 0;; i++) {
            x -= weights[i];
            if (x < 0) {
                return i;
            }
        }
    }

    private void advanceCursor(Deque<OpenCursor> cursors) {
        if (cursors.isEmpty()) {
            return;
        }
        OpenCursor oc = r.nextBoolean() ? cursors.peekFirst() : cursors.peekLast();
        int steps = 1 + r.nextInt(100);
        for (int i = 0; i < steps; i++) {
            if (!oc.expected.hasNext()) {
                assertFalse("cursor from op " + oc.createdAtOp + " not exhausted",
                        oc.cursor.hasNext());
                cursors.remove(oc);
                return;
            }
            Map.Entry<Integer, String> e = oc.expected.next();
            assertTrue("cursor from op " + oc.createdAtOp + " exhausted early, expected key " + e.getKey(),
                    oc.cursor.hasNext());
            Integer key = oc.cursor.next();
            assertEquals("cursor from op " + oc.createdAtOp + " key", e.getKey(), key);
            assertEquals("cursor from op " + oc.createdAtOp + " value for key " + key,
                    e.getValue(), oc.cursor.getValue());
        }
    }

    private void drainCursor(OpenCursor oc) {
        while (oc.expected.hasNext()) {
            Map.Entry<Integer, String> e = oc.expected.next();
            assertTrue("cursor from op " + oc.createdAtOp + " exhausted early, expected key " + e.getKey(),
                    oc.cursor.hasNext());
            Integer key = oc.cursor.next();
            assertEquals("cursor from op " + oc.createdAtOp + " key", e.getKey(), key);
            assertEquals("cursor from op " + oc.createdAtOp + " value for key " + key,
                    e.getValue(), oc.cursor.getValue());
        }
        assertFalse("cursor from op " + oc.createdAtOp + " not exhausted",
                oc.cursor.hasNext());
    }

    private void fullVerify(TreeMap<Integer, String> shadow, MVMap<Integer, String> m) {
        assertEquals("full size", shadow.size(), m.size());
        Iterator<Map.Entry<Integer, String>> expected = shadow.entrySet().iterator();
        Cursor<Integer, String> c = m.cursor(null);
        while (expected.hasNext()) {
            Map.Entry<Integer, String> e = expected.next();
            assertTrue("map exhausted early, expected key " + e.getKey(), c.hasNext());
            Integer key = c.next();
            assertEquals("full key", e.getKey(), key);
            assertEquals("full value for key " + key, e.getValue(), c.getValue());
        }
        assertFalse("map has extra entries", c.hasNext());
    }

    private String value(int k, int length) {
        StringBuilder b = new StringBuilder(length + 20);
        b.append(k).append('_').append(op).append('_');
        while (b.length() < length) {
            b.append('v');
        }
        return b.toString();
    }

    private MVStore openStore(String fileName) {
        MVStore.Builder builder = new MVStore.Builder()
                .fileName(fileName)
                .keysPerPage(keysPerPage)
                .cacheSize(1);
        if (autoCommit) {
            builder.autoCommitBufferSize(autoCommitBufferKb);
        } else {
            // note: autoCommitDisabled() alone only disables the background
            // thread; a non-zero buffer size still triggers implicit commits
            // from beforeWrite(), which would invalidate the rollback oracle
            builder.autoCommitDisabled().autoCommitBufferSize(0);
        }
        MVStore s = builder.open();
        s.setVersionsToKeep(versionsToKeep);
        s.setRetentionTime(retentionTime);
        return s;
    }

    private String configString() {
        return "autoCommit=" + autoCommit + " keysPerPage=" + keysPerPage
                + " autoCommitBufferKb=" + autoCommitBufferKb
                + " versionsToKeep=" + versionsToKeep
                + " retentionTime=" + retentionTime;
    }

    private void assertEquals(String message, Object expected, Object actual) {
        if (!Objects.equals(expected, actual)) {
            fail(message + " expected: " + expected + " actual: " + actual);
        }
    }

}
