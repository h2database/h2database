/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store.fuzz;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.test.store.fuzz.FuzzOperations.AdvanceCursor;
import org.h2.test.store.fuzz.FuzzOperations.AuxChurn;
import org.h2.test.store.fuzz.FuzzOperations.Clear;
import org.h2.test.store.fuzz.FuzzOperations.Commit;
import org.h2.test.store.fuzz.FuzzOperations.Compact;
import org.h2.test.store.fuzz.FuzzOperations.CompactFile;
import org.h2.test.store.fuzz.FuzzOperations.OpenCursorOp;
import org.h2.test.store.fuzz.FuzzOperations.Put;
import org.h2.test.store.fuzz.FuzzOperations.PutBig;
import org.h2.test.store.fuzz.FuzzOperations.RangePut;
import org.h2.test.store.fuzz.FuzzOperations.RangeRemove;
import org.h2.test.store.fuzz.FuzzOperations.Remove;
import org.h2.test.store.fuzz.FuzzOperations.Reopen;
import org.h2.test.store.fuzz.FuzzOperations.Rollback;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/**
 * Seed-driven fuzz test for the MVStore.
 * <p>
 * Each run derives everything (store configuration, operation mix, keys,
 * values) from a single seed. The operation sequence is saved to a trace file
 * under {@code target/fuzz-traces/}; if the run passes the file is deleted,
 * if it fails the file is kept for inspection and can be checked in to
 * {@code src/test/resources/org/h2/test/store/fuzz/} so it is replayed
 * automatically by {@link #replayTests()}.
 */
public class TestMVStoreFuzz extends TestBase {

    /**
     * Master seed for deriving per-run seeds. Change to explore a different
     * set of runs.
     */
    private static final long MASTER_SEED = 0;

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
        for (DynamicTest dt : (Iterable<DynamicTest>) fuzzTests()::iterator) {
            try {
                dt.getExecutable().execute();
            } catch (Exception | Error e) {
                throw e;
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        }
        for (DynamicTest dt : (Iterable<DynamicTest>) replayTests()::iterator) {
            try {
                dt.getExecutable().execute();
            } catch (Exception | Error e) {
                throw e;
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        }
    }

    @TestFactory
    Stream<DynamicTest> fuzzTests() throws Exception {
        if (config == null) {
            init();
        }
        String fileName = "memFS:" + getTestName();
        int opsPerRun = getSize(1000, 5000);
        Random seedRng = new Random(MASTER_SEED);
        Stream<Long> seedStream = Stream.generate(seedRng::nextLong).limit(RUNS);
        return seedStream.map(seed -> DynamicTest.dynamicTest("seed=" + seed,
                () -> runSeed(fileName, opsPerRun, seed)));
    }

    /**
     * Replay all trace files checked in under
     * {@code src/test/resources/org/h2/test/store/fuzz/}.
     */
    @TestFactory
    Stream<DynamicTest> replayTests() throws Exception {
        if (config == null) {
            init();
        }
        String fileName = "memFS:" + getTestName() + "_replay";
        List<DynamicTest> tests = new ArrayList<>();
        URL resourceDir = getClass().getResource("/org/h2/test/store/fuzz/");
        if (resourceDir != null) {
            Path dir = Paths.get(resourceDir.toURI());
            if (Files.isDirectory(dir)) {
                Files.list(dir)
                        .filter(p -> p.toString().endsWith(".txt"))
                        .sorted()
                        .forEach(p -> tests.add(DynamicTest.dynamicTest(
                                "replay:" + p.getFileName(),
                                () -> {
                                    FuzzScript script = FuzzScript.load(p);
                                    script.replay(fileName);
                                })));
            }
        }
        return tests.stream();
    }

    private void runSeed(String fileName, int opsPerRun, long seed) throws Exception {
        Path traceFile = traceFilePath(seed);
        Random r = new Random(seed);
        FuzzConfig config = FuzzConfig.fromRandom(r);
        int[] weights = rollWeights(r, config.autoCommit);
        int weightSum = 0;
        for (int w : weights) {
            weightSum += w;
        }
        List<FuzzOperation> ops = generateOps(r, config, weights, weightSum, opsPerRun);
        FuzzScript script = new FuzzScript(config, ops);
        script.save(traceFile);
        try {
            script.replay(fileName);
            Files.deleteIfExists(traceFile);
        } catch (FuzzScript.ReplayFailure ex) {
            // truncate to just the failing op so checked-in files stay minimal
            FuzzScript truncated = script.truncateTo(ex.opIndex);
            truncated.save(traceFile);
            // replay the truncated script once more to confirm the failure is reproducible
            try {
                truncated.replay(fileName);
                // second run passed — intermittent failure, discard the file
                Files.deleteIfExists(traceFile);
                println("intermittent failure seed=" + seed + " op=" + ex.opIndex
                        + " (not reproducible, file deleted)");
            } catch (FuzzScript.ReplayFailure confirmed) {
                println("confirmed failure seed=" + seed + " op=" + ex.opIndex
                        + " trace=" + traceFile);
            }
            throw ex;
        }
    }

    private List<FuzzOperation> generateOps(Random r, FuzzConfig config,
            int[] weights, int weightSum, int opsPerRun) {
        List<FuzzOperation> ops = new ArrayList<>(opsPerRun);
        int keysPerPage = config.keysPerPage;
        boolean autoCommit = config.autoCommit;

        for (int i = 0; i < opsPerRun; i++) {
            int picked = pickOp(r, weights, weightSum);
            int k = r.nextInt(KEY_RANGE);
            println("op " + i + ": " + OP_NAMES[picked] + " k=" + k);

            switch (picked) {
            case OP_PUT:
                ops.add(new Put(k, value(k, i, 10 + r.nextInt(40))));
                break;
            case OP_PUT_BIG:
                ops.add(new PutBig(k, value(k, i, 1024 + r.nextInt(8192))));
                break;
            case OP_REMOVE:
                ops.add(new Remove(k));
                break;
            case OP_RANGE_PUT: {
                int n = 1 + r.nextInt(2 * keysPerPage);
                int step = r.nextBoolean() ? 1 : -1;
                Map<Integer, String> entries = new LinkedHashMap<>();
                int ki = k;
                for (int j = 0; j < n; j++, ki += step) {
                    entries.put(ki, value(ki, i, 10 + r.nextInt(40)));
                }
                ops.add(new RangePut(step, entries));
                break;
            }
            case OP_RANGE_REMOVE: {
                int n = 1 + r.nextInt(2 * keysPerPage);
                int step = r.nextBoolean() ? 1 : -1;
                List<Integer> keys = new ArrayList<>();
                int ki = k;
                for (int j = 0; j < n; j++, ki += step) {
                    keys.add(ki);
                }
                ops.add(new RangeRemove(step, keys));
                break;
            }
            case OP_CLEAR:
                ops.add(new Clear());
                break;
            case OP_COMMIT:
                ops.add(new Commit());
                break;
            case OP_ROLLBACK:
                ops.add(new Rollback());
                break;
            case OP_COMPACT:
                ops.add(new Compact(50 + r.nextInt(50), 1024 << r.nextInt(12)));
                break;
            case OP_COMPACT_FILE:
                ops.add(new CompactFile());
                break;
            case OP_REOPEN:
                ops.add(new Reopen());
                break;
            case OP_OPEN_CURSOR: {
                Integer from = r.nextBoolean() ? null : k;
                ops.add(new OpenCursorOp(from, i));
                break;
            }
            case OP_ADVANCE_CURSOR: {
                String which = r.nextBoolean() ? "first" : "last";
                int steps = 1 + r.nextInt(100);
                ops.add(new AdvanceCursor(which, steps));
                break;
            }
            case OP_AUX_CHURN: {
                int n = 1 + r.nextInt(50);
                List<AuxChurn.Entry> entries = new ArrayList<>();
                for (int j = 0; j < n; j++) {
                    int ak = r.nextInt(KEY_RANGE);
                    if (r.nextBoolean()) {
                        byte[] junk = new byte[r.nextInt(512)];
                        r.nextBytes(junk);
                        entries.add(new AuxChurn.Entry(ak, junk));
                    } else {
                        entries.add(new AuxChurn.Entry(ak));
                    }
                }
                boolean removeMap = r.nextInt(20) == 0;
                ops.add(new AuxChurn(entries, removeMap));
                break;
            }
            default:
                throw new IllegalStateException("unknown op: " + picked);
            }
        }
        return ops;
    }

    private static int[] rollWeights(Random r, boolean autoCommit) {
        int[] weights = new int[OP_COUNT];
        for (int i = 0; i < OP_COUNT; i++) {
            weights[i] = r.nextInt(3) == 0 ? 0 : 1 + r.nextInt(8);
        }
        weights[OP_PUT] = Math.max(weights[OP_PUT], 4);
        weights[OP_CLEAR] = Math.min(weights[OP_CLEAR], 1);
        weights[OP_REOPEN] = Math.min(weights[OP_REOPEN], 1);
        weights[OP_COMPACT_FILE] = Math.min(weights[OP_COMPACT_FILE], 1);
        if (autoCommit) {
            weights[OP_ROLLBACK] = 0;
        }
        return weights;
    }

    private static int pickOp(Random r, int[] weights, int weightSum) {
        int x = r.nextInt(weightSum);
        for (int i = 0;; i++) {
            x -= weights[i];
            if (x < 0) {
                return i;
            }
        }
    }

    private static String value(int k, int opIndex, int length) {
        StringBuilder b = new StringBuilder(length + 20);
        b.append(k).append('_').append(opIndex).append('_');
        while (b.length() < length) {
            b.append('v');
        }
        return b.toString();
    }

    private static Path traceFilePath(long seed) throws IOException {
        Path dir = Paths.get("src", "test", "resources", "org", "h2", "test", "store", "fuzz");
        Files.createDirectories(dir);
        return dir.resolve("seed-" + seed + ".txt");
    }
}
