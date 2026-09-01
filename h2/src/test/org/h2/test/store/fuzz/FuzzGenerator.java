/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store.fuzz;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

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

/**
 * Derives a complete {@link FuzzScript} from a single seed. All random
 * decisions (store config, operation weights, keys, values) are made here so
 * the test class stays free of RNG logic.
 */
public final class FuzzGenerator {

    static final int KEY_RANGE = 3000;

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
    static final int OP_COUNT = 14;

    static final String[] OP_NAMES = {
            "put", "putBig", "remove", "rangePut", "rangeRemove", "clear",
            "commit", "rollback", "compact", "compactFile", "reopen",
            "openCursor", "advanceCursor", "auxChurn"
    };

    private final Random r;
    private final FuzzConfig config;

    public FuzzGenerator(long seed) {
        this.r = new Random(seed);
        this.config = FuzzConfig.fromRandom(r);
    }

    public FuzzConfig getConfig() {
        return config;
    }

    /** Generate a script with {@code opsPerRun} operations. */
    public FuzzScript generate(int opsPerRun) {
        List<FuzzOperation> ops = new ArrayList<>(opsPerRun);
        int keysPerPage = config.keysPerPage;

        for (int i = 0; i < opsPerRun; i++) {
            int picked = pickOp();
            int k = r.nextInt(KEY_RANGE);

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
        return new FuzzScript(config, ops);
    }

    /** Human-readable op name for the most recently picked op index. */
    public static String opName(int opId) {
        return OP_NAMES[opId];
    }

    private int pickOp() {
        int picked;
        do {
            picked = r.nextInt(OP_COUNT);
        } while (config.autoCommit && picked == OP_ROLLBACK);
        return picked;
    }

    static String value(int k, int opIndex, int length) {
        StringBuilder b = new StringBuilder(length + 20);
        b.append(k).append('_').append(opIndex).append('_');
        while (b.length() < length) {
            b.append('v');
        }
        return b.toString();
    }
}
