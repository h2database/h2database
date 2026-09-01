/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store.fuzz;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.h2.test.store.fuzz.FuzzOperations.AdvanceCursor;
import org.h2.test.store.fuzz.FuzzOperations.AuxChurn;
import org.h2.test.store.fuzz.FuzzOperations.Clear;
import org.h2.test.store.fuzz.FuzzOperations.Commit;
import org.h2.test.store.fuzz.FuzzOperations.Compact;
import org.h2.test.store.fuzz.FuzzOperations.CompactFile;
import org.h2.test.store.fuzz.FuzzOperations.OpenCursor;
import org.h2.test.store.fuzz.FuzzOperations.Put;
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
    private static final int OP_REMOVE = 1;
    private static final int OP_CLEAR = 2;
    private static final int OP_COMMIT = 3;
    private static final int OP_ROLLBACK = 4;
    private static final int OP_COMPACT = 5;
    private static final int OP_COMPACT_FILE = 6;
    private static final int OP_REOPEN = 7;
    private static final int OP_AUX_CHURN = 8;
    private static final int OP_OPEN_CURSOR = 9; // disabled
    private static final int OP_ADVANCE_CURSOR = 10; // disabled
    static final int OP_COUNT = OP_AUX_CHURN + 1;

    private final Random r;
    private final FuzzConfig config;

    public FuzzGenerator(long seed) {
        this.r = new Random(seed);
        this.config = rollConfig();
    }

    private FuzzConfig rollConfig() {
        boolean autoCommit = r.nextBoolean();
        int keysPerPage = 4 + r.nextInt(28);
        int autoCommitBufferKb = r.nextBoolean() ? 4 : 1024;
        int versionsToKeep = new int[]{0, 0, 5, 20}[r.nextInt(4)];
        int retentionTime = new int[]{0, 0, 1000, 45000}[r.nextInt(4)];
        return new FuzzConfig(autoCommit, keysPerPage, autoCommitBufferKb,
                versionsToKeep, retentionTime);
    }

    public FuzzConfig getConfig() {
        return config;
    }

    /** Generate a script with {@code opsPerRun} operations. */
    public FuzzScript generate(int opsPerRun) {
        List<FuzzOperation> ops = new ArrayList<>(opsPerRun);
        boolean committed = false;

        for (int i = 0; i < opsPerRun; i++) {
            int picked = pickOp(committed);
            int k = r.nextInt(KEY_RANGE);

            switch (picked) {
            case OP_PUT: {
                int len = r.nextBoolean() ? 10 + r.nextInt(40) : 1024 + r.nextInt(8192);
                ops.add(new Put(k, len));
                break;
            }
            case OP_REMOVE:
                ops.add(new Remove(k));
                break;
            case OP_CLEAR:
                ops.add(new Clear());
                break;
            case OP_COMMIT:
                ops.add(new Commit());
                committed = true;
                break;
            case OP_ROLLBACK:
                ops.add(new Rollback());
                break;
            case OP_COMPACT:
                ops.add(new Compact(r.nextInt(100), 1 << r.nextInt(20)));
                break;
            case OP_COMPACT_FILE:
                ops.add(new CompactFile());
                break;
            case OP_REOPEN:
                ops.add(new Reopen());
                break;
            case OP_OPEN_CURSOR: {
                Integer from = r.nextBoolean() ? null : k;
                ops.add(new OpenCursor(from, i));
                break;
            }
            case OP_ADVANCE_CURSOR: {
                String which = r.nextBoolean() ? "first" : "last";
                int steps = 1 + r.nextInt(100);
                ops.add(new AdvanceCursor(which, steps));
                break;
            }
            case OP_AUX_CHURN: {
                int writes = r.nextInt(100);
                int deletes = r.nextInt(100);
                int len = r.nextInt(1024);
                boolean remove = r.nextBoolean();
                ops.add(new AuxChurn(writes, deletes, len, remove));
                break;
            }
            default:
                throw new IllegalStateException("unknown op: " + picked);
            }
        }
        return new FuzzScript(config, ops);
    }

    private int pickOp(boolean committed) {
        int picked;
        do {
            picked = r.nextInt(OP_COUNT);
        } while ((config.autoCommit && (picked == OP_ROLLBACK || picked == OP_COMMIT))
                || (!committed && picked == OP_ROLLBACK));
        return picked;
    }

}
