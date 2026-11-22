/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store.fuzz;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.store.fs.FileUtils;
import org.h2.test.store.fuzz.FuzzRunContext.OpenCursor;

/**
 * A complete fuzz run: store config plus an ordered list of operations.
 * Can be saved to and loaded from a plain-text trace file for deterministic
 * replay independent of the random number generator.
 */
public final class FuzzScript {

    private final FuzzConfig config;
    private final List<FuzzOperation> operations;

    public FuzzScript(FuzzConfig config, List<FuzzOperation> operations) {
        this.config = config;
        this.operations = Collections.unmodifiableList(new ArrayList<>(operations));
    }

    public FuzzConfig getConfig() {
        return config;
    }

    public List<FuzzOperation> getOperations() {
        return operations;
    }

    public void save(Path file) throws IOException {
        Files.createDirectories(file.getParent());
        try (BufferedWriter w = Files.newBufferedWriter(file)) {
            w.write(config.toLine());
            w.newLine();
            for (FuzzOperation op : operations) {
                w.write(op.toLine());
                w.newLine();
            }
        }
    }

    public static FuzzScript load(Path file) throws IOException {
        try (BufferedReader r = Files.newBufferedReader(file)) {
            String configLine = r.readLine();
            if (configLine == null) {
                throw new IOException("Empty trace file: " + file);
            }
            FuzzConfig config = FuzzConfig.fromLine(configLine);
            List<FuzzOperation> ops = new ArrayList<>();
            String line;
            while ((line = r.readLine()) != null) {
                if (!line.isEmpty()) {
                    ops.add(FuzzOperations.fromLine(line));
                }
            }
            return new FuzzScript(config, ops);
        }
    }

    /** Thrown when replay fails; carries the index of the failing operation. */
    public static final class ReplayFailure extends Exception {
        public final int opIndex;

        ReplayFailure(int opIndex, Throwable cause) {
            super("op " + opIndex + ": " + cause.getMessage(), cause);
            this.opIndex = opIndex;
        }
    }

    /**
     * Execute all operations against a fresh store at {@code fileName},
     * then drain all open cursors and do a final full verify.
     *
     * @throws ReplayFailure wrapping the real cause with the failing op index
     */
    public void replay(String fileName) throws ReplayFailure {
        FileUtils.delete(fileName);
        MVStore store = config.openStore(fileName);
        MVMap<Integer, String> map = store.openMap("data");
        FuzzRunContext ctx = new FuzzRunContext(store, map, fileName, config);
        int i = 0;
        try {
            for (; i < operations.size(); i++) {
                ctx.opIndex = i;
                operations.get(i).execute(ctx);
                if (i % 200 == 199) {
                    ctx.fullVerify();
                }
            }
            i = operations.size(); // mark: failure during drain/verify counts as last op
            while (!ctx.cursors.isEmpty()) {
                ctx.drainCursor(ctx.cursors.removeFirst());
            }
            ctx.fullVerify();
        } catch (Throwable t) {
            throw new ReplayFailure(Math.min(i, operations.size() - 1), t);
        } finally {
            ctx.store.close();
            FileUtils.delete(fileName);
        }
    }

    /** Return a new script containing only operations [0, toIndex] (inclusive). */
    public FuzzScript truncateTo(int toIndex) {
        return new FuzzScript(config, operations.subList(0, toIndex + 1));
    }
}
