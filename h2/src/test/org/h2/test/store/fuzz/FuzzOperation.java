/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store.fuzz;

/**
 * A single operation in a fuzz run. Each operation knows how to execute
 * against a live store (via {@link FuzzRunContext}) and how to serialize
 * itself to a line in a trace file.
 */
public interface FuzzOperation {

    /**
     * Execute this operation: mutate the store, update the shadow oracle in
     * ctx, and run spot-check assertions.
     */
    void execute(FuzzRunContext ctx);

    /** Serialize to a single line suitable for a trace file. */
    String toLine();
}
