/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.sql.SQLException;
import java.util.List;

/**
 * Result of a batch execution.
 */
public class BatchResult {

    private final long[] updateCounts;

    private final ResultInterface generatedKeys;

    private final List<SQLException> exceptions;

    public BatchResult(long[] updateCounts, ResultInterface generatedKeys, List<SQLException> exceptions) {
        this.updateCounts = updateCounts;
        this.generatedKeys = generatedKeys;
        this.exceptions = exceptions;
    }

    public long[] getUpdateCounts() {
        return updateCounts;
    }

    public ResultInterface getGeneratedKeys() {
        return generatedKeys;
    }

    public List<SQLException> getExceptions() {
        return exceptions;
    }

}
