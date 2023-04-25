/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

/**
 * A versioned value (possibly null).
 * It contains current value and latest committed value if current one is uncommitted.
 * Also for uncommitted values it contains operationId - a combination of
 * transactionId and logId.
 */
public class VersionedValue<T> {

    protected VersionedValue() {}

    public boolean isCommitted() {
        return true;
    }

    public long getOperationId() {
        return 0L;
    }

    @SuppressWarnings("unchecked")
    public T getCurrentValue() {
        return (T)this;
    }

    @SuppressWarnings("unchecked")
    public T getCommittedValue() {
        return (T)this;
    }

}
