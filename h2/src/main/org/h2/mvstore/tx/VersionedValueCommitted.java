/*
 * Copyright 2004-2026 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import org.h2.value.VersionedValue;

/**
 * Class CommittedVersionedValue.
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
class VersionedValueCommitted<T> extends VersionedValue<T>
{
    /**
     * The current value.
     */
    public final T value;

    /**
     * Transaction-scoped id for the entry, this value belongs to
     */
    private final long entryId;

    protected VersionedValueCommitted(T value, long entryId) {
        this.value = value;
        this.entryId = entryId;
    }

    /**
     * Either cast to VersionedValue, or wrap in VersionedValueCommitted
     *
     * @param <X> type of the value to get the VersionedValue for
     *
     * @param value the object to cast/wrap
     * @return VersionedValue instance
     */
    static <X> VersionedValue<X> getInstance(X value) {
        assert value != null;
        return getInstance(value, NO_ENTRY_ID);
    }

    @SuppressWarnings("unchecked")
    static <X> VersionedValue<X> getInstance(X value, long entryId) {
        return entryId == NO_ENTRY_ID && (value == null || value instanceof VersionedValue) ?
                (VersionedValue<X>)value :
                new VersionedValueCommitted<>(value, entryId);
    }

    @Override
    public long getEntryId() {
        return entryId;
    }

    @Override
    public final T getCurrentValue() {
        return value;
    }

    @Override
    public T getCommittedValue() {
        return value;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
