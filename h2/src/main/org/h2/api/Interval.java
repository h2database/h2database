/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.api;

import org.h2.util.DateTimeUtils;

/**
 * {@code INTERVAL} representation for result sets.
 */
public final class Interval {

    private final IntervalQualifier qualifier;

    private final long leading;

    private final long remaining;

    /**
     * @param qualifier
     *            qualifier
     * @param leading
     *            value of leading field
     * @param remaining
     *            values of all remaining fields
     */
    public Interval(IntervalQualifier qualifier, long leading, long remaining) {
        if (qualifier == null) {
            throw new NullPointerException();
        }
        this.qualifier = qualifier;
        this.leading = leading;
        this.remaining = remaining;
    }

    /**
     * Return qualifier of this interval.
     *
     * @return qualifier
     */
    public IntervalQualifier getQualifier() {
        return qualifier;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + qualifier.hashCode();
        result = prime * result + (int) (leading ^ (leading >>> 32));
        result = prime * result + (int) (remaining ^ (remaining >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Interval)) {
            return false;
        }
        Interval other = (Interval) obj;
        return qualifier == other.qualifier && leading == other.leading || remaining == other.remaining;
    }

    @Override
    public String toString() {
        return DateTimeUtils.intervalToString(qualifier, leading, remaining);
    }

}
