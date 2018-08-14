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

    private final boolean negative;

    private final long leading;

    private final long remaining;

    /**
     * @param qualifier
     *            qualifier
     * @param negative
     *            whether interval is negative
     * @param leading
     *            value of leading field
     * @param remaining
     *            values of all remaining fields
     */
    public Interval(IntervalQualifier qualifier, boolean negative, long leading, long remaining) {
        if (qualifier == null) {
            throw new NullPointerException();
        }
        if (leading == 0L && remaining == 0L) {
            negative = false;
        } else if (leading < 0L || remaining < 0L) {
            throw new RuntimeException();
        }
        this.qualifier = qualifier;
        this.negative = negative;
        this.leading = leading;
        this.remaining = remaining;
    }

    /**
     * Returns qualifier of this interval.
     *
     * @return qualifier
     */
    public IntervalQualifier getQualifier() {
        return qualifier;
    }

    /**
     * Returns where the interval is negative.
     *
     * @return where the interval is negative
     */
    public boolean isNegative() {
        return negative;
    }

    /**
     * Returns value of leading field of this interval. For {@code SECOND}
     * intervals returns integer part of seconds.
     *
     * @return value of leading field
     */
    public long getLeading() {
        return leading;
    }

    /**
     * Returns combined value of remaining fields of this interval. For
     * {@code SECOND} intervals returns nanoseconds.
     *
     * @return combined value of remaining fields
     */
    public long getRemaining() {
        return remaining;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + qualifier.hashCode();
        result = prime * result + (negative ? 1231 : 1237);
        result = prime * result + (int) (leading ^ leading >>> 32);
        result = prime * result + (int) (remaining ^ remaining >>> 32);
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
        return qualifier == other.qualifier && negative == other.negative && leading == other.leading
                && remaining == other.remaining;
    }

    @Override
    public String toString() {
        return DateTimeUtils.intervalToString(qualifier, negative, leading, remaining);
    }

}
