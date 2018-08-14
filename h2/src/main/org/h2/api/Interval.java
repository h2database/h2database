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
     * @param years
     *            years
     * @return interval
     */
    public static Interval ofYears(long years) {
        return new Interval(IntervalQualifier.YEAR, years < 0, Math.abs(years), 0);
    }

    /**
     * @param months
     *            months
     * @return interval
     */
    public static Interval ofMonths(long months) {
        return new Interval(IntervalQualifier.MONTH, months < 0, Math.abs(months), 0);
    }

    /**
     * @param days
     *            days
     * @return interval
     */
    public static Interval ofDays(long days) {
        return new Interval(IntervalQualifier.DAY, days < 0, Math.abs(days), 0);
    }

    /**
     * @param hours
     *            hours
     * @return interval
     */
    public static Interval ofHours(long hours) {
        return new Interval(IntervalQualifier.HOUR, hours < 0, Math.abs(hours), 0);
    }

    /**
     * @param minutes
     *            minutes
     * @return interval
     */
    public static Interval ofMinutes(long minutes) {
        return new Interval(IntervalQualifier.MINUTE, minutes < 0, Math.abs(minutes), 0);
    }

    /**
     * @param nanos
     *            nanoseconds (including seconds)
     * @return interval
     */
    public static Interval ofNanos(long nanos) {
        boolean negative = nanos < 0;
        if (negative) {
            nanos = -nanos;
            if (nanos < 0) {
                throw new IllegalArgumentException();
            }
        }
        return new Interval(IntervalQualifier.SECOND, negative, nanos / 1_000_000_000, nanos % 1_000_000_000);
    }

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

    /**
     * @return years, or 0
     */
    public long getYears() {
        return DateTimeUtils.yearsFromInterval(qualifier, negative, leading, remaining);
    }

    /**
     * @return months, or 0
     */
    public long getMonths() {
        return DateTimeUtils.monthsFromInterval(qualifier, negative, leading, remaining);
    }

    /**
     * @return days, or 0
     */
    public long getDays() {
        return DateTimeUtils.daysFromInterval(qualifier, negative, leading, remaining);
    }

    /**
     * @return hours, or 0
     */
    public long getHours() {
        return DateTimeUtils.hoursFromInterval(qualifier, negative, leading, remaining);
    }

    /**
     * @return minutes, or 0
     */
    public long getMinutes() {
        return DateTimeUtils.minutesFromInterval(qualifier, negative, leading, remaining);
    }

    /**
     * @return nanoseconds (including seconds), or 0
     */
    public long getNanos() {
        return DateTimeUtils.nanosFromInterval(qualifier, negative, leading, remaining);
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
