/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.api;

import static org.h2.util.DateTimeUtils.NANOS_PER_MINUTE;
import static org.h2.util.DateTimeUtils.NANOS_PER_SECOND;

import org.h2.message.DbException;
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
     * Creates a new INTERVAL YEAR.
     *
     * @param years
     *            years, |years|&lt;10<sup>18</sup>
     * @return INTERVAL YEAR
     */
    public static Interval ofYears(long years) {
        return new Interval(IntervalQualifier.YEAR, years < 0, Math.abs(years), 0);
    }

    /**
     * Creates a new INTERVAL MONTH.
     *
     * @param months
     *            months, |months|&lt;10<sup>18</sup>
     * @return INTERVAL MONTH
     */
    public static Interval ofMonths(long months) {
        return new Interval(IntervalQualifier.MONTH, months < 0, Math.abs(months), 0);
    }

    /**
     * Creates a new INTERVAL DAY.
     *
     * @param days
     *            days, |days|&lt;10<sup>18</sup>
     * @return INTERVAL DAY
     */
    public static Interval ofDays(long days) {
        return new Interval(IntervalQualifier.DAY, days < 0, Math.abs(days), 0);
    }

    /**
     * Creates a new INTERVAL HOUR.
     *
     * @param hours
     *            hours, |hours|&lt;10<sup>18</sup>
     * @return INTERVAL HOUR
     */
    public static Interval ofHours(long hours) {
        return new Interval(IntervalQualifier.HOUR, hours < 0, Math.abs(hours), 0);
    }

    /**
     * Creates a new INTERVAL MINUTE.
     *
     * @param minutes
     *            minutes, |minutes|&lt;10<sup>18</sup>
     * @return interval
     */
    public static Interval ofMinutes(long minutes) {
        return new Interval(IntervalQualifier.MINUTE, minutes < 0, Math.abs(minutes), 0);
    }

    /**
     * Creates a new INTERVAL SECOND.
     *
     * @param seconds
     *            seconds, |seconds|&lt;10<sup>18</sup>
     * @return INTERVAL SECOND
     */
    public static Interval ofSeconds(long seconds) {
        return new Interval(IntervalQualifier.SECOND, seconds < 0, Math.abs(seconds), 0);
    }

    /**
     * Creates a new INTERVAL SECOND.
     *
     * <p>
     * If both arguments are not equal to zero they should have the same sign.
     * </p>
     *
     * @param seconds
     *            seconds, |seconds|&lt;10<sup>18</sup>
     * @param nanos
     *            nanoseconds, |nanos|&lt;1,000,000,000
     * @return INTERVAL SECOND
     */
    public static Interval ofSeconds(long seconds, int nanos) {
        boolean negative = (seconds | nanos) < 0;
        if (negative) {
            if (seconds > 0 || nanos > 0) {
                throw new IllegalArgumentException();
            }
            seconds = -seconds;
            nanos = -nanos;
        }
        return new Interval(IntervalQualifier.SECOND, negative, seconds, nanos);
    }

    /**
     * Creates a new INTERVAL SECOND.
     *
     * @param nanos
     *            nanoseconds (including seconds)
     * @return INTERVAL SECOND
     */
    public static Interval ofNanos(long nanos) {
        boolean negative = nanos < 0;
        if (negative) {
            nanos = -nanos;
            if (nanos < 0) {
                // Long.MIN_VALUE = -9_223_372_036_854_775_808L
                return new Interval(IntervalQualifier.SECOND, true, 9_223_372_036L, 854_775_808);
            }
        }
        return new Interval(IntervalQualifier.SECOND, negative, nanos / NANOS_PER_SECOND, nanos % NANOS_PER_SECOND);
    }

    /**
     * Creates a new INTERVAL YEAR TO MONTH.
     *
     * <p>
     * If both arguments are not equal to zero they should have the same sign.
     * </p>
     *
     * @param years
     *            years, |years|&lt;10<sup>18</sup>
     * @param months
     *            months, |months|&lt;12
     * @return INTERVAL YEAR TO MONTH
     */
    public static Interval ofYearsMonths(long years, int months) {
        boolean negative = (years | months) < 0;
        if (negative) {
            if (years > 0 || months > 0) {
                throw new IllegalArgumentException();
            }
            years = -years;
            months = -months;
        }
        return new Interval(IntervalQualifier.YEAR_TO_MONTH, negative, years, months);
    }

    /**
     * Creates a new INTERVAL DAY TO HOUR.
     *
     * <p>
     * If both arguments are not equal to zero they should have the same sign.
     * </p>
     *
     * @param days
     *            days, |days|&lt;10<sup>18</sup>
     * @param hours
     *            hours, |hours|&lt;24
     * @return INTERVAL DAY TO HOUR
     */
    public static Interval ofDaysHours(long days, int hours) {
        boolean negative = (days | hours) < 0;
        if (negative) {
            if (days > 0 || hours > 0) {
                throw new IllegalArgumentException();
            }
            days = -days;
            hours = -hours;
        }
        return new Interval(IntervalQualifier.DAY_TO_HOUR, negative, days, hours);
    }

    /**
     * Creates a new INTERVAL DAY TO MINUTE.
     *
     * <p>
     * Non-zero arguments should have the same sign.
     * </p>
     *
     * @param days
     *            days, |days|&lt;10<sup>18</sup>
     * @param hours
     *            hours, |hours|&lt;24
     * @param minutes
     *            minutes, |minutes|&lt;60
     * @return INTERVAL DAY TO MINUTE
     */
    public static Interval ofDaysHoursMinutes(long days, int hours, int minutes) {
        boolean negative = (days | hours | minutes) < 0;
        if (negative) {
            if (days > 0 || hours > 0 || minutes > 0) {
                throw new IllegalArgumentException();
            }
            days = -days;
            hours = -hours;
            minutes = -minutes;
            if ((hours | minutes) < 0) {
                // Integer.MIN_VALUE
                throw new IllegalArgumentException();
            }
        }
        // Overflow in days or hours will be detected by constructor
        if (minutes >= 60) {
            throw new IllegalArgumentException();
        }
        return new Interval(IntervalQualifier.DAY_TO_MINUTE, negative, days, hours * 60L + minutes);
    }

    /**
     * Creates a new INTERVAL DAY TO SECOND.
     *
     * <p>
     * Non-zero arguments should have the same sign.
     * </p>
     *
     * @param days
     *            days, |days|&lt;10<sup>18</sup>
     * @param hours
     *            hours, |hours|&lt;24
     * @param minutes
     *            minutes, |minutes|&lt;60
     * @param seconds
     *            seconds, |seconds|&lt;60
     * @return INTERVAL DAY TO SECOND
     */
    public static Interval ofDaysHoursMinutesSeconds(long days, int hours, int minutes, int seconds) {
        return ofDaysHoursMinutesNanos(days, hours, minutes, seconds * NANOS_PER_SECOND);
    }

    /**
     * Creates a new INTERVAL DAY TO SECOND.
     *
     * <p>
     * Non-zero arguments should have the same sign.
     * </p>
     *
     * @param days
     *            days, |days|&lt;10<sup>18</sup>
     * @param hours
     *            hours, |hours|&lt;24
     * @param minutes
     *            minutes, |minutes|&lt;60
     * @param nanos
     *            nanoseconds, |nanos|&lt;60,000,000,000
     * @return INTERVAL DAY TO SECOND
     */
    public static Interval ofDaysHoursMinutesNanos(long days, int hours, int minutes, long nanos) {
        boolean negative = (days | hours | minutes | nanos) < 0;
        if (negative) {
            if (days > 0 || hours > 0 || minutes > 0 || nanos > 0) {
                throw new IllegalArgumentException();
            }
            days = -days;
            hours = -hours;
            minutes = -minutes;
            nanos = -nanos;
            if ((hours | minutes | nanos) < 0) {
                // Integer.MIN_VALUE, Long.MIN_VALUE
                throw new IllegalArgumentException();
            }
        }
        if (hours >= 24 || minutes >= 60 || nanos >= NANOS_PER_MINUTE) {
            throw new IllegalArgumentException();
        }
        return new Interval(IntervalQualifier.DAY_TO_SECOND, negative, days,
                (hours * 60L + minutes) * NANOS_PER_MINUTE + nanos);
    }

    /**
     * Creates a new INTERVAL HOUR TO MINUTE.
     *
     * <p>
     * If both arguments are not equal to zero they should have the same sign.
     * </p>
     *
     * @param hours
     *            hours, |hours|&lt;10<sup>18</sup>
     * @param minutes
     *            minutes, |minutes|&lt;60
     * @return INTERVAL HOUR TO MINUTE
     */
    public static Interval ofHoursMinutes(long hours, int minutes) {
        boolean negative = (hours | minutes) < 0;
        if (negative) {
            if (hours > 0 || minutes > 0) {
                throw new IllegalArgumentException();
            }
            hours = -hours;
            minutes = -minutes;
        }
        return new Interval(IntervalQualifier.HOUR_TO_MINUTE, negative, hours, minutes);
    }

    /**
     * Creates a new INTERVAL HOUR TO SECOND.
     *
     * <p>
     * Non-zero arguments should have the same sign.
     * </p>
     *
     * @param hours
     *            hours, |hours|&lt;10<sup>18</sup>
     * @param minutes
     *            minutes, |minutes|&lt;60
     * @param seconds
     *            seconds, |seconds|&lt;60
     * @return INTERVAL HOUR TO SECOND
     */
    public static Interval ofHoursMinutesSeconds(long hours, int minutes, int seconds) {
        return ofHoursMinutesNanos(hours, minutes, seconds * NANOS_PER_SECOND);
    }

    /**
     * Creates a new INTERVAL HOUR TO SECOND.
     *
     * <p>
     * Non-zero arguments should have the same sign.
     * </p>
     *
     * @param hours
     *            hours, |hours|&lt;10<sup>18</sup>
     * @param minutes
     *            minutes, |minutes|&lt;60
     * @param nanos
     *            nanoseconds, |seconds|&lt;60,000,000,000
     * @return INTERVAL HOUR TO SECOND
     */
    public static Interval ofHoursMinutesNanos(long hours, int minutes, long nanos) {
        boolean negative = (hours | minutes | nanos) < 0;
        if (negative) {
            if (hours > 0 || minutes > 0 || nanos > 0) {
                throw new IllegalArgumentException();
            }
            hours = -hours;
            minutes = -minutes;
            nanos = -nanos;
            if ((minutes | nanos) < 0) {
                // Integer.MIN_VALUE, Long.MIN_VALUE
                throw new IllegalArgumentException();
            }
        }
        // Overflow in hours or minutes will be detected by constructor
        if (nanos >= NANOS_PER_MINUTE) {
            throw new IllegalArgumentException();
        }
        return new Interval(IntervalQualifier.HOUR_TO_SECOND, negative, hours, minutes * NANOS_PER_MINUTE + nanos);
    }

    /**
     * Creates a new INTERVAL MINUTE TO SECOND
     *
     * <p>
     * If both arguments are not equal to zero they should have the same sign.
     * </p>
     *
     * @param minutes
     *            minutes, |minutes|&lt;10<sup>18</sup>
     * @param seconds
     *            seconds, |seconds|&lt;60
     * @return INTERVAL MINUTE TO SECOND
     */
    public static Interval ofMinutesSeconds(long minutes, int seconds) {
        return ofMinutesNanos(minutes, seconds * NANOS_PER_SECOND);
    }

    /**
     * Creates a new INTERVAL MINUTE TO SECOND
     *
     * <p>
     * If both arguments are not equal to zero they should have the same sign.
     * </p>
     *
     * @param minutes
     *            minutes, |minutes|&lt;10<sup>18</sup>
     * @param nanos
     *            nanoseconds, |nanos|&lt;60,000,000,000
     * @return INTERVAL MINUTE TO SECOND
     */
    public static Interval ofMinutesNanos(long minutes, long nanos) {
        boolean negative = (minutes | nanos) < 0;
        if (negative) {
            if (minutes > 0 || nanos > 0) {
                throw new IllegalArgumentException();
            }
            minutes = -minutes;
            nanos = -nanos;
        }
        return new Interval(IntervalQualifier.MINUTE_TO_SECOND, negative, minutes, nanos);
    }

    /**
     * Creates a new interval. Do not use this constructor, use static methods
     * instead.
     *
     * @param qualifier
     *            qualifier
     * @param negative
     *            whether interval is negative
     * @param leading
     *            value of leading field
     * @param remaining
     *            combined value of all remaining fields
     */
    public Interval(IntervalQualifier qualifier, boolean negative, long leading, long remaining) {
        this.qualifier = qualifier;
        try {
            this.negative = DateTimeUtils.validateInterval(qualifier, negative, leading, remaining);
        } catch (DbException e) {
            throw new IllegalArgumentException();
        }
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
     * Returns years value, if any.
     *
     * @return years, or 0
     */
    public long getYears() {
        return DateTimeUtils.yearsFromInterval(qualifier, negative, leading, remaining);
    }

    /**
     * Returns months value, if any.
     *
     * @return months, or 0
     */
    public long getMonths() {
        return DateTimeUtils.monthsFromInterval(qualifier, negative, leading, remaining);
    }

    /**
     * Returns days value, if any.
     *
     * @return days, or 0
     */
    public long getDays() {
        return DateTimeUtils.daysFromInterval(qualifier, negative, leading, remaining);
    }

    /**
     * Returns hours value, if any.
     *
     * @return hours, or 0
     */
    public long getHours() {
        return DateTimeUtils.hoursFromInterval(qualifier, negative, leading, remaining);
    }

    /**
     * Returns minutes value, if any.
     *
     * @return minutes, or 0
     */
    public long getMinutes() {
        return DateTimeUtils.minutesFromInterval(qualifier, negative, leading, remaining);
    }

    /**
     * Returns value of integer part of seconds, if any.
     *
     * @return seconds, or 0
     */
    public long getSeconds() {
        if (qualifier == IntervalQualifier.SECOND) {
            return negative ? -leading : leading;
        }
        return getSecondsAndNanos() / NANOS_PER_SECOND;
    }

    /**
     * Returns value of fractional part of seconds (in nanoseconds), if any.
     *
     * @return nanoseconds, or 0
     */
    public long getNanosOfSecond() {
        if (qualifier == IntervalQualifier.SECOND) {
            return negative ? -remaining : remaining;
        }
        return getSecondsAndNanos() % NANOS_PER_SECOND;
    }

    /**
     * Returns seconds value measured in nanoseconds, if any.
     *
     * <p>
     * This method returns a long value that cannot fit all possible values of
     * INTERVAL SECOND. For a very large intervals of this type use
     * {@link #getSeconds()} and {@link #getNanosOfSecond()} instead. This
     * method can be safely used for intervals of other day-time types.
     * </p>
     *
     * @return nanoseconds (including seconds), or 0
     */
    public long getSecondsAndNanos() {
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
