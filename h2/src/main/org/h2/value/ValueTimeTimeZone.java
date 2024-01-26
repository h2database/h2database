/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import org.h2.api.ErrorCode;
import org.h2.engine.CastDataProvider;
import org.h2.message.DbException;
import org.h2.util.DateTimeUtils;

/**
 * Implementation of the TIME WITH TIME ZONE data type.
 */
public final class ValueTimeTimeZone extends Value {

    /**
     * The default precision and display size of the textual representation of a
     * time. Example: 10:00:00+10:00
     */
    public static final int DEFAULT_PRECISION = 14;

    /**
     * The maximum precision and display size of the textual representation of a
     * time. Example: 10:00:00.123456789+10:00
     */
    public static final int MAXIMUM_PRECISION = 24;

    /**
     * Nanoseconds since midnight
     */
    private final long nanos;

    /**
     * Time zone offset from UTC in seconds, range of -18 hours to +18 hours.
     * This range is compatible with OffsetTime from JSR-310.
     */
    private final int timeZoneOffsetSeconds;

    /**
     * @param nanos
     *            nanoseconds since midnight
     */
    private ValueTimeTimeZone(long nanos, int timeZoneOffsetSeconds) {
        this.nanos = nanos;
        this.timeZoneOffsetSeconds = timeZoneOffsetSeconds;
    }

    /**
     * Get or create a time value.
     *
     * @param nanos
     *            the nanoseconds since midnight
     * @param timeZoneOffsetSeconds
     *            the timezone offset in seconds
     * @return the value
     */
    public static ValueTimeTimeZone fromNanos(long nanos, int timeZoneOffsetSeconds) {
        if (nanos < 0L || nanos >= DateTimeUtils.NANOS_PER_DAY) {
            throw DbException.get(ErrorCode.INVALID_DATETIME_CONSTANT_2, "TIME WITH TIME ZONE",
                    DateTimeUtils.appendTime(new StringBuilder(), nanos).toString());
        }
        /*
         * Some current and historic time zones have offsets larger than 12
         * hours. JSR-310 determines 18 hours as maximum possible offset in both
         * directions, so we use this limit too for compatibility.
         */
        if (timeZoneOffsetSeconds < (-18 * 60 * 60) || timeZoneOffsetSeconds > (18 * 60 * 60)) {
            throw new IllegalArgumentException("timeZoneOffsetSeconds " + timeZoneOffsetSeconds);
        }
        return (ValueTimeTimeZone) Value.cache(new ValueTimeTimeZone(nanos, timeZoneOffsetSeconds));
    }

    /**
     * Parse a string to a ValueTime.
     *
     * @param s
     *            the string to parse
     * @param provider
     *            the cast information provider, may be {@code null} for
     *            literals with time zone
     * @return the time
     */
    public static ValueTimeTimeZone parse(String s, CastDataProvider provider) {
        try {
            return (ValueTimeTimeZone) DateTimeUtils.parseTime(s, provider, true);
        } catch (Exception e) {
            throw DbException.get(ErrorCode.INVALID_DATETIME_CONSTANT_2, e, "TIME WITH TIME ZONE", s);
        }
    }

    /**
     * @return nanoseconds since midnight
     */
    public long getNanos() {
        return nanos;
    }

    /**
     * The time zone offset in seconds.
     *
     * @return the offset
     */
    public int getTimeZoneOffsetSeconds() {
        return timeZoneOffsetSeconds;
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_TIME_TZ;
    }

    @Override
    public int getValueType() {
        return TIME_TZ;
    }

    @Override
    public int getMemory() {
        return 32;
    }

    @Override
    public String getString() {
        return toString(new StringBuilder(MAXIMUM_PRECISION)).toString();
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return toString(builder.append("TIME WITH TIME ZONE '")).append('\'');
    }

    private StringBuilder toString(StringBuilder builder) {
        return DateTimeUtils.appendTimeZone(DateTimeUtils.appendTime(builder, nanos), timeZoneOffsetSeconds);
    }

    @Override
    public int compareTypeSafe(Value o, CompareMode mode, CastDataProvider provider) {
        ValueTimeTimeZone t = (ValueTimeTimeZone) o;
        return Long.compare(nanos - timeZoneOffsetSeconds * DateTimeUtils.NANOS_PER_SECOND,
                t.nanos - t.timeZoneOffsetSeconds * DateTimeUtils.NANOS_PER_SECOND);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (!(other instanceof ValueTimeTimeZone)) {
            return false;
        }
        ValueTimeTimeZone t = (ValueTimeTimeZone) other;
        return nanos == t.nanos && timeZoneOffsetSeconds == t.timeZoneOffsetSeconds;
    }

    @Override
    public int hashCode() {
        return (int) (nanos ^ (nanos >>> 32) ^ timeZoneOffsetSeconds);
    }

}
