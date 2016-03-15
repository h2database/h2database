/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (http://h2database.com/html/license.html). Initial Developer: H2
 * Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import org.h2.message.DbException;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;

/**
 * Implementation of the TIMESTAMP data type.
 */
public final class ValueTimestampUtc extends Value {

    /**
     * The precision in digits.
     */
    public static final int PRECISION = 23;

    /**
     * The display size of the textual representation of a timestamp. Example:
     * 2001-01-01 23:59:59.000 UTC
     */
    static final int DISPLAY_SIZE = 27;

    /**
     * The default scale for timestamps.
     */
    static final int DEFAULT_SCALE = 10;

    /**
     * Time in nanoseconds since 1 Jan 1970 i.e. similar format to
     * System.currentTimeMillis()
     */
    private final long utcDateTimeNanos;

    private ValueTimestampUtc(long utcDateTimeNanos) {
        this.utcDateTimeNanos = utcDateTimeNanos;
    }

    /**
     * Get or create a timestamp value for the given date/time in millis.
     *
     * @param utcDateTimeMillis the date and time in UTC milliseconds
     * @param nanos the nanoseconds since the last millisecond
     * @return the value
     */
    public static ValueTimestampUtc fromMillisNanos(long utcDateTimeMillis, int nanos) {
        if (nanos < 0 || nanos >= 1000 * 1000) {
            throw new IllegalArgumentException("nanos out of range " + nanos);
        }
        return (ValueTimestampUtc) Value.cache(new ValueTimestampUtc(
                utcDateTimeMillis * 1000 * 1000 + nanos));
    }

    /**
     * Get or create a timestamp value for the given date/time in millis.
     *
     * @param ms the milliseconds
     * @return the value
     */
    public static ValueTimestampUtc fromMillis(long ms) {
        return fromMillisNanos(ms, (short) 0);
    }

    /**
     * Get or create a timestamp value for the given date/time in nanos.
     *
     * @param nanos the nanos
     * @return the value
     */
    public static ValueTimestampUtc fromNanos(long nanos) {
        return (ValueTimestampUtc) Value.cache(new ValueTimestampUtc(nanos));
    }

    /**
     * Parse a string to a ValueTimestamp. This method supports the format
     * +/-year-month-day hour:minute:seconds.fractional and an optional timezone
     * part.
     *
     * @param s the string to parse
     * @return the date
     */
    public static ValueTimestampUtc parse(String s) {
        ValueTimestamp t1 = ValueTimestamp.parse(s);
        java.sql.Timestamp t2 = t1.getTimestamp();
        return fromMillisNanos(t2.getTime(), t2.getNanos());
    }

    /**
     * Time in nanoseconds since 1 Jan 1970 i.e. similar format to
     * System.currentTimeMillis().
     *
     * @return the number of milliseconds
     */
    public long getUtcDateTimeNanos() {
        return utcDateTimeNanos;
    }

    /**
     * Time in milliseconds since 1 Jan 1970 i.e. same format as
     * System.currentTimeMillis()
     *
     * @return the number of milliseconds
     */
    public long getUtcDateTimeMillis() {
        return utcDateTimeNanos / 1000 / 1000;
    }

    /**
     * Get the number of nanoseconds since the last full millisecond.
     *
     * @return the number of nanoseconds
     */
    int getNanosSinceLastMillis() {
        return (int) (utcDateTimeNanos % (1000 * 1000));
    }

    @Override
    public java.sql.Timestamp getTimestamp() {
        java.sql.Timestamp ts = new java.sql.Timestamp(getUtcDateTimeMillis());
        ts.setNanos(getNanosSinceLastMillis());
        return ts;
    }

    @Override
    public int getType() {
        return Value.TIMESTAMP_UTC;
    }

    @Override
    public String getString() {
        Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        cal.setTimeInMillis(getUtcDateTimeMillis());
        StringBuilder buff = new StringBuilder(DISPLAY_SIZE);

        // date part
        int y = cal.get(Calendar.YEAR);
        int m = cal.get(Calendar.MONTH);
        int d = cal.get(Calendar.DAY_OF_MONTH);
        if (y > 0 && y < 10000) {
            StringUtils.appendZeroPadded(buff, 4, y);
        } else {
            buff.append(y);
        }
        buff.append('-');
        StringUtils.appendZeroPadded(buff, 2, m);
        buff.append('-');
        StringUtils.appendZeroPadded(buff, 2, d);

        buff.append(' ');

        // time part
        long timeNanos = cal.get(Calendar.HOUR_OF_DAY);
        timeNanos *= 24;
        timeNanos += cal.get(Calendar.MINUTE);
        timeNanos *= 60;
        timeNanos += cal.get(Calendar.SECOND);
        timeNanos *= 60;
        timeNanos += cal.get(Calendar.MILLISECOND);
        timeNanos *= 1000 * 1000;
        timeNanos += getNanosSinceLastMillis();
        ValueTime.appendTime(buff, timeNanos, true);
        buff.append(" UTC");
        return buff.toString();
    }

    @Override
    public String getSQL() {
        return "TIMESTAMP UTC '" + getString() + "'";
    }

    @Override
    public long getPrecision() {
        return PRECISION;
    }

    @Override
    public int getScale() {
        return DEFAULT_SCALE;
    }

    @Override
    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    @Override
    public Value convertScale(boolean onlyToSmallerScale, int targetScale) {
        if (targetScale >= DEFAULT_SCALE) {
            return this;
        }
        if (targetScale < 0) {
            throw DbException.getInvalidValueException("scale", targetScale);
        }
        // TODO
        // long n = timeNanos;
        // BigDecimal bd = BigDecimal.valueOf(n);
        // bd = bd.movePointLeft(9);
        // bd = ValueDecimal.setScale(bd, targetScale);
        // bd = bd.movePointRight(9);
        // long n2 = bd.longValue();
        // if (n2 == n) {
        //     return this;
        // }
        return this;
    }

    @Override
    protected int compareSecure(Value o, CompareMode mode) {
        ValueTimestampUtc t = (ValueTimestampUtc) o;
        return MathUtils.compareLong(utcDateTimeNanos, t.utcDateTimeNanos);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (!(other instanceof ValueTimestampUtc)) {
            return false;
        }
        ValueTimestampUtc x = (ValueTimestampUtc) other;
        return utcDateTimeNanos == x.utcDateTimeNanos;
    }

    @Override
    public int hashCode() {
        return (int) (utcDateTimeNanos ^ (utcDateTimeNanos >>> 32));
    }

    @Override
    public Object getObject() {
        return getTimestamp();
    }

    @Override
    public long getLong() {
        return utcDateTimeNanos;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setTimestamp(parameterIndex, getTimestamp());
    }

    @Override
    public Value add(Value v) {
        ValueTimestampUtc t = (ValueTimestampUtc) v.convertTo(Value.TIMESTAMP_UTC);
        long d1 = utcDateTimeNanos + t.utcDateTimeNanos;
        return new ValueTimestampUtc(d1);
    }

    @Override
    public Value subtract(Value v) {
        ValueTimestampUtc t = (ValueTimestampUtc) v.convertTo(Value.TIMESTAMP_UTC);
        long d1 = utcDateTimeNanos - t.utcDateTimeNanos;
        return new ValueTimestampUtc(d1);
    }

}
