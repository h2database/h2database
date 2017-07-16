/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;
import org.h2.api.ErrorCode;
import org.h2.engine.Mode;
import org.h2.message.DbException;
import org.h2.util.DateTimeUtils;
import org.h2.util.MathUtils;

/**
 * Implementation of the TIMESTAMP data type.
 */
public class ValueTimestamp extends Value {

    /**
     * The precision in digits.
     */
    public static final int PRECISION = 23;

    /**
     * The display size of the textual representation of a timestamp.
     * Example: 2001-01-01 23:59:59.000
     */
    static final int DISPLAY_SIZE = 23;

    /**
     * The default scale for timestamps.
     */
    static final int DEFAULT_SCALE = 10;

    /**
     * A bit field with bits for the year, month, and day (see DateTimeUtils for
     * encoding)
     */
    private final long dateValue;
    /**
     * The nanoseconds since midnight.
     */
    private final long timeNanos;

    private ValueTimestamp(long dateValue, long timeNanos) {
        this.dateValue = dateValue;
        if (timeNanos < 0 || timeNanos >= 24L * 60 * 60 * 1000 * 1000 * 1000) {
            throw new IllegalArgumentException("timeNanos out of range " + timeNanos);
        }
        this.timeNanos = timeNanos;
    }

    /**
     * Get or create a date value for the given date.
     *
     * @param dateValue the date value, a bit field with bits for the year,
     *            month, and day
     * @param timeNanos the nanoseconds since midnight
     * @return the value
     */
    public static ValueTimestamp fromDateValueAndNanos(long dateValue, long timeNanos) {
        return (ValueTimestamp) Value.cache(new ValueTimestamp(dateValue, timeNanos));
    }

    /**
     * Get or create a timestamp value for the given timestamp.
     *
     * @param timestamp the timestamp
     * @return the value
     */
    public static ValueTimestamp get(Timestamp timestamp) {
        long ms = timestamp.getTime();
        long nanos = timestamp.getNanos() % 1000000;
        long dateValue = DateTimeUtils.dateValueFromDate(ms);
        nanos += DateTimeUtils.nanosFromDate(ms);
        return fromDateValueAndNanos(dateValue, nanos);
    }

    /**
     * Get or create a timestamp value for the given date/time in millis.
     *
     * @param ms the milliseconds
     * @param nanos the nanoseconds
     * @return the value
     */
    public static ValueTimestamp fromMillisNanos(long ms, int nanos) {
        long dateValue = DateTimeUtils.dateValueFromDate(ms);
        long timeNanos = nanos + DateTimeUtils.nanosFromDate(ms);
        return fromDateValueAndNanos(dateValue, timeNanos);
    }

    /**
     * Get or create a timestamp value for the given date/time in millis.
     *
     * @param ms the milliseconds
     * @return the value
     */
    public static ValueTimestamp fromMillis(long ms) {
        long dateValue = DateTimeUtils.dateValueFromDate(ms);
        long nanos = DateTimeUtils.nanosFromDate(ms);
        return fromDateValueAndNanos(dateValue, nanos);
    }

    /**
     * Parse a string to a ValueTimestamp. This method supports the format
     * +/-year-month-day hour[:.]minute[:.]seconds.fractional and an optional timezone
     * part.
     *
     * @param s the string to parse
     * @return the date
     */
    public static ValueTimestamp parse(String s) {
        return parse(s, null);
    }

    /**
     * Parse a string to a ValueTimestamp, using the given {@link Mode}.
     * This method supports the format +/-year-month-day[ -]hour[:.]minute[:.]seconds.fractional
     * and an optional timezone part.
     *
     * @param s the string to parse
     * @param mode the database {@link Mode}
     * @return the date
     */
    public static ValueTimestamp parse(String s, Mode mode) {
        try {
            return parseTry(s, mode);
        } catch (Exception e) {
            throw DbException.get(ErrorCode.INVALID_DATETIME_CONSTANT_2,
                    e, "TIMESTAMP", s);
        }
    }

    /**
     * See:
     * https://stackoverflow.com/questions/3976616/how-to-find-nth-occurrence-of-character-in-a-string#answer-3976656
     */
    private static int findNthIndexOf(String str, char chr, int n) {
        int pos = str.indexOf(chr);
        while (--n > 0 && pos != -1)
            pos = str.indexOf(chr, pos + 1);
        return pos;
    }

    private static ValueTimestamp parseTry(String s, Mode mode) {
        int dateEnd = s.indexOf(' ');
        if (dateEnd < 0) {
            // ISO 8601 compatibility
            dateEnd = s.indexOf('T');
            if (dateEnd < 0 && mode != null && mode.allowDB2TimestampFormat) {
                // DB2 also allows dash between date and time
                dateEnd = findNthIndexOf(s, '-', 3);
            }
        }
        int timeStart;
        if (dateEnd < 0) {
            dateEnd = s.length();
            timeStart = -1;
        } else {
            timeStart = dateEnd + 1;
        }
        long dateValue = DateTimeUtils.parseDateValue(s, 0, dateEnd);
        long nanos;
        if (timeStart < 0) {
            nanos = 0;
        } else {
            int timeEnd = s.length();
            TimeZone tz = null;
            if (s.endsWith("Z")) {
                tz = TimeZone.getTimeZone("UTC");
                timeEnd--;
            } else {
                int timeZoneStart = s.indexOf('+', dateEnd + 1);
                if (timeZoneStart < 0) {
                    timeZoneStart = s.indexOf('-', dateEnd + 1);
                }
                if (timeZoneStart >= 0) {
                    String tzName = "GMT" + s.substring(timeZoneStart);
                    tz = TimeZone.getTimeZone(tzName);
                    if (!tz.getID().startsWith(tzName)) {
                        throw new IllegalArgumentException(
                                tzName + " (" + tz.getID() + "?)");
                    }
                    timeEnd = timeZoneStart;
                } else {
                    timeZoneStart = s.indexOf(' ', dateEnd + 1);
                    if (timeZoneStart > 0) {
                        String tzName = s.substring(timeZoneStart + 1);
                        tz = TimeZone.getTimeZone(tzName);
                        if (!tz.getID().startsWith(tzName)) {
                            throw new IllegalArgumentException(tzName);
                        }
                        timeEnd = timeZoneStart;
                    }
                }
            }
            nanos = DateTimeUtils.parseTimeNanos(s, dateEnd + 1, timeEnd, true);
            if (tz != null) {
                int year = DateTimeUtils.yearFromDateValue(dateValue);
                int month = DateTimeUtils.monthFromDateValue(dateValue);
                int day = DateTimeUtils.dayFromDateValue(dateValue);
                long ms = nanos / 1000000;
                nanos -= ms * 1000000;
                long second = ms / 1000;
                ms -= second * 1000;
                int minute = (int) (second / 60);
                second -= minute * 60;
                int hour = minute / 60;
                minute -= hour * 60;
                long millis = DateTimeUtils.getMillis(
                        tz, year, month, day, hour, minute, (int) second, (int) ms);
                ms = DateTimeUtils.convertToLocal(
                        new Date(millis),
                        Calendar.getInstance(TimeZone.getTimeZone("UTC")));
                long md = DateTimeUtils.MILLIS_PER_DAY;
                long absoluteDay = (ms >= 0 ? ms : ms - md + 1) / md;
                dateValue = DateTimeUtils.dateValueFromAbsoluteDay(absoluteDay);
                ms -= absoluteDay * md;
                nanos += ms * 1000000;
            }
        }
        return ValueTimestamp.fromDateValueAndNanos(dateValue, nanos);
    }

    /**
     * A bit field with bits for the year, month, and day (see DateTimeUtils for
     * encoding).
     *
     * @return the data value
     */
    public long getDateValue() {
        return dateValue;
    }

    /**
     * The nanoseconds since midnight.
     *
     * @return the nanoseconds
     */
    public long getTimeNanos() {
        return timeNanos;
    }

    @Override
    public Timestamp getTimestamp() {
        return DateTimeUtils.convertDateValueToTimestamp(dateValue, timeNanos);
    }

    @Override
    public int getType() {
        return Value.TIMESTAMP;
    }

    @Override
    public String getString() {
        StringBuilder buff = new StringBuilder(DISPLAY_SIZE);
        ValueDate.appendDate(buff, dateValue);
        buff.append(' ');
        ValueTime.appendTime(buff, timeNanos, true);
        return buff.toString();
    }

    @Override
    public String getSQL() {
        return "TIMESTAMP '" + getString() + "'";
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
        long n = timeNanos;
        BigDecimal bd = BigDecimal.valueOf(n);
        bd = bd.movePointLeft(9);
        bd = ValueDecimal.setScale(bd, targetScale);
        bd = bd.movePointRight(9);
        long n2 = bd.longValue();
        if (n2 == n) {
            return this;
        }
        return fromDateValueAndNanos(dateValue, n2);
    }

    @Override
    protected int compareSecure(Value o, CompareMode mode) {
        ValueTimestamp t = (ValueTimestamp) o;
        int c = MathUtils.compareLong(dateValue, t.dateValue);
        if (c != 0) {
            return c;
        }
        return MathUtils.compareLong(timeNanos, t.timeNanos);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (!(other instanceof ValueTimestamp)) {
            return false;
        }
        ValueTimestamp x = (ValueTimestamp) other;
        return dateValue == x.dateValue && timeNanos == x.timeNanos;
    }

    @Override
    public int hashCode() {
        return (int) (dateValue ^ (dateValue >>> 32) ^ timeNanos ^ (timeNanos >>> 32));
    }

    @Override
    public Object getObject() {
        return getTimestamp();
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex)
            throws SQLException {
        prep.setTimestamp(parameterIndex, getTimestamp());
    }

    @Override
    public Value add(Value v) {
        ValueTimestamp t = (ValueTimestamp) v.convertTo(Value.TIMESTAMP);
        long d1 = DateTimeUtils.absoluteDayFromDateValue(dateValue);
        long d2 = DateTimeUtils.absoluteDayFromDateValue(t.dateValue);
        return DateTimeUtils.normalizeTimestamp(d1 + d2, timeNanos + t.timeNanos);
    }

    @Override
    public Value subtract(Value v) {
        ValueTimestamp t = (ValueTimestamp) v.convertTo(Value.TIMESTAMP);
        long d1 = DateTimeUtils.absoluteDayFromDateValue(dateValue);
        long d2 = DateTimeUtils.absoluteDayFromDateValue(t.dateValue);
        return DateTimeUtils.normalizeTimestamp(d1 - d2, timeNanos - t.timeNanos);
    }

}
