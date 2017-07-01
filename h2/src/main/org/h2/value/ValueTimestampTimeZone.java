/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (http://h2database.com/html/license.html). Initial Developer: H2
 * Group
 */
package org.h2.value;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.TimeZone;
import org.h2.api.ErrorCode;
import org.h2.api.TimestampWithTimeZone;
import org.h2.message.DbException;
import org.h2.util.DateTimeUtils;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;

/**
 * Implementation of the TIMESTAMP WITH TIME ZONE data type.
 *
 * @see <a href="https://en.wikipedia.org/wiki/ISO_8601#Time_zone_designators">
 *      ISO 8601 Time zone designators</a>
 */
public class ValueTimestampTimeZone extends Value {

    /**
     * The precision in digits.
     */
    public static final int PRECISION = 30;

    /**
     * The display size of the textual representation of a timestamp. Example:
     * 2001-01-01 23:59:59.000 +10:00
     */
    static final int DISPLAY_SIZE = 30;

    /**
     * The default scale for timestamps.
     */
    static final int DEFAULT_SCALE = 10;

    private static final TimeZone GMT_TIMEZONE = TimeZone.getTimeZone("GMT");

    /**
     * A bit field with bits for the year, month, and day (see DateTimeUtils for
     * encoding)
     */
    private final long dateValue;
    /**
     * The nanoseconds since midnight.
     */
    private final long timeNanos;
    /**
     * Time zone offset from UTC in minutes, range of -12hours to +12hours
     */
    private final short timeZoneOffsetMins;

    private ValueTimestampTimeZone(long dateValue, long timeNanos,
            short timeZoneOffsetMins) {
        if (timeNanos < 0 || timeNanos >= 24L * 60 * 60 * 1000 * 1000 * 1000) {
            throw new IllegalArgumentException(
                    "timeNanos out of range " + timeNanos);
        }
        if (timeZoneOffsetMins < (-12 * 60)
                || timeZoneOffsetMins >= (12 * 60)) {
            throw new IllegalArgumentException(
                    "timeZoneOffsetMins out of range " + timeZoneOffsetMins);
        }
        this.dateValue = dateValue;
        this.timeNanos = timeNanos;
        this.timeZoneOffsetMins = timeZoneOffsetMins;
    }

    /**
     * Get or create a date value for the given date.
     *
     * @param dateValue the date value, a bit field with bits for the year,
     *            month, and day
     * @param timeNanos the nanoseconds since midnight
     * @param timeZoneOffsetMins the timezone offset in minutes
     * @return the value
     */
    public static ValueTimestampTimeZone fromDateValueAndNanos(long dateValue,
            long timeNanos, short timeZoneOffsetMins) {
        return (ValueTimestampTimeZone) Value.cache(new ValueTimestampTimeZone(
                dateValue, timeNanos, timeZoneOffsetMins));
    }

    /**
     * Get or create a timestamp value for the given timestamp.
     *
     * @param timestamp the timestamp
     * @return the value
     */
    public static ValueTimestampTimeZone get(TimestampWithTimeZone timestamp) {
        return fromDateValueAndNanos(timestamp.getYMD(),
                timestamp.getNanosSinceMidnight(),
                timestamp.getTimeZoneOffsetMins());
    }

    /**
     * Parse a string to a ValueTimestamp. This method supports the format
     * +/-year-month-day hour:minute:seconds.fractional and an optional timezone
     * part.
     *
     * @param s the string to parse
     * @return the date
     */
    public static ValueTimestampTimeZone parse(String s) {
        try {
            return parseTry(s);
        } catch (Exception e) {
            throw DbException.get(ErrorCode.INVALID_DATETIME_CONSTANT_2, e,
                    "TIMESTAMP WITH TIME ZONE", s);
        }
    }

    private static ValueTimestampTimeZone parseTry(String s) {
        int dateEnd = s.indexOf(' ');
        if (dateEnd < 0) {
            // ISO 8601 compatibility
            dateEnd = s.indexOf('T');
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
        short tzMinutes = 0;
        if (timeStart < 0) {
            nanos = 0;
        } else {
            int timeEnd = s.length();
            if (s.endsWith("Z")) {
                timeEnd--;
            } else {
                int timeZoneStart = s.indexOf('+', dateEnd);
                if (timeZoneStart < 0) {
                    timeZoneStart = s.indexOf('-', dateEnd);
                }
                TimeZone tz = null;
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
                if (tz != null) {
                    long millis = DateTimeUtils
                            .convertDateValueToMillis(GMT_TIMEZONE, dateValue);
                    tzMinutes = (short) (tz.getOffset(millis) / 1000 / 60);
                }
            }
            nanos = DateTimeUtils.parseTimeNanos(s, dateEnd + 1, timeEnd, true);
        }
        return ValueTimestampTimeZone.fromDateValueAndNanos(dateValue, nanos,
                tzMinutes);
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

    /**
     * The timezone offset in minutes.
     *
     * @return the offset
     */
    public short getTimeZoneOffsetMins() {
        return timeZoneOffsetMins;
    }

    @Override
    public Timestamp getTimestamp() {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public int getType() {
        return Value.TIMESTAMP_TZ;
    }

    @Override
    public String getString() {
        StringBuilder buff = new StringBuilder(DISPLAY_SIZE);
        ValueDate.appendDate(buff, dateValue);
        buff.append(' ');
        ValueTime.appendTime(buff, timeNanos, true);
        appendTimeZone(buff, timeZoneOffsetMins);
        return buff.toString();
    }

    /**
     * Append a time zone to the string builder.
     *
     * @param buff the target string builder
     * @param tz the time zone in minutes
     */
    private static void appendTimeZone(StringBuilder buff, short tz) {
        if (tz < 0) {
            buff.append('-');
            tz = (short) -tz;
        } else {
            buff.append('+');
        }
        int hours = tz / 60;
        tz -= hours * 60;
        int mins = tz;
        StringUtils.appendZeroPadded(buff, 2, hours);
        if (mins != 0) {
            buff.append(':');
            StringUtils.appendZeroPadded(buff, 2, mins);
        }
    }

    @Override
    public String getSQL() {
        return "TIMESTAMP WITH TIME ZONE '" + getString() + "'";
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
        return fromDateValueAndNanos(dateValue, n2, timeZoneOffsetMins);
    }

    @Override
    protected int compareSecure(Value o, CompareMode mode) {
        ValueTimestampTimeZone t = (ValueTimestampTimeZone) o;
        // We are pretending that the dateValue is in UTC because that gives us
        // a stable sort even if the DST database changes.

        // convert to minutes and add timezone offset
        long a = DateTimeUtils.convertDateValueToMillis(
                TimeZone.getTimeZone("UTC"), dateValue) /
                (1000L * 60L);
        long ma = timeNanos / (1000L * 1000L * 1000L * 60L);
        a += ma;
        a -= timeZoneOffsetMins;

        // convert to minutes and add timezone offset
        long b = DateTimeUtils.convertDateValueToMillis(
                TimeZone.getTimeZone("UTC"), t.dateValue) /
                (1000L * 60L);
        long mb = t.timeNanos / (1000L * 1000L * 1000L * 60L);
        b += mb;
        b -= t.timeZoneOffsetMins;

        // compare date
        int c = MathUtils.compareLong(a, b);
        if (c != 0) {
            return c;
        }
        // compare time
        long na = timeNanos - (ma * 1000L * 1000L * 1000L * 60L);
        long nb = t.timeNanos - (mb * 1000L * 1000L * 1000L * 60L);
        return MathUtils.compareLong(na, nb);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (!(other instanceof ValueTimestampTimeZone)) {
            return false;
        }
        ValueTimestampTimeZone x = (ValueTimestampTimeZone) other;
        return dateValue == x.dateValue && timeNanos == x.timeNanos
                && timeZoneOffsetMins == x.timeZoneOffsetMins;
    }

    @Override
    public int hashCode() {
        return (int) (dateValue ^ (dateValue >>> 32) ^ timeNanos
                ^ (timeNanos >>> 32) ^ timeZoneOffsetMins);
    }

    @Override
    public Object getObject() {
        return new TimestampWithTimeZone(dateValue, timeNanos,
                timeZoneOffsetMins);
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex)
            throws SQLException {
        prep.setString(parameterIndex, getString());
    }

    @Override
    public Value add(Value v) {
        throw DbException.getUnsupportedException(
                "manipulating TIMESTAMP WITH TIME ZONE values is unsupported");
    }

    @Override
    public Value subtract(Value v) {
        throw DbException.getUnsupportedException(
                "manipulating TIMESTAMP WITH TIME ZONE values is unsupported");
    }

}
