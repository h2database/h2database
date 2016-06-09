/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.api;

import java.io.Serializable;
import org.h2.util.DateTimeUtils;
import org.h2.util.StringUtils;

/**
 * How we expose "DATETIME WITH TIMEZONE" in our ResultSets.
 */
public class TimestampWithTimeZone implements Serializable, Cloneable {

    /**
     * The serial version UID.
     */
    private static final long serialVersionUID = 4413229090646777107L;

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

    public TimestampWithTimeZone(long dateValue, long timeNanos, short timeZoneOffsetMins) {
        this.dateValue = dateValue;
        this.timeNanos = timeNanos;
        this.timeZoneOffsetMins = timeZoneOffsetMins;
    }

    /**
     * @return the year-month-day bit field
     */
    public long getYMD() {
        return dateValue;
    }

    public long getYear() {
        return DateTimeUtils.yearFromDateValue(dateValue);
    }

    public long getMonth() {
        return DateTimeUtils.monthFromDateValue(dateValue);
    }

    public long getDay() {
        return DateTimeUtils.dayFromDateValue(dateValue);
    }

    public long getNanosSinceMidnight() {
        return timeNanos;
    }

    /**
     * The time zone offset in minutes.
     *
     * @return the offset
     */
    public short getTimeZoneOffsetMins() {
        return timeZoneOffsetMins;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        int y = DateTimeUtils.yearFromDateValue(dateValue);
        int month = DateTimeUtils.monthFromDateValue(dateValue);
        int d = DateTimeUtils.dayFromDateValue(dateValue);
        if (y > 0 && y < 10000) {
            StringUtils.appendZeroPadded(buff, 4, y);
        } else {
            buff.append(y);
        }
        buff.append('-');
        StringUtils.appendZeroPadded(buff, 2, month);
        buff.append('-');
        StringUtils.appendZeroPadded(buff, 2, d);
        buff.append(' ');
        long nanos = timeNanos;
        long ms = nanos / 1000000;
        nanos -= ms * 1000000;
        long s = ms / 1000;
        ms -= s * 1000;
        long min = s / 60;
        s -= min * 60;
        long h = min / 60;
        min -= h * 60;
        StringUtils.appendZeroPadded(buff, 2, h);
        buff.append(':');
        StringUtils.appendZeroPadded(buff, 2, min);
        buff.append(':');
        StringUtils.appendZeroPadded(buff, 2, s);
        buff.append('.');
        int start = buff.length();
        StringUtils.appendZeroPadded(buff, 3, ms);
        if (nanos > 0) {
            StringUtils.appendZeroPadded(buff, 6, nanos);
        }
        for (int i = buff.length() - 1; i > start; i--) {
            if (buff.charAt(i) != '0') {
                break;
            }
            buff.deleteCharAt(i);
        }
        short tz = timeZoneOffsetMins;
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
        return buff.toString();
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + timeZoneOffsetMins;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        TimestampWithTimeZone other = (TimestampWithTimeZone) obj;
        if (dateValue != other.dateValue) {
            return false;
        }
        if (timeNanos != other.timeNanos) {
            return false;
        }
        if (timeZoneOffsetMins != other.timeZoneOffsetMins) {
            return false;
        }
        return true;
    }

}
