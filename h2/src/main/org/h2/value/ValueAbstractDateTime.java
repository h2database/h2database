/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.util.TimeZone;

import org.h2.util.DateTimeUtils;

/**
 * Abstract date-time value.
 */
public abstract class ValueAbstractDateTime extends Value {

    /**
     * A bit field with bits for the year, month, and day (see DateTimeUtils for
     * encoding).
     *
     * @return the data value
     */
    public long getDateValue() {
        return 0;
    }

    /**
     * @return nanoseconds since midnight
     */
    public long getTimeNanos() {
        return 0;
    }

    /**
     * The timezone offset in minutes.
     *
     * @return the offset
     */
    public short getTimeZoneOffsetMins() {
        TimeZone tz = TimeZone.getDefault();
        long millis = DateTimeUtils.convertDateTimeValueToMillis(tz, getDateValue(), getTimeNanos() / 1000000);
        return (short) (tz.getOffset(millis) / 60000);
    }

}
