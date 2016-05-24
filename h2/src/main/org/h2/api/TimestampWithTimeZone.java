/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.api;

import java.sql.Timestamp;

/**
 * Extends java.sql.Timestamp to add our time zone information.
 */
public class TimestampWithTimeZone extends Timestamp {

    /**
     * The serial version UID.
     */
    private static final long serialVersionUID = 4413229090646777107L;

    /**
     * Time zone offset from UTC in minutes, range of -12hours to +12hours
     */
    private final short timeZoneOffsetMins;

    public TimestampWithTimeZone(long timeMillis, int nanos, short timeZoneOffsetMins) {
        super(timeMillis);
        setNanos(nanos);
        this.timeZoneOffsetMins = timeZoneOffsetMins;
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
    public int hashCode() {
        return 31 * super.hashCode() + timeZoneOffsetMins;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        TimestampWithTimeZone other = (TimestampWithTimeZone) obj;
        if (timeZoneOffsetMins != other.timeZoneOffsetMins) {
            return false;
        }
        return true;
    }

}
