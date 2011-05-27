/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.h2.message.Message;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;

/**
 * This utility class contains time conversion functions.
 */
public class DateTimeUtils {

    public static Timestamp convertTimestampToCalendar(Timestamp x, Calendar calendar) throws SQLException {
        if (x != null) {
            Timestamp y = new Timestamp(getLocalTime(x, calendar));
            // fix the nano seconds
            y.setNanos(x.getNanos());
            x = y;
        }
        return x;
    }

    public static Time cloneAndNormalizeTime(Time value) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(value);
        cal.set(1970, 0, 1);
        return new Time(cal.getTime().getTime());
    }

    public static Date cloneAndNormalizeDate(Date value) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(value);
        cal.set(Calendar.MILLISECOND, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        return new Date(cal.getTime().getTime());
    }

    public static Value convertDateToUniversal(Date x, Calendar source) throws SQLException {
        return ValueDate.get(new Date(getUniversalTime(source, x)));
    }

    public static Value convertTimeToUniversal(Time x, Calendar source) throws SQLException {
        return ValueTime.get(new Time(getUniversalTime(source, x)));
    }

    public static Value convertTimestampToUniversal(Timestamp x, Calendar source) throws SQLException {
        Timestamp y = new Timestamp(getUniversalTime(source, x));
        // fix the nano seconds
        y.setNanos(x.getNanos());
        return ValueTimestamp.getNoCopy(y);
    }

    private static long getUniversalTime(Calendar source, java.util.Date x) throws SQLException {
        if (source == null) {
            throw Message.getInvalidValueException("calendar", null);
        }
        source = (Calendar) source.clone();
        Calendar universal = Calendar.getInstance();
        source.setTime(x);
        convertTime(source, universal);
        return universal.getTime().getTime();
    }

    private static long getLocalTime(java.util.Date x, Calendar target) throws SQLException {
        if (target == null) {
            throw Message.getInvalidValueException("calendar", null);
        }
        target = (Calendar) target.clone();
        Calendar local = Calendar.getInstance();
        local.setTime(x);
        convertTime(local, target);
        return target.getTime().getTime();
    }

    private static void convertTime(Calendar from, Calendar to) {
        to.set(Calendar.YEAR, from.get(Calendar.YEAR));
        to.set(Calendar.MONTH, from.get(Calendar.MONTH));
        to.set(Calendar.DAY_OF_MONTH, from.get(Calendar.DAY_OF_MONTH));
        to.set(Calendar.HOUR_OF_DAY, from.get(Calendar.HOUR_OF_DAY));
        to.set(Calendar.MINUTE, from.get(Calendar.MINUTE));
        to.set(Calendar.SECOND, from.get(Calendar.SECOND));
        to.set(Calendar.MILLISECOND, from.get(Calendar.MILLISECOND));
    }

    public static Date convertDateToCalendar(Date x, Calendar calendar) throws SQLException {
        return x == null ? null : new Date(getLocalTime(x, calendar));
    }

    public static Time convertTimeToCalendar(Time x, Calendar calendar) throws SQLException {
        return x == null ? null : new Time(getLocalTime(x, calendar));
    }

    public static java.util.Date parseDateTime(String original, int type, int errorCode) throws SQLException {
        String s = original;
        if (s == null) {
            return null;
        }
        try {
            int timeStart = 0;
            TimeZone tz = null;
            if (type == Value.TIME) {
                timeStart = 0;
            } else {
                timeStart = s.indexOf(' ') + 1;
                if (timeStart <= 0) {
                    // ISO 8601 compatibility
                    timeStart = s.indexOf('T') + 1;
                }
            }

            int year = 1970, month = 1, day = 1;
            if (type != Value.TIME) {
                // support +year
                if (s.startsWith("+")) {
                    s = s.substring(1);
                }
                // start at position 1 to support -year
                int s1 = s.indexOf('-', 1);
                int s2 = s.indexOf('-', s1 + 1);
                if (s1 <= 0 || s2 <= s1) {
                    throw Message.getSQLException(errorCode, new String[] { s, "format yyyy-mm-dd" });
                }
                year = Integer.parseInt(s.substring(0, s1));
                month = Integer.parseInt(s.substring(s1 + 1, s2));
                int end = timeStart == 0 ? s.length() : timeStart - 1;
                day = Integer.parseInt(s.substring(s2 + 1, end));
            }
            int hour = 0, minute = 0, second = 0, nano = 0;
            if (type != Value.DATE) {
                int s1 = s.indexOf(':', timeStart);
                int s2 = s.indexOf(':', s1 + 1);
                int s3 = s.indexOf('.', s2 + 1);
                if (s1 <= 0 || s2 <= s1) {
                    throw Message.getSQLException(errorCode, new String[] { s, "format hh:mm:ss" });
                }

                if (s.endsWith("Z")) {
                    s = s.substring(0, s.length() - 1);
                    tz = TimeZone.getTimeZone("UTC");
                } else {
                    int timezoneStart = s.indexOf('+', s2 + 1);
                    if (timezoneStart < 0) {
                        timezoneStart = s.indexOf('-', s2 + 1);
                    }
                    if (timezoneStart >= 0) {
                        String tzName = "GMT" + s.substring(timezoneStart);
                        tz = TimeZone.getTimeZone(tzName);
                        if (!tz.getID().equals(tzName)) {
                            throw Message.getSQLException(errorCode, new String[] { s, tz.getID() + " <>" + tzName });
                        }
                        s = s.substring(0, timezoneStart).trim();
                    }
                }

                hour = Integer.parseInt(s.substring(timeStart, s1));
                minute = Integer.parseInt(s.substring(s1 + 1, s2));
                if (s3 < 0) {
                    second = Integer.parseInt(s.substring(s2 + 1));
                } else {
                    second = Integer.parseInt(s.substring(s2 + 1, s3));
                    String n = (s + "000000000").substring(s3 + 1, s3 + 10);
                    nano = Integer.parseInt(n);
                }
            }
            if (hour < 0 || hour > 23) {
                throw new IllegalArgumentException("hour: " + hour);
            }
            long time;
            try {
                time = getTime(false, tz, year, month, day, hour, minute, second, type != Value.TIMESTAMP, nano);
            } catch (IllegalArgumentException e) {
                // special case: if the time simply doesn't exist, use the lenient version
                if (e.toString().indexOf("HOUR_OF_DAY") > 0) {
                    time = getTime(true, tz, year, month, day, hour, minute, second, type != Value.TIMESTAMP, nano);
                } else {
                    throw e;
                }
            }
            switch (type) {
            case Value.DATE:
                return new java.sql.Date(time);
            case Value.TIME:
                return new java.sql.Time(time);
            case Value.TIMESTAMP: {
                Timestamp ts = new Timestamp(time);
                ts.setNanos(nano);
                return ts;
            }
            default:
                throw Message.getInternalError("type:" + type);
            }
        } catch (IllegalArgumentException e) {
            throw Message.getSQLException(errorCode, new String[]{original, e.toString()}, e);
        }
    }

    private static long getTime(boolean lenient, TimeZone tz, int year, int month, int day, int hour, int minute, int second, boolean setMillis, int nano) {
        Calendar c;
        if (tz == null) {
            c = Calendar.getInstance();
        } else {
            c = Calendar.getInstance(tz);
        }
        c.setLenient(lenient);
        if (year <= 0) {
            c.set(Calendar.ERA, GregorianCalendar.BC);
            c.set(Calendar.YEAR, 1 - year);
        } else {
            c.set(Calendar.YEAR, year);
        }
        c.set(Calendar.MONTH, month - 1); // january is 0
        c.set(Calendar.DAY_OF_MONTH, day);
        c.set(Calendar.HOUR_OF_DAY, hour);
        c.set(Calendar.MINUTE, minute);
        c.set(Calendar.SECOND, second);
        if (setMillis) {
            c.set(Calendar.MILLISECOND, nano / 1000000);
        }
        return c.getTime().getTime();
    }

}
