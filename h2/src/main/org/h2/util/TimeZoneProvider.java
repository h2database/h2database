/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Provides access to time zone API.
 */
public abstract class TimeZoneProvider {

    /**
     * The UTC time zone provider.
     */
    public static final TimeZoneProvider UTC = new Simple((short) 0);

    /**
     * A small cache for timezone providers.
     */
    public static TimeZoneProvider[] CACHE;

    /**
     * The number of cache elements (needs to be a power of 2).
     */
    private static final int CACHE_SIZE = 32;

    /**
     * Returns the time zone provider with the specified offset.
     *
     * @param offset
     *            UTC offset in seconds
     * @return the time zone provider with the specified offset
     */
    public static TimeZoneProvider ofOffset(int offset) {
        if (offset == 0) {
            return UTC;
        }
        return new Simple(offset);
    }

    /**
     * Returns the time zone provider with the specified name.
     *
     * @param id
     *            the ID of the time zone
     * @return the time zone provider with the specified name
     * @throws IllegalArgumentException
     *             if time zone with specified ID isn't known
     */
    public static TimeZoneProvider ofId(String id) throws IllegalArgumentException {
        int length = id.length();
        if (length == 1 && id.charAt(0) == 'Z') {
            return UTC;
        }
        int index = 0;
        if (id.startsWith("GMT") || id.startsWith("UTC")) {
            if (length == 3) {
                return UTC;
            }
            index += 3;
        }
        readOffset: if (length - index >= 2) {
            boolean negative = false;
            char c = id.charAt(index);
            if (c == '+') {
                c = id.charAt(++index);
            } else if (c == '-') {
                negative = true;
                c = id.charAt(++index);
            } else {
                break readOffset;
            }
            if (c >= '0' && c <= '9') {
                int hour = c - '0';
                if (++index < length) {
                    c = id.charAt(index);
                    if (c >= '0' && c <= '9') {
                        hour = hour * 10 + c - '0';
                        index++;
                    }
                }
                if (index == length) {
                    int offset = hour * 3_600;
                    return ofOffset(negative ? -offset : offset);
                }
                if (id.charAt(index) == ':') {
                    if (++index < length) {
                        c = id.charAt(index);
                        if (c >= '0' && c <= '9') {
                            int minute = c - '0';
                            if (++index < length) {
                                c = id.charAt(index);
                                if (c >= '0' && c <= '9') {
                                    minute = minute * 10 + c - '0';
                                    index++;
                                }
                            }
                            if (index == length) {
                                int offset = (hour * 60 + minute) * 60;
                                return ofOffset(negative ? -offset : offset);
                            }
                            if (id.charAt(index) == ':') {
                                if (++index < length) {
                                    c = id.charAt(index);
                                    if (c >= '0' && c <= '9') {
                                        int second = c - '0';
                                        if (++index < length) {
                                            c = id.charAt(index);
                                            if (c >= '0' && c <= '9') {
                                                second = second * 10 + c - '0';
                                                index++;
                                            }
                                        }
                                        if (index == length) {
                                            int offset = (hour * 60 + minute) * 60 + second;
                                            return ofOffset(negative ? -offset : offset);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if (index > 0) {
                throw new IllegalArgumentException(id);
            }
        }
        int hash = id.hashCode() & (CACHE_SIZE - 1);
        TimeZoneProvider[] cache = CACHE;
        if (cache != null) {
            TimeZoneProvider provider = cache[hash];
            if (provider != null && provider.getId().equals(id)) {
                return provider;
            }
        }
        TimeZoneProvider provider = ofId(id, index, length);
        if (cache == null) {
            CACHE = cache = new TimeZoneProvider[CACHE_SIZE];
        }
        cache[hash] = provider;
        return provider;
    }

    private static TimeZoneProvider ofId(String id, int index, int length) {
        if (JSR310.PRESENT) {
            return JSR310Utils.getTimeZoneProvider(id);
        }
        TimeZone tz = TimeZone.getTimeZone(id);
        if (!tz.getID().startsWith(id)) {
            throw new IllegalArgumentException(id + " (" + tz.getID() + "?)");
        }
        return new WithTimeZone7(TimeZone.getTimeZone(id));
    }

    /**
     * Returns the time zone provider for the system default time zone.
     *
     * @return the time zone provider for the system default time zone
     */
    public static TimeZoneProvider getDefault() {
        if (JSR310.PRESENT) {
            return JSR310Utils.getDefaultTimeZoneProvider();
        }
        return new WithTimeZone7(TimeZone.getDefault());
    }

    /**
     * Calculates the time zone offset in seconds for the specified EPOCH
     * seconds.
     *
     * @param epochSeconds
     *            seconds since EPOCH
     * @return time zone offset in minutes
     */
    public abstract int getTimeZoneOffsetUTC(long epochSeconds);

    /**
     * Calculates the time zone offset in seconds for the specified date value
     * and nanoseconds since midnight in local time.
     *
     * @param dateValue
     *            date value
     * @param timeNanos
     *            nanoseconds since midnight
     * @return time zone offset in minutes
     */
    public abstract int getTimeZoneOffsetLocal(long dateValue, long timeNanos);

    /**
     * Calculates the epoch seconds from local date and time.
     *
     * @param dateValue
     *            date value
     * @param timeNanos
     *            nanoseconds since midnight
     * @return the epoch seconds value
     */
    public abstract long getEpochSecondsFromLocal(long dateValue, long timeNanos);

    /**
     * Returns the ID of the time zone.
     *
     * @return the ID of the time zone
     */
    public abstract String getId();

    /**
     * Get the standard time name or daylight saving time name of the time zone.
     *
     * @param epochSeconds
     *            seconds since EPOCH
     * @return the standard time name or daylight saving time name of the time
     *         zone
     */
    public abstract String getShortId(long epochSeconds);

    /**
     * Returns whether this is a simple time zone provider with a fixed offset
     * from UTC.
     *
     * @return whether this is a simple time zone provider with a fixed offset
     *         from UTC
     */
    public boolean hasFixedOffset() {
        return false;
    }

    private static final class Simple extends TimeZoneProvider {

        private final int offset;

        private volatile String id;

        Simple(int offset) {
            this.offset = offset;
        }

        @Override
        public int getTimeZoneOffsetUTC(long epochSeconds) {
            return offset;
        }

        @Override
        public int getTimeZoneOffsetLocal(long dateValue, long timeNanos) {
            return offset;
        }

        @Override
        public long getEpochSecondsFromLocal(long dateValue, long timeNanos) {
            return DateTimeUtils.getEpochSeconds(dateValue, timeNanos, offset);
        }

        @Override
        public String getId() {
            String id = this.id;
            if (id == null) {
                this.id = DateTimeUtils.timeZoneNameFromOffsetSeconds(offset);
            }
            return id;
        }

        @Override
        public String getShortId(long epochSeconds) {
            return getId();
        }

        @Override
        public boolean hasFixedOffset() {
            return true;
        }

        @Override
        public String toString() {
            return "TimeZoneProvider " + getId();
        }

    }

    /**
     * Abstract time zone provider with time zone.
     */
    static abstract class WithTimeZone extends TimeZoneProvider {

        /**
         * Number of seconds in 400 years.
         */
        static final long SECONDS_PER_PERIOD = 146_097L * 60 * 60 * 24;

        /**
         * Number of seconds per year.
         */
        static final long SECONDS_PER_YEAR = SECONDS_PER_PERIOD / 400;

        WithTimeZone() {
        }

        @Override
        public final int getTimeZoneOffsetLocal(long dateValue, long timeNanos) {
            int second = (int) (timeNanos / DateTimeUtils.NANOS_PER_SECOND);
            int minute = second / 60;
            second -= minute * 60;
            int hour = minute / 60;
            minute -= hour * 60;
            int year = DateTimeUtils.yearFromDateValue(dateValue);
            int month = DateTimeUtils.monthFromDateValue(dateValue);
            int day = DateTimeUtils.dayFromDateValue(dateValue);
            return getTimeZoneOffsetLocal(year, month, day, hour, minute, second);
        }

        /**
         * Get the timezone offset.
         *
         * @param year the year
         * @param month the month (1 - 12)
         * @param day the day (1 - 31)
         * @param hour the hour
         * @param minute the minute
         * @param second the second
         * @return the offset in seconds
         */
        abstract int getTimeZoneOffsetLocal(int year, int month, int day, int hour, int minute, int second);

        @Override
        public final long getEpochSecondsFromLocal(long dateValue, long timeNanos) {
            int year = DateTimeUtils.yearFromDateValue(dateValue), month = DateTimeUtils.monthFromDateValue(dateValue),
                    day = DateTimeUtils.dayFromDateValue(dateValue);
            int second = (int) (timeNanos / DateTimeUtils.NANOS_PER_SECOND);
            int minute = second / 60;
            second -= minute * 60;
            int hour = minute / 60;
            minute -= hour * 60;
            return getEpochSecondsFromLocal(year, month, day, hour, minute, second);
        }

        /**
         * Get the epoch seconds.
         *
         * @param year the year
         * @param month the month (1 - 12)
         * @param day the day (1 - 31)
         * @param hour the hour
         * @param minute the minute
         * @param second the second
         * @return the epoch seconds
         */
        abstract long getEpochSecondsFromLocal(int year, int month, int day, int hour, int minute, int second);

    }

    private static final class WithTimeZone7 extends WithTimeZone {

        private static final long EPOCH_SECONDS_HIGH = 730_000 * SECONDS_PER_PERIOD;

        private static final long EPOCH_SECONDS_LOW = -3 * SECONDS_PER_PERIOD;

        private final AtomicReference<GregorianCalendar> cachedCalendar = new AtomicReference<>();

        private final TimeZone timeZone;

        WithTimeZone7(TimeZone timeZone) {
            this.timeZone = timeZone;
        }

        @Override
        public int getTimeZoneOffsetUTC(long epochSeconds) {
            return timeZone.getOffset(epochSecondsForCalendar(epochSeconds) * 1_000) / 1_000;
        }

        @Override
        int getTimeZoneOffsetLocal(int year, int month, int day, int hour, int minute, int second) {
            year = yearForCalendar(year);
            GregorianCalendar c = cachedCalendar.getAndSet(null);
            if (c == null) {
                c = createCalendar();
            }
            c.clear();
            c.set(Calendar.ERA, GregorianCalendar.AD);
            c.set(Calendar.YEAR, year);
            c.set(Calendar.MONTH, /* January is 0 */ month - 1);
            c.set(Calendar.DAY_OF_MONTH, day);
            c.set(Calendar.HOUR_OF_DAY, hour);
            c.set(Calendar.MINUTE, minute);
            c.set(Calendar.SECOND, second);
            c.set(Calendar.MILLISECOND, 0);
            int offset = c.get(Calendar.ZONE_OFFSET) + c.get(Calendar.DST_OFFSET);
            cachedCalendar.compareAndSet(null, c);
            return offset / 1_000;
        }

        @Override
        long getEpochSecondsFromLocal(int year, int month, int day, int hour, int minute, int second) {
            int yearForCalendar = yearForCalendar(year);
            GregorianCalendar c = cachedCalendar.getAndSet(null);
            if (c == null) {
                c = createCalendar();
            }
            c.clear();
            c.set(Calendar.ERA, GregorianCalendar.AD);
            c.set(Calendar.YEAR, yearForCalendar);
            c.set(Calendar.MONTH, /* January is 0 */ month - 1);
            c.set(Calendar.DAY_OF_MONTH, day);
            c.set(Calendar.HOUR_OF_DAY, hour);
            c.set(Calendar.MINUTE, minute);
            c.set(Calendar.SECOND, second);
            c.set(Calendar.MILLISECOND, 0);
            long epoch = c.getTimeInMillis();
            cachedCalendar.compareAndSet(null, c);
            return epoch / 1_000 + (year - yearForCalendar) * SECONDS_PER_YEAR;
        }

        private GregorianCalendar createCalendar() {
            GregorianCalendar c = new GregorianCalendar(timeZone);
            c.setGregorianChange(DateTimeUtils.PROLEPTIC_GREGORIAN_CHANGE);
            return c;
        }

        @Override
        public String getId() {
            return timeZone.getID();
        }

        @Override
        public String getShortId(long epochSeconds) {
            return timeZone.getDisplayName(
                    timeZone.inDaylightTime(new Date(epochSecondsForCalendar(epochSeconds) * 1_000)), TimeZone.SHORT);
        }

        /**
         * Returns a year within the range 1..292,000,399 for the given year.
         * Too large and too small years are replaced with years within the
         * range using the 400 years period of the Gregorian calendar.
         *
         * java.util.* datetime API doesn't support too large and too small
         * years. Years before 1 need special handing, and very old years also
         * expose bugs in java.util.GregorianCalendar.
         *
         * Because we need them only to calculate a time zone offset, it's safe
         * to normalize them to such range. There are no transitions before the
         * year 1, and large years can have only the periodic transition rules.
         *
         * @param year
         *            the year
         * @return the specified year or the replacement year within the range
         */
        private static int yearForCalendar(int year) {
            if (year > 292_000_000) {
                year = year % 400 + 292_000_000;
            } else if (year <= 0) {
                year = year % 400 + 400;
            }
            return year;
        }

        /**
         * Returns EPOCH seconds within the range
         * -50,491,123,199..9,214,642,606,780,799
         * (0370-01-01T00:00:01Z..+292002369-12-31T23:59:59Z). Too large and too
         * small EPOCH seconds are replaced with EPOCH seconds within the range
         * using the 400 years period of the Gregorian calendar.
         *
         * @param epochSeconds
         *            the EPOCH seconds
         * @return the specified or the replacement EPOCH seconds within the
         *         range
         */
        private static long epochSecondsForCalendar(long epochSeconds) {
            if (epochSeconds > EPOCH_SECONDS_HIGH) {
                epochSeconds = epochSeconds % SECONDS_PER_PERIOD + EPOCH_SECONDS_HIGH;
            } else if (epochSeconds < EPOCH_SECONDS_LOW) {
                epochSeconds = epochSeconds % SECONDS_PER_PERIOD + EPOCH_SECONDS_LOW;
            }
            return epochSeconds;
        }

        @Override
        public String toString() {
            return "TimeZoneProvider " + timeZone.getID();
        }

    }

}
