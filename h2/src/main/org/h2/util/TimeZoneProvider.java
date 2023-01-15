/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.zone.ZoneRules;
import java.util.Locale;

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
        if (offset < (-18 * 60 * 60) || offset > (18 * 60 * 60)) {
            throw new IllegalArgumentException("Time zone offset " + offset + " seconds is out of range");
        }
        return new Simple(offset);
    }

    /**
     * Returns the time zone provider with the specified name.
     *
     * @param id
     *            the ID of the time zone
     * @return the time zone provider with the specified name
     * @throws RuntimeException
     *             if time zone with specified ID isn't known
     */
    public static TimeZoneProvider ofId(String id) throws RuntimeException {
        int length = id.length();
        if (length == 1 && id.charAt(0) == 'Z') {
            return UTC;
        }
        int index = 0;
        if (id.startsWith("GMT") || id.startsWith("UTC")) {
            if (length == 3) {
                return UTC;
            }
            index = 3;
        }
        if (length > index) {
            boolean negative = false;
            char c = id.charAt(index);
            if (length > index + 1) {
                if (c == '+') {
                    c = id.charAt(++index);
                } else if (c == '-') {
                    negative = true;
                    c = id.charAt(++index);
                }
            }
            if (index != 3 && c >= '0' && c <= '9') {
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
        TimeZoneProvider provider = new WithTimeZone(ZoneId.of(id, ZoneId.SHORT_IDS));
        if (cache == null) {
            CACHE = cache = new TimeZoneProvider[CACHE_SIZE];
        }
        cache[hash] = provider;
        return provider;
    }

    /**
     * Returns the time zone provider for the system default time zone.
     *
     * @return the time zone provider for the system default time zone
     */
    public static TimeZoneProvider getDefault() {
        ZoneId zoneId = ZoneId.systemDefault();
        ZoneOffset offset;
        if (zoneId instanceof ZoneOffset) {
            offset = (ZoneOffset) zoneId;
        } else {
            ZoneRules rules = zoneId.getRules();
            if (!rules.isFixedOffset()) {
                return new WithTimeZone(zoneId);
            }
            offset = rules.getOffset(Instant.EPOCH);
        }
        return ofOffset(offset.getTotalSeconds());
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

    /**
     * Time zone provider with offset.
     */
    private static final class Simple extends TimeZoneProvider {

        private final int offset;

        private volatile String id;

        Simple(int offset) {
            this.offset = offset;
        }

        @Override
        public int hashCode() {
            return offset + 129607;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || obj.getClass() != Simple.class) {
                return false;
            }
            return offset == ((Simple) obj).offset;
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
                this.id = id = DateTimeUtils.timeZoneNameFromOffsetSeconds(offset);
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
     * Time zone provider with time zone.
     */
    static final class WithTimeZone extends TimeZoneProvider {

        /**
         * Number of seconds in 400 years.
         */
        static final long SECONDS_PER_PERIOD = 146_097L * 60 * 60 * 24;

        /**
         * Number of seconds per year.
         */
        static final long SECONDS_PER_YEAR = SECONDS_PER_PERIOD / 400;

        private static volatile DateTimeFormatter TIME_ZONE_FORMATTER;

        private final ZoneId zoneId;

        WithTimeZone(ZoneId timeZone) {
            this.zoneId = timeZone;
        }

        @Override
        public int hashCode() {
            return zoneId.hashCode() + 951689;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || obj.getClass() != WithTimeZone.class) {
                return false;
            }
            return zoneId.equals(((WithTimeZone) obj).zoneId);
        }

        @Override
        public int getTimeZoneOffsetUTC(long epochSeconds) {
            /*
             * Construct an Instant with EPOCH seconds within the range
             * -31,557,014,135,532,000..31,556,889,832,715,999
             * (-999999999-01-01T00:00-18:00..
             * +999999999-12-31T23:59:59.999999999+18:00). Too large and too
             * small EPOCH seconds are replaced with EPOCH seconds within the
             * range using the 400 years period of the Gregorian calendar.
             *
             * H2 has slightly wider range of EPOCH seconds than Instant, and
             * ZoneRules.getOffset(Instant) does not support all Instant values
             * in all time zones.
             */
            if (epochSeconds > 31_556_889_832_715_999L) {
                epochSeconds -= SECONDS_PER_PERIOD;
            } else if (epochSeconds < -31_557_014_135_532_000L) {
                epochSeconds += SECONDS_PER_PERIOD;
            }
            return zoneId.getRules().getOffset(Instant.ofEpochSecond(epochSeconds)).getTotalSeconds();
        }

        @Override
        public int getTimeZoneOffsetLocal(long dateValue, long timeNanos) {
            int second = (int) (timeNanos / DateTimeUtils.NANOS_PER_SECOND);
            int minute = second / 60;
            second -= minute * 60;
            int hour = minute / 60;
            minute -= hour * 60;
            return ZonedDateTime.of(LocalDateTime.of(yearForCalendar(DateTimeUtils.yearFromDateValue(dateValue)),
                    DateTimeUtils.monthFromDateValue(dateValue), DateTimeUtils.dayFromDateValue(dateValue), hour,
                    minute, second), zoneId).getOffset().getTotalSeconds();
        }

        @Override
        public long getEpochSecondsFromLocal(long dateValue, long timeNanos) {
            int second = (int) (timeNanos / DateTimeUtils.NANOS_PER_SECOND);
            int minute = second / 60;
            second -= minute * 60;
            int hour = minute / 60;
            minute -= hour * 60;
            int year = DateTimeUtils.yearFromDateValue(dateValue);
            int yearForCalendar = yearForCalendar(year);
            long epoch = ZonedDateTime
                    .of(LocalDateTime.of(yearForCalendar, DateTimeUtils.monthFromDateValue(dateValue),
                            DateTimeUtils.dayFromDateValue(dateValue), hour, minute, second), zoneId)
                    .toOffsetDateTime().toEpochSecond();
            return epoch + (year - yearForCalendar) * SECONDS_PER_YEAR;
        }

        @Override
        public String getId() {
            return zoneId.getId();
        }

        @Override
        public String getShortId(long epochSeconds) {
            DateTimeFormatter timeZoneFormatter = TIME_ZONE_FORMATTER;
            if (timeZoneFormatter == null) {
                TIME_ZONE_FORMATTER = timeZoneFormatter = DateTimeFormatter.ofPattern("z", Locale.ENGLISH);
            }
            return ZonedDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds), zoneId).format(timeZoneFormatter);
        }

        /**
         * Returns a year within the range -999,999,999..999,999,999 for the
         * given year. Too large and too small years are replaced with years
         * within the range using the 400 years period of the Gregorian
         * calendar.
         *
         * Because we need them only to calculate a time zone offset, it's safe
         * to normalize them to such range.
         *
         * @param year
         *            the year
         * @return the specified year or the replacement year within the range
         */
        private static int yearForCalendar(int year) {
            if (year > 999_999_999) {
                year -= 400;
            } else if (year < -999_999_999) {
                year += 400;
            }
            return year;
        }

        @Override
        public String toString() {
            return "TimeZoneProvider " + zoneId.getId();
        }

    }

}
