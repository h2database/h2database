/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

import org.h2.api.ErrorCode;
import org.h2.api.IntervalQualifier;
import org.h2.engine.CastDataProvider;
import org.h2.message.DbException;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueInterval;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimeTimeZone;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;

/**
 * This utility class provides access to JSR 310 classes.
 */
public class JSR310Utils {

    private static final class WithTimeZone8 extends TimeZoneProvider.WithTimeZone {

        private static final long EPOCH_SECONDS_HIGH = 31556889864403199L;

        private static final long EPOCH_SECONDS_LOW = -31557014167219200L;

        private static volatile DateTimeFormatter TIME_ZONE_FORMATTER;

        private final ZoneId zoneId;

        WithTimeZone8(ZoneId timeZone) {
            this.zoneId = timeZone;
        }

        @Override
        public int getTimeZoneOffsetUTC(long epochSeconds) {
            return zoneId.getRules().getOffset(epochSecondsForCalendar(epochSeconds)).getTotalSeconds();
        }

        @Override
        int getTimeZoneOffsetLocal(int year, int month, int day, int hour, int minute, int second) {
            year = yearForCalendar(year);
            return ZonedDateTime.of(LocalDateTime.of(year, month, day, hour, minute, second), zoneId).getOffset()
                    .getTotalSeconds();
        }

        @Override
        long getEpochSecondsFromLocal(int year, int month, int day, int hour, int minute, int second) {
            int yearForCalendar = yearForCalendar(year);
            long epoch = ZonedDateTime.of(LocalDateTime.of(yearForCalendar, month, day, hour, minute, second), zoneId)
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

        /**
         * Returns an Instant with EPOCH seconds within the range
         * -31,557,014,167,219,200..31,556,889,864,403,199
         * (-1000000000-01-01T00:00Z..1000000000-12-31T23:59:59.999999999Z). Too
         * large and too small EPOCH seconds are replaced with EPOCH seconds
         * within the range using the 400 years period of the Gregorian
         * calendar.
         *
         * @param epochSeconds
         *            the EPOCH seconds
         * @return an Instant with specified or the replacement EPOCH seconds
         *         within the range
         */
        private static Instant epochSecondsForCalendar(long epochSeconds) {
            if (epochSeconds > EPOCH_SECONDS_HIGH) {
                epochSeconds -= SECONDS_PER_PERIOD;
            } else if (epochSeconds < EPOCH_SECONDS_LOW) {
                epochSeconds += SECONDS_PER_PERIOD;
            }
            return Instant.ofEpochSecond(epochSeconds);
        }

        @Override
        public String toString() {
            return "TimeZoneProvider " + zoneId.getId();
        }

    }

    private static final long MIN_DATE_VALUE = (-999_999_999L << DateTimeUtils.SHIFT_YEAR)
            + (1 << DateTimeUtils.SHIFT_MONTH) + 1;

    private static final long MAX_DATE_VALUE = (999_999_999L << DateTimeUtils.SHIFT_YEAR)
            + (12 << DateTimeUtils.SHIFT_MONTH) + 31;

    private static final long MIN_INSTANT_SECOND = -31_557_014_167_219_200L;

    private static final long MAX_INSTANT_SECOND = 31_556_889_864_403_199L;

    private JSR310Utils() {
        // utility class
    }

    /**
     * Converts a value to a LocalDate.
     *
     * This method should only be called from Java 8 or later version.
     *
     * @param value
     *            the value to convert
     * @return the LocalDate
     */
    public static Object valueToLocalDate(Value value) {
        long dateValue = ((ValueDate) value.convertTo(Value.DATE)).getDateValue();
        if (dateValue > MAX_DATE_VALUE) {
            dateValue = MAX_DATE_VALUE;
        } else if (dateValue < MIN_DATE_VALUE) {
            dateValue = MIN_DATE_VALUE;
        }
        return LocalDate.of(DateTimeUtils.yearFromDateValue(dateValue), DateTimeUtils.monthFromDateValue(dateValue),
                DateTimeUtils.dayFromDateValue(dateValue));
    }

    /**
     * Converts a value to a LocalTime.
     *
     * This method should only be called from Java 8 or later version.
     *
     * @param value
     *            the value to convert
     * @return the LocalTime
     */
    public static Object valueToLocalTime(Value value) {
        return LocalTime.ofNanoOfDay(((ValueTime) value.convertTo(Value.TIME)).getNanos());
    }

    /**
     * Converts a value to a LocalDateTime.
     *
     * This method should only be called from Java 8 or later version.
     *
     * @param value
     *            the value to convert
     * @param provider
     *            the cast information provider
     * @return the LocalDateTime
     */
    public static Object valueToLocalDateTime(Value value, CastDataProvider provider) {
        ValueTimestamp valueTimestamp = (ValueTimestamp) value.convertTo(Value.TIMESTAMP, provider, false);
        return localDateTimeFromDateNanos(valueTimestamp.getDateValue(), valueTimestamp.getTimeNanos());
    }

    /**
     * Converts a value to a Instant.
     *
     * This method should only be called from Java 8 or later version.
     *
     * @param value
     *            the value to convert
     * @param provider
     *            the cast information provider
     * @return the Instant
     */
    public static Object valueToInstant(Value value, CastDataProvider provider) {
        ValueTimestampTimeZone valueTimestampTimeZone = (ValueTimestampTimeZone) value.convertTo(Value.TIMESTAMP_TZ,
                provider, false);
        long timeNanos = valueTimestampTimeZone.getTimeNanos();
        long epochSecond = DateTimeUtils.absoluteDayFromDateValue( //
                valueTimestampTimeZone.getDateValue()) * DateTimeUtils.SECONDS_PER_DAY //
                + timeNanos / DateTimeUtils.NANOS_PER_SECOND //
                - valueTimestampTimeZone.getTimeZoneOffsetSeconds();
        timeNanos %= DateTimeUtils.NANOS_PER_SECOND;
        if (epochSecond > MAX_INSTANT_SECOND) {
            epochSecond = MAX_INSTANT_SECOND;
            timeNanos = DateTimeUtils.NANOS_PER_SECOND - 1;
        } else if (epochSecond < MIN_INSTANT_SECOND) {
            epochSecond = MIN_INSTANT_SECOND;
            timeNanos = 0;
        }
        return Instant.ofEpochSecond(epochSecond, timeNanos);
    }

    /**
     * Converts a value to a OffsetDateTime.
     *
     * This method should only be called from Java 8 or later version.
     *
     * @param value
     *            the value to convert
     * @param provider
     *            the cast information provider
     * @return the OffsetDateTime
     */
    public static Object valueToOffsetDateTime(Value value, CastDataProvider provider) {
        return valueToOffsetDateTime(value, provider, false);
    }

    /**
     * Converts a value to a ZonedDateTime.
     *
     * This method should only be called from Java 8 or later version.
     *
     * @param value
     *            the value to convert
     * @param provider
     *            the cast information provider
     * @return the ZonedDateTime
     */
    public static Object valueToZonedDateTime(Value value, CastDataProvider provider) {
        return valueToOffsetDateTime(value, provider, true);
    }

    private static Object valueToOffsetDateTime(Value value, CastDataProvider provider, boolean zoned) {
        ValueTimestampTimeZone valueTimestampTimeZone = (ValueTimestampTimeZone) value.convertTo(Value.TIMESTAMP_TZ,
                provider, false);
        long dateValue = valueTimestampTimeZone.getDateValue();
        long timeNanos = valueTimestampTimeZone.getTimeNanos();
        LocalDateTime localDateTime = (LocalDateTime) localDateTimeFromDateNanos(dateValue, timeNanos);

        int timeZoneOffsetSeconds = valueTimestampTimeZone.getTimeZoneOffsetSeconds();

        ZoneOffset offset = ZoneOffset.ofTotalSeconds(timeZoneOffsetSeconds);

        return zoned ? ZonedDateTime.of(localDateTime, offset) : OffsetDateTime.of(localDateTime, offset);
    }

    /**
     * Converts a value to a OffsetTime.
     *
     * This method should only be called from Java 8 or later version.
     *
     * @param value
     *            the value to convert
     * @param provider
     *            the cast information provider
     * @return the OffsetTime
     */
    public static Object valueToOffsetTime(Value value, CastDataProvider provider) {
        ValueTimeTimeZone valueTimeTimeZone = (ValueTimeTimeZone) value.convertTo(Value.TIME_TZ, provider, false);
        return OffsetTime.of(LocalTime.ofNanoOfDay(valueTimeTimeZone.getNanos()),
                ZoneOffset.ofTotalSeconds(valueTimeTimeZone.getTimeZoneOffsetSeconds()));
    }

    /**
     * Converts a value to a Period.
     *
     * This method should only be called from Java 8 or later version.
     *
     * @param value
     *            the value to convert
     * @return the Period
     */
    public static Object valueToPeriod(Value value) {
        if (!(value instanceof ValueInterval)) {
            value = value.convertTo(Value.INTERVAL_YEAR_TO_MONTH);
        }
        if (!DataType.isYearMonthIntervalType(value.getValueType())) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, (Throwable) null, value.getString());
        }
        ValueInterval v = (ValueInterval) value;
        IntervalQualifier qualifier = v.getQualifier();
        boolean negative = v.isNegative();
        long leading = v.getLeading();
        long remaining = v.getRemaining();
        int y = Value.convertToInt(IntervalUtils.yearsFromInterval(qualifier, negative, leading, remaining), null);
        int m = Value.convertToInt(IntervalUtils.monthsFromInterval(qualifier, negative, leading, remaining), null);
        return Period.of(y, m, 0);
    }

    /**
     * Converts a value to a Duration.
     *
     * This method should only be called from Java 8 or later version.
     *
     * @param value
     *            the value to convert
     * @return the Duration
     */
    public static Object valueToDuration(Value value) {
        if (!(value instanceof ValueInterval)) {
            value = value.convertTo(Value.INTERVAL_DAY_TO_SECOND);
        }
        if (DataType.isYearMonthIntervalType(value.getValueType())) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, (Throwable) null, value.getString());
        }
        BigInteger[] dr = IntervalUtils.intervalToAbsolute((ValueInterval) value)
                .divideAndRemainder(BigInteger.valueOf(1_000_000_000));
        return Duration.ofSeconds(dr[0].longValue(), dr[1].longValue());
    }

    /**
     * Converts a LocalDate to a Value.
     *
     * @param localDate
     *            the LocalDate to convert, not {@code null}
     * @return the value
     */
    public static Value localDateToValue(Object localDate) {
        LocalDate ld = (LocalDate) localDate;
        return ValueDate.fromDateValue(DateTimeUtils.dateValue(ld.getYear(), ld.getMonthValue(), ld.getDayOfMonth()));
    }

    /**
     * Converts a LocalTime to a Value.
     *
     * @param localTime
     *            the LocalTime to convert, not {@code null}
     * @return the value
     */
    public static Value localTimeToValue(Object localTime) {
        return ValueTime.fromNanos(((LocalTime) localTime).toNanoOfDay());
    }

    /**
     * Converts a LocalDateTime to a Value.
     *
     * @param localDateTime
     *            the LocalDateTime to convert, not {@code null}
     * @return the value
     */
    public static Value localDateTimeToValue(Object localDateTime) {
        LocalDateTime ldt = (LocalDateTime) localDateTime;
        LocalDate localDate = ldt.toLocalDate();
        long dateValue = DateTimeUtils.dateValue(localDate.getYear(), localDate.getMonthValue(),
                localDate.getDayOfMonth());
        long timeNanos = ldt.toLocalTime().toNanoOfDay();
        return ValueTimestamp.fromDateValueAndNanos(dateValue, timeNanos);
    }

    /**
     * Converts a Instant to a Value.
     *
     * @param instant
     *            the Instant to convert, not {@code null}
     * @return the value
     */
    public static Value instantToValue(Object instant) {
        Instant i = (Instant) instant;
        long epochSecond = i.getEpochSecond();
        int nano = i.getNano();
        long absoluteDay = epochSecond / 86_400;
        // Round toward negative infinity
        if (epochSecond < 0 && (absoluteDay * 86_400 != epochSecond)) {
            absoluteDay--;
        }
        long timeNanos = (epochSecond - absoluteDay * 86_400) * 1_000_000_000 + nano;
        return ValueTimestampTimeZone.fromDateValueAndNanos(DateTimeUtils.dateValueFromAbsoluteDay(absoluteDay),
                timeNanos, 0);
    }

    /**
     * Converts a OffsetDateTime to a Value.
     *
     * @param offsetDateTime
     *            the OffsetDateTime to convert, not {@code null}
     * @return the value
     */
    public static ValueTimestampTimeZone offsetDateTimeToValue(Object offsetDateTime) {
        OffsetDateTime o = (OffsetDateTime) offsetDateTime;
        LocalDateTime localDateTime = o.toLocalDateTime();
        LocalDate localDate = localDateTime.toLocalDate();
        long dateValue = DateTimeUtils.dateValue(localDate.getYear(), localDate.getMonthValue(),
                localDate.getDayOfMonth());
        return ValueTimestampTimeZone.fromDateValueAndNanos(dateValue, localDateTime.toLocalTime().toNanoOfDay(),
                o.getOffset().getTotalSeconds());
    }

    /**
     * Converts a ZonedDateTime to a Value.
     *
     * @param zonedDateTime
     *            the ZonedDateTime to convert, not {@code null}
     * @return the value
     */
    public static ValueTimestampTimeZone zonedDateTimeToValue(Object zonedDateTime) {
        ZonedDateTime z = (ZonedDateTime) zonedDateTime;
        LocalDateTime localDateTime = z.toLocalDateTime();
        LocalDate localDate = localDateTime.toLocalDate();
        long dateValue = DateTimeUtils.dateValue(localDate.getYear(), localDate.getMonthValue(),
                localDate.getDayOfMonth());
        return ValueTimestampTimeZone.fromDateValueAndNanos(dateValue, localDateTime.toLocalTime().toNanoOfDay(),
                z.getOffset().getTotalSeconds());
    }

    /**
     * Converts a OffsetTime to a Value.
     *
     * @param offsetTime
     *            the OffsetTime to convert, not {@code null}
     * @return the value
     */
    public static ValueTimeTimeZone offsetTimeToValue(Object offsetTime) {
        OffsetTime o = (OffsetTime) offsetTime;
        return ValueTimeTimeZone.fromNanos(o.toLocalTime().toNanoOfDay(), o.getOffset().getTotalSeconds());
    }

    private static Object localDateTimeFromDateNanos(long dateValue, long timeNanos) {
        if (dateValue > MAX_DATE_VALUE) {
            dateValue = MAX_DATE_VALUE;
            timeNanos = DateTimeUtils.NANOS_PER_DAY - 1;
        } else if (dateValue < MIN_DATE_VALUE) {
            dateValue = MIN_DATE_VALUE;
            timeNanos = 0;
        }
        return LocalDateTime.of(LocalDate.of(DateTimeUtils.yearFromDateValue(dateValue),
                DateTimeUtils.monthFromDateValue(dateValue), DateTimeUtils.dayFromDateValue(dateValue)),
                LocalTime.ofNanoOfDay(timeNanos));
    }

    /**
     * Converts a Period to a Value.
     *
     * @param period
     *            the Period to convert, not {@code null}
     * @return the value
     */
    public static ValueInterval periodToValue(Object period) {
        Period p = (Period) period;
        int days = p.getDays();
        if (days != 0) {
            throw DbException.getInvalidValueException("Period.days", days);
        }
        int years = p.getYears();
        int months = p.getMonths();
        IntervalQualifier qualifier;
        boolean negative = false;
        long leading = 0L, remaining = 0L;
        if (years == 0) {
            if (months == 0L) {
                // Use generic qualifier
                qualifier = IntervalQualifier.YEAR_TO_MONTH;
            } else {
                qualifier = IntervalQualifier.MONTH;
                leading = months;
                if (leading < 0) {
                    leading = -leading;
                    negative = true;
                }
            }
        } else {
            if (months == 0L) {
                qualifier = IntervalQualifier.YEAR;
                leading = years;
                if (leading < 0) {
                    leading = -leading;
                    negative = true;
                }
            } else {
                qualifier = IntervalQualifier.YEAR_TO_MONTH;
                leading = years * 12 + months;
                if (leading < 0) {
                    leading = -leading;
                    negative = true;
                }
                remaining = leading % 12;
                leading /= 12;
            }
        }
        return ValueInterval.from(qualifier, negative, leading, remaining);
    }

    /**
     * Converts a Duration to a Value.
     *
     * @param duration
     *            the Duration to convert, not {@code null}
     * @return the value
     */
    public static ValueInterval durationToValue(Object duration) {
        Duration d = (Duration) duration;
        long seconds = d.getSeconds();
        int nano = d.getNano();
        boolean negative = seconds < 0;
        seconds = Math.abs(seconds);
        if (negative && nano != 0) {
            nano = 1_000_000_000 - nano;
            seconds--;
        }
        return ValueInterval.from(IntervalQualifier.SECOND, negative, seconds, nano);
    }

    /**
     * Returns a default time zone provider.
     *
     * @return the default time zone provider
     */
    public static TimeZoneProvider getDefaultTimeZoneProvider() {
        return new WithTimeZone8(ZoneId.systemDefault());
    }

    /**
     * Returns a time zone provider for a specified time zone ID.
     *
     * @param timeZoneId
     *            the time zone ID
     * @return the time zone provider
     */
    public static TimeZoneProvider getTimeZoneProvider(String timeZoneId) {
        return new WithTimeZone8(ZoneId.of(timeZoneId, ZoneId.SHORT_IDS));
    }

}
