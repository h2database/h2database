/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import static org.h2.util.DateTimeUtils.NANOS_PER_SECOND;
import static org.h2.util.DateTimeUtils.SECONDS_PER_DAY;
import static org.h2.util.DateTimeUtils.SHIFT_MONTH;
import static org.h2.util.DateTimeUtils.SHIFT_YEAR;
import static org.h2.util.DateTimeUtils.absoluteDayFromDateValue;
import static org.h2.util.DateTimeUtils.dateValue;
import static org.h2.util.DateTimeUtils.dateValueFromAbsoluteDay;
import static org.h2.util.DateTimeUtils.dayFromDateValue;
import static org.h2.util.DateTimeUtils.monthFromDateValue;
import static org.h2.util.DateTimeUtils.yearFromDateValue;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.h2.api.ErrorCode;
import org.h2.api.IntervalQualifier;
import org.h2.engine.CastDataProvider;
import org.h2.message.DbException;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
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

    private static final long MIN_DATE_VALUE = (-999_999_999L << SHIFT_YEAR)
            + (1 << SHIFT_MONTH) + 1;

    private static final long MAX_DATE_VALUE = (999_999_999L << SHIFT_YEAR)
            + (12 << SHIFT_MONTH) + 31;

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
     * @param provider
     *            the cast information provider
     * @return the LocalDate
     */
    public static LocalDate valueToLocalDate(Value value, CastDataProvider provider) {
        long dateValue = value.convertToDate(provider).getDateValue();
        if (dateValue > MAX_DATE_VALUE) {
            return LocalDate.MAX;
        } else if (dateValue < MIN_DATE_VALUE) {
            return LocalDate.MIN;
        }
        return LocalDate.of(yearFromDateValue(dateValue), monthFromDateValue(dateValue),
                dayFromDateValue(dateValue));
    }

    /**
     * Converts a value to a LocalTime.
     *
     * This method should only be called from Java 8 or later version.
     *
     * @param value
     *            the value to convert
     * @param provider
     *            the cast information provider
     * @return the LocalTime
     */
    public static LocalTime valueToLocalTime(Value value, CastDataProvider provider) {
        return LocalTime.ofNanoOfDay(((ValueTime) value.convertTo(TypeInfo.TYPE_TIME, provider)).getNanos());
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
    public static LocalDateTime valueToLocalDateTime(Value value, CastDataProvider provider) {
        ValueTimestamp valueTimestamp = (ValueTimestamp) value.convertTo(TypeInfo.TYPE_TIMESTAMP, provider);
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
    public static Instant valueToInstant(Value value, CastDataProvider provider) {
        ValueTimestampTimeZone valueTimestampTimeZone = (ValueTimestampTimeZone) value
                .convertTo(TypeInfo.TYPE_TIMESTAMP_TZ, provider);
        long timeNanos = valueTimestampTimeZone.getTimeNanos();
        long epochSecond = absoluteDayFromDateValue(valueTimestampTimeZone.getDateValue())
                * SECONDS_PER_DAY //
                + timeNanos / NANOS_PER_SECOND //
                - valueTimestampTimeZone.getTimeZoneOffsetSeconds();
        if (epochSecond > MAX_INSTANT_SECOND) {
            return Instant.MAX;
        } else if (epochSecond < MIN_INSTANT_SECOND) {
            return Instant.MIN;
        }
        return Instant.ofEpochSecond(epochSecond, timeNanos % NANOS_PER_SECOND);
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
    public static OffsetDateTime valueToOffsetDateTime(Value value, CastDataProvider provider) {
        ValueTimestampTimeZone v = (ValueTimestampTimeZone) value.convertTo(TypeInfo.TYPE_TIMESTAMP_TZ, provider);
        return OffsetDateTime.of(localDateTimeFromDateNanos(v.getDateValue(), v.getTimeNanos()),
                ZoneOffset.ofTotalSeconds(v.getTimeZoneOffsetSeconds()));
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
    public static ZonedDateTime valueToZonedDateTime(Value value, CastDataProvider provider) {
        ValueTimestampTimeZone v = (ValueTimestampTimeZone) value.convertTo(TypeInfo.TYPE_TIMESTAMP_TZ, provider);
        return ZonedDateTime.of(localDateTimeFromDateNanos(v.getDateValue(), v.getTimeNanos()),
                ZoneOffset.ofTotalSeconds(v.getTimeZoneOffsetSeconds()));
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
    public static OffsetTime valueToOffsetTime(Value value, CastDataProvider provider) {
        ValueTimeTimeZone valueTimeTimeZone = (ValueTimeTimeZone) value.convertTo(TypeInfo.TYPE_TIME_TZ, provider);
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
    public static Period valueToPeriod(Value value) {
        if (!(value instanceof ValueInterval)) {
            value = value.convertTo(TypeInfo.TYPE_INTERVAL_YEAR_TO_MONTH);
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
    public static Duration valueToDuration(Value value) {
        if (!(value instanceof ValueInterval)) {
            value = value.convertTo(TypeInfo.TYPE_INTERVAL_DAY_TO_SECOND);
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
    public static ValueDate localDateToValue(LocalDate localDate) {
        return ValueDate.fromDateValue(
                dateValue(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth()));
    }

    /**
     * Converts a LocalTime to a Value.
     *
     * @param localTime
     *            the LocalTime to convert, not {@code null}
     * @return the value
     */
    public static ValueTime localTimeToValue(LocalTime localTime) {
        return ValueTime.fromNanos(localTime.toNanoOfDay());
    }

    /**
     * Converts a LocalDateTime to a Value.
     *
     * @param localDateTime
     *            the LocalDateTime to convert, not {@code null}
     * @return the value
     */
    public static ValueTimestamp localDateTimeToValue(LocalDateTime localDateTime) {
        LocalDate localDate = localDateTime.toLocalDate();
        return ValueTimestamp.fromDateValueAndNanos(
                dateValue(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth()),
                localDateTime.toLocalTime().toNanoOfDay());
    }

    /**
     * Converts a Instant to a Value.
     *
     * @param instant
     *            the Instant to convert, not {@code null}
     * @return the value
     */
    public static ValueTimestampTimeZone instantToValue(Instant instant) {
        long epochSecond = instant.getEpochSecond();
        int nano = instant.getNano();
        long absoluteDay = epochSecond / 86_400;
        // Round toward negative infinity
        if (epochSecond < 0 && (absoluteDay * 86_400 != epochSecond)) {
            absoluteDay--;
        }
        long timeNanos = (epochSecond - absoluteDay * 86_400) * 1_000_000_000 + nano;
        return ValueTimestampTimeZone.fromDateValueAndNanos(dateValueFromAbsoluteDay(absoluteDay),
                timeNanos, 0);
    }

    /**
     * Converts a OffsetDateTime to a Value.
     *
     * @param offsetDateTime
     *            the OffsetDateTime to convert, not {@code null}
     * @return the value
     */
    public static ValueTimestampTimeZone offsetDateTimeToValue(OffsetDateTime offsetDateTime) {
        LocalDateTime localDateTime = offsetDateTime.toLocalDateTime();
        LocalDate localDate = localDateTime.toLocalDate();
        return ValueTimestampTimeZone.fromDateValueAndNanos(
                dateValue(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth()),
                localDateTime.toLocalTime().toNanoOfDay(), //
                offsetDateTime.getOffset().getTotalSeconds());
    }

    /**
     * Converts a ZonedDateTime to a Value.
     *
     * @param zonedDateTime
     *            the ZonedDateTime to convert, not {@code null}
     * @return the value
     */
    public static ValueTimestampTimeZone zonedDateTimeToValue(ZonedDateTime zonedDateTime) {
        LocalDateTime localDateTime = zonedDateTime.toLocalDateTime();
        LocalDate localDate = localDateTime.toLocalDate();
        return ValueTimestampTimeZone.fromDateValueAndNanos(
                dateValue(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth()),
                localDateTime.toLocalTime().toNanoOfDay(), //
                zonedDateTime.getOffset().getTotalSeconds());
    }

    /**
     * Converts a OffsetTime to a Value.
     *
     * @param offsetTime
     *            the OffsetTime to convert, not {@code null}
     * @return the value
     */
    public static ValueTimeTimeZone offsetTimeToValue(OffsetTime offsetTime) {
        return ValueTimeTimeZone.fromNanos(offsetTime.toLocalTime().toNanoOfDay(),
                offsetTime.getOffset().getTotalSeconds());
    }

    private static LocalDateTime localDateTimeFromDateNanos(long dateValue, long timeNanos) {
        if (dateValue > MAX_DATE_VALUE) {
            return LocalDateTime.MAX;
        } else if (dateValue < MIN_DATE_VALUE) {
            return LocalDateTime.MIN;
        }
        return LocalDateTime.of(LocalDate.of(yearFromDateValue(dateValue),
                monthFromDateValue(dateValue), dayFromDateValue(dateValue)),
                LocalTime.ofNanoOfDay(timeNanos));
    }

    /**
     * Converts a Period to a Value.
     *
     * @param period
     *            the Period to convert, not {@code null}
     * @return the value
     */
    public static ValueInterval periodToValue(Period period) {
        int days = period.getDays();
        if (days != 0) {
            throw DbException.getInvalidValueException("Period.days", days);
        }
        int years = period.getYears();
        int months = period.getMonths();
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
    public static ValueInterval durationToValue(Duration duration) {
        long seconds = duration.getSeconds();
        int nano = duration.getNano();
        boolean negative = seconds < 0;
        seconds = Math.abs(seconds);
        if (negative && nano != 0) {
            nano = 1_000_000_000 - nano;
            seconds--;
        }
        return ValueInterval.from(IntervalQualifier.SECOND, negative, seconds, nano);
    }

}
