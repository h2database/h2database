/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 * Iso8601: Initial Developer: Philippe Marschall (firstName dot lastName
 * at gmail dot com)
 */
package org.h2.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Date;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.h2.api.TimestampWithTimeZone;
import org.h2.message.DbException;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;

/**
 * This utility class contains time conversion functions for Java 8
 * Date and Time API classes.
 *
 * <p>This class is implemented using reflection so that it compiles on
 * Java 7 as well.</p>
 *
 * <p>For LocalDate and LocalTime the conversion methods provided by
 * the JDK are used. For OffsetDateTime a custom conversion method is
 * used because it has no equivalent in JDBC. For LocalDateTime a
 * custom conversion method is used instead of the one provided by the
 * JDK as well.</p>
 *
 * <p>Using the JDK provided conversion method for LocalDateTime would
 * introduces some errors in edge cases. Consider the following case:
 * at 2016-03-27 02:00 in Europe/Berlin the clocks were set to
 * 2016-03-27 03:00. This means that 2016-03-27 02:15 does not exist in
 * Europe/Berlin. Unfortunately java.sql.Timestamp is in the the time
 * zone of the JVM. That means if you run a JVM with the time zone
 * Europe/Berlin then the SQL value 'TIMESTAMP 2016-03-27 02:15:00' can
 * not be represented. java.time.LocalDateTime does not have these
 * limitations but if we convert through java.sql.Timestamp we inherit
 * its limitations. Therefore that conversion must be avoided.</p>
 *
 * <p>Once the driver requires Java 8 all the reflection can be removed.</p>
 */
public class LocalDateTimeUtils {

    // Class<java.time.LocalDate>
    private static final Class<?> LOCAL_DATE;
    // Class<java.time.LocalTime>
    private static final Class<?> LOCAL_TIME;
    // Class<java.time.LocalDateTime>
    private static final Class<?> LOCAL_DATE_TIME;
    // Class<java.time.OffsetDateTime>
    private static final Class<?> OFFSET_DATE_TIME;
    // Class<java.time.ZoneOffset>
    private static final Class<?> ZONE_OFFSET;

    // java.sql.Date#toLocalDate()
    private static final Method TO_LOCAL_DATE;

    // java.time.LocalTime#ofNanoOfDay()
    private static final Method LOCAL_TIME_OF_NANO;

    // java.sql.Date#valueOf(LocalDate)
    private static final Method DATE_VALUE_OF;

    // java.time.LocalTime#toNanoOfDay()
    private static final Method LOCAL_TIME_TO_NANO;

    // java.time.LocalDate#of(int, int, int)
    private static final Method LOCAL_DATE_OF_YEAR_MONTH_DAY;
    // java.time.LocalDate#parse(CharSequence)
    private static final Method LOCAL_DATE_PARSE;
    // java.time.LocalDate#getYear()
    private static final Method LOCAL_DATE_GET_YEAR;
    // java.time.LocalDate#getMonthValue()
    private static final Method LOCAL_DATE_GET_MONTH_VALUE;
    // java.time.LocalDate#getDayOfMonth()
    private static final Method LOCAL_DATE_GET_DAY_OF_MONTH;
    // java.time.LocalDate#atStartOfDay()
    private static final Method LOCAL_DATE_AT_START_OF_DAY;

    // java.time.LocalTime#parse(CharSequence)
    private static final Method LOCAL_TIME_PARSE;

    // java.time.LocalDateTime#plusNanos(long)
    private static final Method LOCAL_DATE_TIME_PLUS_NANOS;
    // java.time.LocalDateTime#toLocalDate()
    private static final Method LOCAL_DATE_TIME_TO_LOCAL_DATE;
    // java.time.LocalDateTime#truncatedTo(TemporalUnit)
    private static final Method LOCAL_DATE_TIME_TRUNCATED_TO;
    // java.time.LocalDateTime#parse(CharSequence)
    private static final Method LOCAL_DATE_TIME_PARSE;

    // java.time.ZoneOffset#ofTotalSeconds(int)
    private static final Method ZONE_OFFSET_OF_TOTAL_SECONDS;

    // java.time.OffsetDateTime#of(LocalDateTime, ZoneOffset)
    private static final Method OFFSET_DATE_TIME_OF_LOCAL_DATE_TIME_ZONE_OFFSET;
    // java.time.OffsetDateTime#parse(CharSequence)
    private static final Method OFFSET_DATE_TIME_PARSE;
    // java.time.OffsetDateTime#toLocalDateTime()
    private static final Method OFFSET_DATE_TIME_TO_LOCAL_DATE_TIME;
    // java.time.OffsetDateTime#getOffset()
    private static final Method OFFSET_DATE_TIME_GET_OFFSET;

    // java.time.ZoneOffset#getTotalSeconds()
    private static final Method ZONE_OFFSET_GET_TOTAL_SECONDS;

    // java.time.Duration#between(Temporal, Temporal)
    private static final Method DURATION_BETWEEN;
    // java.time.Duration#toNanos()
    private static final Method DURATION_TO_NANOS;

    // java.time.temporal.ChronoUnit#DAYS
    private static final Object CHRONO_UNIT_DAYS;

    private static final boolean IS_JAVA8_DATE_API_PRESENT;


    static {
        LOCAL_DATE = tryGetClass("java.time.LocalDate");
        LOCAL_TIME = tryGetClass("java.time.LocalTime");
        LOCAL_DATE_TIME = tryGetClass("java.time.LocalDateTime");
        OFFSET_DATE_TIME = tryGetClass("java.time.OffsetDateTime");
        ZONE_OFFSET = tryGetClass("java.time.ZoneOffset");
        IS_JAVA8_DATE_API_PRESENT = LOCAL_DATE != null && LOCAL_TIME != null &&
                LOCAL_DATE_TIME != null && OFFSET_DATE_TIME != null &&
                ZONE_OFFSET != null;

        if (IS_JAVA8_DATE_API_PRESENT) {

            Class<?> temporalUnit = getClass("java.time.temporal.TemporalUnit");
            Class<?> chronoUnit = getClass("java.time.temporal.ChronoUnit");
            Class<?> duration = getClass("java.time.Duration");
            Class<?> temporal = getClass("java.time.temporal.Temporal");

            TO_LOCAL_DATE = getMethod(java.sql.Date.class, "toLocalDate");

            LOCAL_TIME_OF_NANO = getMethod(LOCAL_TIME, "ofNanoOfDay", long.class);

            DATE_VALUE_OF = getMethod(java.sql.Date.class, "valueOf", LOCAL_DATE);

            LOCAL_TIME_TO_NANO = getMethod(LOCAL_TIME, "toNanoOfDay");

            LOCAL_DATE_OF_YEAR_MONTH_DAY = getMethod(LOCAL_DATE, "of",
                    int.class, int.class, int.class);
            LOCAL_DATE_PARSE = getMethod(LOCAL_DATE, "parse",
                    CharSequence.class);
            LOCAL_DATE_GET_YEAR = getMethod(LOCAL_DATE, "getYear");
            LOCAL_DATE_GET_MONTH_VALUE = getMethod(LOCAL_DATE, "getMonthValue");
            LOCAL_DATE_GET_DAY_OF_MONTH = getMethod(LOCAL_DATE, "getDayOfMonth");
            LOCAL_DATE_AT_START_OF_DAY = getMethod(LOCAL_DATE, "atStartOfDay");

            LOCAL_TIME_PARSE = getMethod(LOCAL_TIME, "parse", CharSequence.class);

            LOCAL_DATE_TIME_PLUS_NANOS = getMethod(LOCAL_DATE_TIME, "plusNanos", long.class);
            LOCAL_DATE_TIME_TO_LOCAL_DATE = getMethod(LOCAL_DATE_TIME, "toLocalDate");
            LOCAL_DATE_TIME_TRUNCATED_TO = getMethod(LOCAL_DATE_TIME, "truncatedTo", temporalUnit);
            LOCAL_DATE_TIME_PARSE = getMethod(LOCAL_DATE_TIME, "parse", CharSequence.class);

            ZONE_OFFSET_OF_TOTAL_SECONDS = getMethod(ZONE_OFFSET, "ofTotalSeconds", int.class);

            OFFSET_DATE_TIME_TO_LOCAL_DATE_TIME = getMethod(OFFSET_DATE_TIME, "toLocalDateTime");
            OFFSET_DATE_TIME_GET_OFFSET = getMethod(OFFSET_DATE_TIME, "getOffset");
            OFFSET_DATE_TIME_OF_LOCAL_DATE_TIME_ZONE_OFFSET = getMethod(
                    OFFSET_DATE_TIME, "of", LOCAL_DATE_TIME, ZONE_OFFSET);
            OFFSET_DATE_TIME_PARSE = getMethod(OFFSET_DATE_TIME, "parse", CharSequence.class);

            ZONE_OFFSET_GET_TOTAL_SECONDS = getMethod(ZONE_OFFSET, "getTotalSeconds");

            DURATION_BETWEEN = getMethod(duration, "between", temporal, temporal);
            DURATION_TO_NANOS = getMethod(duration, "toNanos");

            CHRONO_UNIT_DAYS = getFieldValue(chronoUnit, "DAYS");
        } else {
            TO_LOCAL_DATE = null;
            LOCAL_TIME_OF_NANO = null;
            DATE_VALUE_OF = null;
            LOCAL_TIME_TO_NANO = null;
            LOCAL_DATE_OF_YEAR_MONTH_DAY = null;
            LOCAL_DATE_PARSE = null;
            LOCAL_DATE_GET_YEAR = null;
            LOCAL_DATE_GET_MONTH_VALUE = null;
            LOCAL_DATE_GET_DAY_OF_MONTH = null;
            LOCAL_DATE_AT_START_OF_DAY = null;
            LOCAL_TIME_PARSE = null;
            LOCAL_DATE_TIME_PLUS_NANOS = null;
            LOCAL_DATE_TIME_TO_LOCAL_DATE = null;
            LOCAL_DATE_TIME_TRUNCATED_TO = null;
            LOCAL_DATE_TIME_PARSE = null;
            ZONE_OFFSET_OF_TOTAL_SECONDS = null;
            OFFSET_DATE_TIME_TO_LOCAL_DATE_TIME = null;
            OFFSET_DATE_TIME_GET_OFFSET = null;
            OFFSET_DATE_TIME_OF_LOCAL_DATE_TIME_ZONE_OFFSET = null;
            OFFSET_DATE_TIME_PARSE = null;
            ZONE_OFFSET_GET_TOTAL_SECONDS = null;
            DURATION_BETWEEN = null;
            DURATION_TO_NANOS = null;
            CHRONO_UNIT_DAYS = null;
        }
    }

    private LocalDateTimeUtils() {
        // utility class
    }

    /**
     * Checks if the Java 8 Date and Time API is present.
     *
     * <p>This is the case on Java 8 and later and not the case on
     * Java 7. Versions older than Java 7 are not supported.</p>
     *
     * @return if the Java 8 Date and Time API is present
     */
    public static boolean isJava8DateApiPresent() {
        return IS_JAVA8_DATE_API_PRESENT;
    }

    /**
     * Returns the class java.time.LocalDate.
     *
     * @return the class java.time.LocalDate, null on Java 7
     */
    public static Class<?> getLocalDateClass() {
        return LOCAL_DATE;
    }


    /**
     * Returns the class java.time.LocalTime.
     *
     * @return the class java.time.LocalTime, null on Java 7
     */
    public static Class<?> getLocalTimeClass() {
        return LOCAL_TIME;
    }

    /**
     * Returns the class java.time.LocalDateTime.
     *
     * @return the class java.time.LocalDateTime, null on Java 7
     */
    public static Class<?> getLocalDateTimeClass() {
        return LOCAL_DATE_TIME;
    }

    /**
     * Returns the class java.time.OffsetDateTime.
     *
     * @return the class java.time.OffsetDateTime, null on Java 7
     */
    public static Class<?> getOffsetDateTimeClass() {
        return OFFSET_DATE_TIME;
    }

    /**
     * Parses an ISO date string into a java.time.LocalDate.
     *
     * @param text the ISO date string
     * @return the java.time.LocalDate instance
     */
    public static Object parseLocalDate(CharSequence text) {
        try {
            return LOCAL_DATE_PARSE.invoke(null, text);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalArgumentException("error when parsing text '" + text + "'", e);
        }
    }

    /**
     * Parses an ISO time string into a java.time.LocalTime.
     *
     * @param text the ISO time string
     * @return the java.time.LocalTime instance
     */
    public static Object parseLocalTime(CharSequence text) {
        try {
            return LOCAL_TIME_PARSE.invoke(null, text);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalArgumentException("error when parsing text '" + text + "'", e);
        }
    }

    /**
     * Parses an ISO date string into a java.time.LocalDateTime.
     *
     * @param text the ISO date string
     * @return the java.time.LocalDateTime instance
     */
    public static Object parseLocalDateTime(CharSequence text) {
        try {
            return LOCAL_DATE_TIME_PARSE.invoke(null, text);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalArgumentException("error when parsing text '" + text + "'", e);
        }
    }

    /**
     * Parses an ISO date string into a java.time.OffsetDateTime.
     *
     * @param text the ISO date string
     * @return the java.time.OffsetDateTime instance
     */
    public static Object parseOffsetDateTime(CharSequence text) {
        try {
            return OFFSET_DATE_TIME_PARSE.invoke(null, text);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalArgumentException("error when parsing text '" + text + "'", e);
        }
    }

    private static Class<?> tryGetClass(String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            return null;
        }
    }

    private static Class<?> getClass(String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Java 8 or later but class " +
                    className + " is missing", e);
        }
    }

    private static Method getMethod(Class<?> clazz, String methodName,
            Class<?>... parameterTypes) {
        try {
            return clazz.getMethod(methodName, parameterTypes);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("Java 8 or later but method " +
                    clazz.getName() + "#" + methodName + "(" +
                    Arrays.toString(parameterTypes) + ") is missing", e);
        }
    }

    private static Object getFieldValue(Class<?> clazz, String fieldName) {
        try {
            return clazz.getField(fieldName).get(null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException("Java 8 or later but field " +
                    clazz.getName() + "#" + fieldName + " is missing", e);
        }
    }

    /**
     * Checks if the given class is LocalDate.
     *
     * <p>This method can be called from Java 7.</p>
     *
     * @param clazz the class to check
     * @return if the class is LocalDate
     */
    public static boolean isLocalDate(Class<?> clazz) {
        return LOCAL_DATE == clazz;
    }

    /**
     * Checks if the given class is LocalTime.
     *
     * <p>This method can be called from Java 7.</p>
     *
     * @param clazz the class to check
     * @return if the class is LocalTime
     */
    public static boolean isLocalTime(Class<?> clazz) {
        return LOCAL_TIME == clazz;
    }

    /**
     * Checks if the given class is LocalDateTime.
     *
     * <p>This method can be called from Java 7.</p>
     *
     * @param clazz the class to check
     * @return if the class is LocalDateTime
     */
    public static boolean isLocalDateTime(Class<?> clazz) {
        return LOCAL_DATE_TIME == clazz;
    }

    /**
     * Checks if the given class is OffsetDateTime.
     *
     * <p>This method can be called from Java 7.</p>
     *
     * @param clazz the class to check
     * @return if the class is OffsetDateTime
     */
    public static boolean isOffsetDateTime(Class<?> clazz) {
        return OFFSET_DATE_TIME == clazz;
    }

    /**
     * Converts a value to a LocalDate.
     *
     * <p>This method should only called from Java 8 or later.</p>
     *
     * @param value the value to convert
     * @return the LocalDate
     */
    public static Object valueToLocalDate(Value value) {
        return dateToLocalDate(value.getDate());
    }

    /**
     * Converts a value to a LocalTime.
     *
     * <p>This method should only called from Java 8 or later.</p>
     *
     * @param value the value to convert
     * @return the LocalTime
     */
    public static Object valueToLocalTime(Value value) {
        try {
            return LOCAL_TIME_OF_NANO.invoke(null,
                    ((ValueTime) value.convertTo(Value.TIME)).getNanos());
        } catch (IllegalAccessException e) {
            throw DbException.convert(e);
        } catch (InvocationTargetException e) {
            throw DbException.convertInvocation(e, "time conversion failed");
        }
    }

    private static Object dateToLocalDate(Date date) {
        try {
            return TO_LOCAL_DATE.invoke(date);
        } catch (IllegalAccessException e) {
            throw DbException.convert(e);
        } catch (InvocationTargetException e) {
            throw DbException.convertInvocation(e, "date conversion failed");
        }
    }

    /**
     * Converts a value to a LocalDateTime.
     *
     * <p>This method should only called from Java 8 or later.</p>
     *
     * @param value the value to convert
     * @return the LocalDateTime
     */
    public static Object valueToLocalDateTime(ValueTimestamp value) {

        long dateValue = value.getDateValue();
        long timeNanos = value.getTimeNanos();
        try {
            Object localDate = localDateFromDateValue(dateValue);
            Object localDateTime = LOCAL_DATE_AT_START_OF_DAY.invoke(localDate);
            return LOCAL_DATE_TIME_PLUS_NANOS.invoke(localDateTime, timeNanos);
        } catch (IllegalAccessException e) {
            throw DbException.convert(e);
        } catch (InvocationTargetException e) {
            throw DbException.convertInvocation(e, "timestamp conversion failed");
        }
    }

    /**
     * Converts a value to a OffsetDateTime.
     *
     * <p>This method should only called from Java 8 or later.</p>
     *
     * @param value the value to convert
     * @return the OffsetDateTime
     */
    public static Object valueToOffsetDateTime(ValueTimestampTimeZone value) {
        return timestampWithTimeZoneToOffsetDateTime((TimestampWithTimeZone) value.getObject());
    }

    private static Object timestampWithTimeZoneToOffsetDateTime(
            TimestampWithTimeZone timestampWithTimeZone) {

        long dateValue = timestampWithTimeZone.getYMD();
        long timeNanos = timestampWithTimeZone.getNanosSinceMidnight();
        try {
            Object localDateTime = localDateTimeFromDateNanos(dateValue, timeNanos);

            short timeZoneOffsetMins = timestampWithTimeZone.getTimeZoneOffsetMins();
            int offsetSeconds = (int) TimeUnit.MINUTES.toSeconds(timeZoneOffsetMins);

            Object offset = ZONE_OFFSET_OF_TOTAL_SECONDS.invoke(null, offsetSeconds);

            return OFFSET_DATE_TIME_OF_LOCAL_DATE_TIME_ZONE_OFFSET.invoke(null,
                    localDateTime, offset);
        } catch (IllegalAccessException e) {
            throw DbException.convert(e);
        } catch (InvocationTargetException e) {
            throw DbException.convertInvocation(e, "timestamp with time zone conversion failed");
        }
    }

    /**
     * Converts a LocalDate to a Value.
     *
     * @param localDate the LocalDate to convert, not {@code null}
     * @return the value
     */
    public static Value localDateToDateValue(Object localDate) {
        try {
            Date date = (Date) DATE_VALUE_OF.invoke(null, localDate);
            return ValueDate.get(date);
        } catch (IllegalAccessException e) {
            throw DbException.convert(e);
        } catch (InvocationTargetException e) {
            throw DbException.convertInvocation(e, "date conversion failed");
        }
    }

    /**
     * Converts a LocalTime to a Value.
     *
     * @param localTime the LocalTime to convert, not {@code null}
     * @return the value
     */
    public static Value localTimeToTimeValue(Object localTime) {
        try {
            return ValueTime.fromNanos((Long) LOCAL_TIME_TO_NANO.invoke(localTime));
        } catch (IllegalAccessException e) {
            throw DbException.convert(e);
        } catch (InvocationTargetException e) {
            throw DbException.convertInvocation(e, "time conversion failed");
        }
    }

    /**
     * Converts a LocalDateTime to a Value.
     *
     * @param localDateTime the LocalDateTime to convert, not {@code null}
     * @return the value
     */
    public static Value localDateTimeToValue(Object localDateTime) {
        try {
            Object localDate = LOCAL_DATE_TIME_TO_LOCAL_DATE.invoke(localDateTime);
            long dateValue = dateValueFromLocalDate(localDate);
            long timeNanos = timeNanosFromLocalDate(localDateTime);
            return ValueTimestamp.fromDateValueAndNanos(dateValue, timeNanos);
        } catch (IllegalAccessException e) {
            throw DbException.convert(e);
        } catch (InvocationTargetException e) {
            throw DbException.convertInvocation(e, "local date time conversion failed");
        }
    }

    /**
     * Converts a OffsetDateTime to a Value.
     *
     * @param offsetDateTime the OffsetDateTime to convert, not {@code null}
     * @return the value
     */
    public static Value offsetDateTimeToValue(Object offsetDateTime) {
        try {
            Object localDateTime = OFFSET_DATE_TIME_TO_LOCAL_DATE_TIME.invoke(offsetDateTime);
            Object localDate = LOCAL_DATE_TIME_TO_LOCAL_DATE.invoke(localDateTime);
            Object zoneOffset = OFFSET_DATE_TIME_GET_OFFSET.invoke(offsetDateTime);

            long dateValue = dateValueFromLocalDate(localDate);
            long timeNanos = timeNanosFromLocalDate(localDateTime);
            short timeZoneOffsetMins = zoneOffsetToOffsetMinute(zoneOffset);
            return ValueTimestampTimeZone.fromDateValueAndNanos(dateValue,
                    timeNanos, timeZoneOffsetMins);
        } catch (IllegalAccessException e) {
            throw DbException.convert(e);
        } catch (InvocationTargetException e) {
            throw DbException.convertInvocation(e, "time conversion failed");
        }
    }

    private static long dateValueFromLocalDate(Object localDate)
                    throws IllegalAccessException, InvocationTargetException {
        int year = (Integer) LOCAL_DATE_GET_YEAR.invoke(localDate);
        int month = (Integer) LOCAL_DATE_GET_MONTH_VALUE.invoke(localDate);
        int day = (Integer) LOCAL_DATE_GET_DAY_OF_MONTH.invoke(localDate);
        return DateTimeUtils.dateValue(year, month, day);
    }

    private static long timeNanosFromLocalDate(Object localDateTime)
                    throws IllegalAccessException, InvocationTargetException {
        Object midnight = LOCAL_DATE_TIME_TRUNCATED_TO.invoke(localDateTime, CHRONO_UNIT_DAYS);
        Object duration = DURATION_BETWEEN.invoke(null, midnight, localDateTime);
        return (Long) DURATION_TO_NANOS.invoke(duration);
    }

    private static short zoneOffsetToOffsetMinute(Object zoneOffset)
                    throws IllegalAccessException, InvocationTargetException {
        int totalSeconds = (Integer) ZONE_OFFSET_GET_TOTAL_SECONDS.invoke(zoneOffset);
        return (short) TimeUnit.SECONDS.toMinutes(totalSeconds);
    }

    private static Object localDateFromDateValue(long dateValue)
                    throws IllegalAccessException, InvocationTargetException {

        int year = DateTimeUtils.yearFromDateValue(dateValue);
        int month = DateTimeUtils.monthFromDateValue(dateValue);
        int day = DateTimeUtils.dayFromDateValue(dateValue);

        return LOCAL_DATE_OF_YEAR_MONTH_DAY.invoke(null, year, month, day);
    }

    private static Object localDateTimeFromDateNanos(long dateValue, long timeNanos)
                    throws IllegalAccessException, InvocationTargetException {
        Object localDate = localDateFromDateValue(dateValue);
        Object localDateTime = LOCAL_DATE_AT_START_OF_DAY.invoke(localDate);
        return LOCAL_DATE_TIME_PLUS_NANOS.invoke(localDateTime, timeNanos);
    }

}
