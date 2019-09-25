/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

/**
 * This utility class to check presence of JSR 310.
 */
public class JSR310 {

    /**
     * {@code Class<java.time.LocalDate>} or {@code null}.
     */
    public static final Class<?> LOCAL_DATE;

    /**
     * {@code Class<java.time.LocalTime>} or {@code null}.
     */
    public static final Class<?> LOCAL_TIME;

    /**
     * {@code Class<java.time.LocalDateTime>} or {@code null}.
     */
    public static final Class<?> LOCAL_DATE_TIME;

    /**
     * {@code Class<java.time.Instant>} or {@code null}.
     */
    public static final Class<?> INSTANT;

    /**
     * {@code Class<java.time.OffsetDateTime>} or {@code null}.
     */
    public static final Class<?> OFFSET_DATE_TIME;

    /**
     * {@code Class<java.time.ZonedDateTime>} or {@code null}.
     */
    public static final Class<?> ZONED_DATE_TIME;

    /**
     * {@code Class<java.time.OffsetTime>} or {@code null}.
     */
    public static final Class<?> OFFSET_TIME;

    /**
     * {@code Class<java.time.Period>} or {@code null}.
     */
    public static final Class<?> PERIOD;

    /**
     * {@code Class<java.time.Duration>} or {@code null}.
     */
    public static final Class<?> DURATION;

    /**
     * Whether the JSR 310 date and time API present in the JRE.
     */
    public static final boolean PRESENT;

    static {
        boolean present = false;
        Class<?> localDate = null, localTime = null, localDateTime = null, instant = null, offsetDateTime = null,
                zonedDateTime = null, offsetTime = null, period = null, duration = null;
        try {
            localDate = Class.forName("java.time.LocalDate");
            localTime = Class.forName("java.time.LocalTime");
            localDateTime = Class.forName("java.time.LocalDateTime");
            instant = Class.forName("java.time.Instant");
            offsetDateTime = Class.forName("java.time.OffsetDateTime");
            zonedDateTime = Class.forName("java.time.ZonedDateTime");
            offsetTime = Class.forName("java.time.OffsetTime");
            period = Class.forName("java.time.Period");
            duration = Class.forName("java.time.Duration");
            present = true;
        } catch (Throwable t) {
            // Ignore
        }
        LOCAL_DATE = localDate;
        LOCAL_TIME = localTime;
        LOCAL_DATE_TIME = localDateTime;
        INSTANT = instant;
        OFFSET_DATE_TIME = offsetDateTime;
        ZONED_DATE_TIME = zonedDateTime;
        OFFSET_TIME = offsetTime;
        PERIOD = period;
        DURATION = duration;
        PRESENT = present;
    }

    private JSR310() {
    }

}
