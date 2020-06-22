/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import static org.h2.util.DateTimeUtils.MILLIS_PER_DAY;
import static org.h2.util.DateTimeUtils.NANOS_PER_DAY;
import static org.h2.util.DateTimeUtils.NANOS_PER_HOUR;
import static org.h2.util.DateTimeUtils.NANOS_PER_MINUTE;
import static org.h2.util.DateTimeUtils.NANOS_PER_SECOND;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DateFormatSymbols;
import java.text.SimpleDateFormat;
import java.time.temporal.WeekFields;
import java.util.Locale;
import java.util.TimeZone;

import org.h2.api.ErrorCode;
import org.h2.api.IntervalQualifier;
import org.h2.engine.Mode.ModeEnum;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.util.DateTimeUtils;
import org.h2.util.IntervalUtils;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueInteger;
import org.h2.value.ValueInterval;
import org.h2.value.ValueNumeric;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimeTimeZone;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;

/**
 * Date and time functions.
 */
public final class DateTimeFunctions {

    // Standard fields

    /**
     * Year.
     */
    public static final int YEAR = 0;

    /**
     * Month.
     */
    public static final int MONTH = YEAR + 1;

    /**
     * Day of month.
     */
    public static final int DAY = MONTH + 1;

    /**
     * Hour.
     */
    public static final int HOUR = DAY + 1;

    /**
     * Minute.
     */
    public static final int MINUTE = HOUR + 1;

    /**
     * Second.
     */
    public static final int SECOND = MINUTE + 1;

    /**
     * Time zone hour.
     */
    public static final int TIMEZONE_HOUR = SECOND + 1;

    /**
     * Time zone minute.
     */
    public static final int TIMEZONE_MINUTE = TIMEZONE_HOUR + 1;

    // Additional fields

    /**
     * Time zone second.
     */
    public static final int TIMEZONE_SECOND = TIMEZONE_MINUTE + 1;

    /**
     * Millennium.
     */
    public static final int MILLENNIUM = TIMEZONE_SECOND + 1;

    /**
     * Century.
     */
    public static final int CENTURY = MILLENNIUM + 1;

    /**
     * Decade.
     */
    public static final int DECADE = CENTURY + 1;

    /**
     * Quarter.
     */
    public static final int QUARTER = DECADE + 1;

    /**
     * Millisecond.
     */
    public static final int MILLISECOND = QUARTER + 1;

    /**
     * Microsecond.
     */
    public static final int MICROSECOND = MILLISECOND + 1;

    /**
     * Nanosecond.
     */
    public static final int NANOSECOND = MICROSECOND + 1;

    /**
     * Day of year.
     */
    public static final int DAY_OF_YEAR = NANOSECOND + 1;

    /**
     * ISO day of week.
     */
    public static final int ISO_DAY_OF_WEEK = DAY_OF_YEAR + 1;

    /**
     * ISO week.
     */
    public static final int ISO_WEEK = ISO_DAY_OF_WEEK + 1;

    /**
     * ISO week-based year.
     */
    public static final int ISO_WEEK_YEAR = ISO_WEEK + 1;

    /**
     * Day of week (locale-specific).
     */
    public static final int DAY_OF_WEEK = ISO_WEEK_YEAR + 1;

    /**
     * Week (locale-specific).
     */
    public static final int WEEK = DAY_OF_WEEK + 1;

    /**
     * Week-based year (locale-specific).
     */
    public static final int WEEK_YEAR = WEEK + 1;

    /**
     * Epoch.
     */
    public static final int EPOCH = WEEK_YEAR + 1;

    /**
     * Day of week (locale-specific) for PostgreSQL compatibility.
     */
    public static final int DOW = EPOCH + 1;

    private static final int FIELDS_COUNT = DOW + 1;

    private static final String[] NAMES = { //
            "YEAR", "MONTH", "DAY", //
            "HOUR", "MINUTE", "SECOND", //
            "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TIMEZONE_SECOND", //
            "MILLENNIUM", "CENTURY", "DECADE", //
            "QUARTER", //
            "MILLISECOND", "MICROSECOND", "NANOSECOND", //
            "DAY_OF_YEAR", //
            "ISO_DAY_OF_WEEK", "ISO_WEEK", "ISO_WEEK_YEAR", //
            "DAY_OF_WEEK", "WEEK", "WEEK_YEAR", //
            "EPOCH", "DOW", //
    };

    private static final BigDecimal BD_SECONDS_PER_DAY = new BigDecimal(DateTimeUtils.SECONDS_PER_DAY);

    private static final BigInteger BI_SECONDS_PER_DAY = BigInteger.valueOf(DateTimeUtils.SECONDS_PER_DAY);

    private static final BigDecimal BD_NANOS_PER_SECOND = new BigDecimal(NANOS_PER_SECOND);

    /**
     * Local definitions of day-of-week, week-of-month, and week-of-year.
     */
    private static volatile WeekFields WEEK_FIELDS;

    /**
     * English names of months and week days.
     */
    private static volatile String[][] MONTHS_AND_WEEKS;

    /**
     * Get date-time field for the specified name.
     *
     * @param name
     *            the name
     * @return the date-time field
     * @throws DbException
     *             on unknown field name
     */
    public static int getField(String name) {
        switch (StringUtils.toUpperEnglish(name)) {
        case "YEAR":
        case "YY":
        case "YYYY":
        case "SQL_TSI_YEAR":
            return YEAR;
        case "MONTH":
        case "M":
        case "MM":
        case "SQL_TSI_MONTH":
            return MONTH;
        case "DAY":
        case "D":
        case "DD":
        case "SQL_TSI_DAY":
            return DAY;
        case "HOUR":
        case "HH":
        case "SQL_TSI_HOUR":
            return HOUR;
        case "MINUTE":
        case "MI":
        case "N":
        case "SQL_TSI_MINUTE":
            return MINUTE;
        case "SECOND":
        case "S":
        case "SS":
        case "SQL_TSI_SECOND":
            return SECOND;
        case "TIMEZONE_HOUR":
            return TIMEZONE_HOUR;
        case "TIMEZONE_MINUTE":
            return TIMEZONE_MINUTE;
        case "TIMEZONE_SECOND":
            return TIMEZONE_SECOND;
        case "MILLENNIUM":
            return MILLENNIUM;
        case "CENTURY":
            return CENTURY;
        case "DECADE":
            return DECADE;
        case "QUARTER":
            return QUARTER;
        case "MILLISECOND":
        case "MILLISECONDS":
        case "MS":
            return MILLISECOND;
        case "MICROSECOND":
        case "MICROSECONDS":
        case "MCS":
            return MICROSECOND;
        case "NANOSECOND":
        case "NS":
            return NANOSECOND;
        case "DAY_OF_YEAR":
        case "DAYOFYEAR":
        case "DY":
        case "DOY":
            return DAY_OF_YEAR;
        case "ISO_DAY_OF_WEEK":
        case "ISODOW":
            return ISO_DAY_OF_WEEK;
        case "ISO_WEEK":
            return ISO_WEEK;
        case "ISO_WEEK_YEAR":
        case "ISO_YEAR":
        case "ISOYEAR":
            return ISO_WEEK_YEAR;
        case "DAY_OF_WEEK":
        case "DAYOFWEEK":
            return DAY_OF_WEEK;
        case "WEEK":
        case "WK":
        case "WW":
        case "SQL_TSI_WEEK":
            return WEEK;
        case "WEEK_YEAR":
            return WEEK_YEAR;
        case "EPOCH":
            return EPOCH;
        case "DOW":
            return DOW;
        default:
            throw DbException.getInvalidValueException("date-time field", name);
        }
    }

    /**
     * Get the name of the specified date-time field.
     * @param field the date-time field
     * @return the name of the specified field
     */
    public static String getFieldName(int field) {
        if (field < 0 || field >= FIELDS_COUNT) {
            throw DbException.getUnsupportedException("datetime field " + field);
        }
        return NAMES[field];
    }

    /**
     * DATEADD function.
     *
     * @param session
     *            the session
     * @param field
     *            the date-time field
     * @param count
     *            count to add
     * @param v
     *            value to add to
     * @return result
     */
    public static Value dateadd(Session session, int field, long count, Value v) {
        if (field != MILLISECOND && field != MICROSECOND && field != NANOSECOND
                && (count > Integer.MAX_VALUE || count < Integer.MIN_VALUE)) {
            throw DbException.getInvalidValueException("DATEADD count", count);
        }
        long[] a = DateTimeUtils.dateAndTimeFromValue(v, session);
        long dateValue = a[0];
        long timeNanos = a[1];
        int type = v.getValueType();
        switch (field) {
        case MILLENNIUM:
            return addYearsMonths(field, true, count * 1_000, v, type, dateValue, timeNanos);
        case CENTURY:
            return addYearsMonths(field, true, count * 100, v, type, dateValue, timeNanos);
        case DECADE:
            return addYearsMonths(field, true, count * 10, v, type, dateValue, timeNanos);
        case YEAR:
            return addYearsMonths(field, true, count, v, type, dateValue, timeNanos);
        case QUARTER:
            return addYearsMonths(field, false, count *= 3, v, type, dateValue, timeNanos);
        case MONTH:
            return addYearsMonths(field, false, count, v, type, dateValue, timeNanos);
        case WEEK:
        case ISO_WEEK:
            count *= 7;
            //$FALL-THROUGH$
        case DAY_OF_WEEK:
        case DOW:
        case ISO_DAY_OF_WEEK:
        case DAY:
        case DAY_OF_YEAR:
            if (type == Value.TIME || type == Value.TIME_TZ) {
                throw DbException.getInvalidValueException("DATEADD time part", getFieldName(field));
            }
            dateValue = DateTimeUtils
                    .dateValueFromAbsoluteDay(DateTimeUtils.absoluteDayFromDateValue(dateValue) + count);
            return DateTimeUtils.dateTimeToValue(v, dateValue, timeNanos);
        case HOUR:
            count *= NANOS_PER_HOUR;
            break;
        case MINUTE:
            count *= NANOS_PER_MINUTE;
            break;
        case SECOND:
        case EPOCH:
            count *= NANOS_PER_SECOND;
            break;
        case MILLISECOND:
            count *= 1_000_000;
            break;
        case MICROSECOND:
            count *= 1_000;
            break;
        case NANOSECOND:
            break;
        case TIMEZONE_HOUR:
            return addToTimeZone(field, count * 3_600, v, type, dateValue, timeNanos);
        case TIMEZONE_MINUTE:
            return addToTimeZone(field, count * 60, v, type, dateValue, timeNanos);
        case TIMEZONE_SECOND:
            return addToTimeZone(field, count, v, type, dateValue, timeNanos);
        default:
            throw DbException.getUnsupportedException("DATEADD " + getFieldName(field));
        }
        timeNanos += count;
        if (timeNanos >= NANOS_PER_DAY || timeNanos < 0) {
            long d;
            if (timeNanos >= NANOS_PER_DAY) {
                d = timeNanos / NANOS_PER_DAY;
            } else {
                d = (timeNanos - NANOS_PER_DAY + 1) / NANOS_PER_DAY;
            }
            dateValue = DateTimeUtils.dateValueFromAbsoluteDay(DateTimeUtils.absoluteDayFromDateValue(dateValue) + d);
            timeNanos -= d * NANOS_PER_DAY;
        }
        if (type == Value.DATE) {
            return ValueTimestamp.fromDateValueAndNanos(dateValue, timeNanos);
        }
        return DateTimeUtils.dateTimeToValue(v, dateValue, timeNanos);
    }

    private static Value addYearsMonths(int field, boolean years, long count, Value v, int type, long dateValue,
            long timeNanos) {
        if (type == Value.TIME || type == Value.TIME_TZ) {
            throw DbException.getInvalidValueException("DATEADD time part", getFieldName(field));
        }
        long year = DateTimeUtils.yearFromDateValue(dateValue);
        long month = DateTimeUtils.monthFromDateValue(dateValue);
        if (years) {
            year += count;
        } else {
            month += count;
        }
        return DateTimeUtils.dateTimeToValue(v, DateTimeUtils.dateValueFromDenormalizedDate(year, month,
                DateTimeUtils.dayFromDateValue(dateValue)), timeNanos);
    }

    private static Value addToTimeZone(int field, long count, Value v, int type, long dateValue, long timeNanos) {
        if (type == Value.TIMESTAMP_TZ) {
            return ValueTimestampTimeZone.fromDateValueAndNanos(dateValue, timeNanos,
                    MathUtils.convertLongToInt(count + ((ValueTimestampTimeZone) v).getTimeZoneOffsetSeconds()));
        } else if (type == Value.TIME_TZ) {
            return ValueTimeTimeZone.fromNanos(timeNanos,
                    MathUtils.convertLongToInt(count + ((ValueTimeTimeZone) v).getTimeZoneOffsetSeconds()));
        } else {
            throw DbException.getUnsupportedException("DATEADD " + getFieldName(field));
        }
    }

    /**
     * Calculate the number of crossed unit boundaries between two timestamps. This
     * method is supported for MS SQL Server compatibility.
     *
     * <pre>
     * DATEDIFF(YEAR, '2004-12-31', '2005-01-01') = 1
     * </pre>
     *
     * @param session
     *            the session
     * @param field
     *            the date-time field
     * @param v1
     *            the first date-time value
     * @param v2
     *            the second date-time value
     * @return the number of crossed boundaries
     */
    public static long datediff(Session session, int field, Value v1, Value v2) {
        long[] a1 = DateTimeUtils.dateAndTimeFromValue(v1, session);
        long dateValue1 = a1[0];
        long absolute1 = DateTimeUtils.absoluteDayFromDateValue(dateValue1);
        long[] a2 = DateTimeUtils.dateAndTimeFromValue(v2, session);
        long dateValue2 = a2[0];
        long absolute2 = DateTimeUtils.absoluteDayFromDateValue(dateValue2);
        switch (field) {
        case NANOSECOND:
        case MICROSECOND:
        case MILLISECOND:
        case SECOND:
        case EPOCH:
        case MINUTE:
        case HOUR:
            long timeNanos1 = a1[1];
            long timeNanos2 = a2[1];
            switch (field) {
            case NANOSECOND:
                return (absolute2 - absolute1) * NANOS_PER_DAY + (timeNanos2 - timeNanos1);
            case MICROSECOND:
                return (absolute2 - absolute1) * (MILLIS_PER_DAY * 1_000)
                        + (timeNanos2 / 1_000 - timeNanos1 / 1_000);
            case MILLISECOND:
                return (absolute2 - absolute1) * MILLIS_PER_DAY
                        + (timeNanos2 / 1_000_000 - timeNanos1 / 1_000_000);
            case SECOND:
            case EPOCH:
                return (absolute2 - absolute1) * 86_400
                        + (timeNanos2 / NANOS_PER_SECOND - timeNanos1 / NANOS_PER_SECOND);
            case MINUTE:
                return (absolute2 - absolute1) * 1_440
                        + (timeNanos2 / NANOS_PER_MINUTE - timeNanos1 / NANOS_PER_MINUTE);
            case HOUR:
                return (absolute2 - absolute1) * 24 + (timeNanos2 / NANOS_PER_HOUR - timeNanos1 / NANOS_PER_HOUR);
            }
            // Fake fall-through
            // $FALL-THROUGH$
        case DAY:
        case DAY_OF_YEAR:
        case DAY_OF_WEEK:
        case DOW:
        case ISO_DAY_OF_WEEK:
            return absolute2 - absolute1;
        case WEEK:
            return weekdiff(absolute1, absolute2, getWeekFields().getFirstDayOfWeek().getValue());
        case ISO_WEEK:
            return weekdiff(absolute1, absolute2, 1);
        case MONTH:
            return (DateTimeUtils.yearFromDateValue(dateValue2) - DateTimeUtils.yearFromDateValue(dateValue1)) * 12
                    + DateTimeUtils.monthFromDateValue(dateValue2) - DateTimeUtils.monthFromDateValue(dateValue1);
        case QUARTER:
            return (DateTimeUtils.yearFromDateValue(dateValue2) - DateTimeUtils.yearFromDateValue(dateValue1)) * 4
                    + (DateTimeUtils.monthFromDateValue(dateValue2) - 1) / 3
                    - (DateTimeUtils.monthFromDateValue(dateValue1) - 1) / 3;
        case MILLENNIUM:
            return millennium(DateTimeUtils.yearFromDateValue(dateValue2))
                    - millennium(DateTimeUtils.yearFromDateValue(dateValue1));
        case CENTURY:
            return century(DateTimeUtils.yearFromDateValue(dateValue2))
                    - century(DateTimeUtils.yearFromDateValue(dateValue1));
        case DECADE:
            return decade(DateTimeUtils.yearFromDateValue(dateValue2))
                    - decade(DateTimeUtils.yearFromDateValue(dateValue1));
        case YEAR:
            return DateTimeUtils.yearFromDateValue(dateValue2) - DateTimeUtils.yearFromDateValue(dateValue1);
        case TIMEZONE_HOUR:
        case TIMEZONE_MINUTE:
        case TIMEZONE_SECOND: {
            int offsetSeconds1;
            if (v1 instanceof ValueTimestampTimeZone) {
                offsetSeconds1 = ((ValueTimestampTimeZone) v1).getTimeZoneOffsetSeconds();
            } else if (v1 instanceof ValueTimeTimeZone) {
                offsetSeconds1 = ((ValueTimeTimeZone) v1).getTimeZoneOffsetSeconds();
            } else {
                offsetSeconds1 = session.currentTimeZone().getTimeZoneOffsetLocal(dateValue1, a1[1]);
            }
            int offsetSeconds2;
            if (v2 instanceof ValueTimestampTimeZone) {
                offsetSeconds2 = ((ValueTimestampTimeZone) v2).getTimeZoneOffsetSeconds();
            } else if (v2 instanceof ValueTimeTimeZone) {
                offsetSeconds2 = ((ValueTimeTimeZone) v2).getTimeZoneOffsetSeconds();
            } else {
                offsetSeconds2 = session.currentTimeZone().getTimeZoneOffsetLocal(dateValue2, a2[1]);
            }
            if (field == TIMEZONE_HOUR) {
                return (offsetSeconds2 / 3_600) - (offsetSeconds1 / 3_600);
            } else if (field == TIMEZONE_MINUTE) {
                return (offsetSeconds2 / 60) - (offsetSeconds1 / 60);
            } else {
                return offsetSeconds2 - offsetSeconds1;
            }
        }
        default:
            throw DbException.getUnsupportedException("DATEDIFF " + getFieldName(field));
        }
    }

    /**
     * Extracts specified field from the specified date-time value.
     *
     * @param session
     *            the session
     * @param field
     *            the date-time field
     * @param value
     *            the date-time value
     * @return extracted field
     */
    public static Value extract(Session session, int field, Value value) {
        Value result;
        if (field != EPOCH) {
            result = ValueInteger.get(extractIntegerField(session, value, field));
        } else {
            // Case where we retrieve the EPOCH time.
            if (value instanceof ValueInterval) {
                ValueInterval interval = (ValueInterval) value;
                if (interval.getQualifier().isYearMonth()) {
                    interval = (ValueInterval) interval.convertTo(TypeInfo.TYPE_INTERVAL_YEAR_TO_MONTH);
                    long leading = interval.getLeading();
                    long remaining = interval.getRemaining();
                    BigInteger bi = BigInteger.valueOf(leading).multiply(BigInteger.valueOf(31557600))
                            .add(BigInteger.valueOf(remaining * 2592000));
                    if (interval.isNegative()) {
                        bi = bi.negate();
                    }
                    return ValueNumeric.get(bi);
                } else {
                    return ValueNumeric.get(new BigDecimal(IntervalUtils.intervalToAbsolute(interval))
                            .divide(BD_NANOS_PER_SECOND));
                }
            }
            // First we retrieve the dateValue and his time in nanoseconds.
            long[] a = DateTimeUtils.dateAndTimeFromValue(value, session);
            long dateValue = a[0];
            long timeNanos = a[1];
            // We compute the time in nanoseconds and the total number of days.
            // Case where the value is of type time e.g. '10:00:00'
            if (value instanceof ValueTime) {
                // In order to retrieve the EPOCH time we only have to convert the time
                // in nanoseconds (previously retrieved) in seconds.
                result = ValueNumeric.get(BigDecimal.valueOf(timeNanos).divide(BD_NANOS_PER_SECOND));
            } else if (value instanceof ValueDate) {
                // Case where the value is of type date '2000:01:01', we have to retrieve the
                // total number of days and multiply it by the number of seconds in a day.
                result = ValueNumeric.get(BigInteger.valueOf(DateTimeUtils.absoluteDayFromDateValue(dateValue))
                        .multiply(BI_SECONDS_PER_DAY));
            } else {
                BigDecimal bd = BigDecimal.valueOf(timeNanos).divide(BD_NANOS_PER_SECOND).add(BigDecimal
                        .valueOf(DateTimeUtils.absoluteDayFromDateValue(dateValue)).multiply(BD_SECONDS_PER_DAY));
                if (value instanceof ValueTimestampTimeZone) {
                    // Case where the value is a of type ValueTimestampTimeZone
                    // ('2000:01:01 10:00:00+05').
                    // We retrieve the time zone offset in seconds
                    // Sum the time in nanoseconds and the total number of days in seconds
                    // and adding the timeZone offset in seconds.
                    result = ValueNumeric.get(bd.subtract(
                            BigDecimal.valueOf(((ValueTimestampTimeZone) value).getTimeZoneOffsetSeconds())));
                } else if (value instanceof ValueTimeTimeZone) {
                    result = ValueNumeric.get(bd.subtract(
                            BigDecimal.valueOf(((ValueTimeTimeZone) value).getTimeZoneOffsetSeconds())));
                } else {
                    // By default, we have the date and the time ('2000:01:01 10:00:00') if no type
                    // is given.
                    // We just have to sum the time in nanoseconds and the total number of days in
                    // seconds.
                    result = ValueNumeric.get(bd);
                }
            }
        }
        return result;
    }

    /**
     * Truncate the given date-time value to the specified field.
     *
     * @param session the session
     * @param field the date-time field
     * @param value the date-time value
     * @return date the truncated value
     */
    public static Value truncateDate(Session session, int field, Value value) {
        long[] fieldDateAndTime = DateTimeUtils.dateAndTimeFromValue(value, session);
        long dateValue = fieldDateAndTime[0];
        long timeNanos = fieldDateAndTime[1];
        switch (field) {
        case MICROSECOND:
            timeNanos = timeNanos / 1_000L * 1_000L;
            break;
        case MILLISECOND:
            timeNanos = timeNanos / 1_000_000L * 1_000_000L;
            break;
        case SECOND:
            timeNanos = timeNanos / NANOS_PER_SECOND * NANOS_PER_SECOND;
            break;
        case MINUTE:
            timeNanos = timeNanos / NANOS_PER_MINUTE * NANOS_PER_MINUTE;
            break;
        case HOUR:
            timeNanos = timeNanos / NANOS_PER_HOUR * NANOS_PER_HOUR;
            break;
        case DAY:
            timeNanos = 0L;
            break;
        case ISO_WEEK:
            dateValue = truncateToWeek(dateValue, 1);
            timeNanos = 0L;
            break;
        case WEEK:
            dateValue = truncateToWeek(dateValue, getWeekFields().getFirstDayOfWeek().getValue());
            timeNanos = 0L;
            break;
        case ISO_WEEK_YEAR:
            dateValue = truncateToWeekYear(dateValue, 1, 4);
            timeNanos = 0L;
            break;
        case WEEK_YEAR: {
            WeekFields weekFields = getWeekFields();
            dateValue = truncateToWeekYear(dateValue, weekFields.getFirstDayOfWeek().getValue(),
                    weekFields.getMinimalDaysInFirstWeek());
            break;
        }
        case MONTH:
            dateValue = dateValue & (-1L << DateTimeUtils.SHIFT_MONTH) | 1L;
            timeNanos = 0L;
            break;
        case QUARTER:
            dateValue = DateTimeUtils.dateValue(DateTimeUtils.yearFromDateValue(dateValue),
                    ((DateTimeUtils.monthFromDateValue(dateValue) - 1) / 3) * 3 + 1, 1);
            timeNanos = 0L;
            break;
        case YEAR:
            dateValue = dateValue & (-1L << DateTimeUtils.SHIFT_YEAR) | (1L << DateTimeUtils.SHIFT_MONTH | 1L);
            timeNanos = 0L;
            break;
        case DECADE: {
            int year = DateTimeUtils.yearFromDateValue(dateValue);
            if (year >= 0) {
                year = year / 10 * 10;
            } else {
                year = (year - 9) / 10 * 10;
            }
            dateValue = DateTimeUtils.dateValue(year, 1, 1);
            timeNanos = 0L;
            break;
        }
        case CENTURY: {
            int year = DateTimeUtils.yearFromDateValue(dateValue);
            if (year > 0) {
                year = (year - 1) / 100 * 100 + 1;
            } else {
                year = year / 100 * 100 - 99;
            }
            dateValue = DateTimeUtils.dateValue(year, 1, 1);
            timeNanos = 0L;
            break;
        }
        case MILLENNIUM: {
            int year = DateTimeUtils.yearFromDateValue(dateValue);
            if (year > 0) {
                year = (year - 1) / 1000 * 1000 + 1;
            } else {
                year = year / 1000 * 1000 - 999;
            }
            dateValue = DateTimeUtils.dateValue(year, 1, 1);
            timeNanos = 0L;
            break;
        }
        default:
            throw DbException.getUnsupportedException("DATE_TRUNC " + getFieldName(field));
        }
        Value result = DateTimeUtils.dateTimeToValue(value, dateValue, timeNanos);
        if (session.getMode().getEnum() == ModeEnum.PostgreSQL && result.getValueType() == Value.DATE) {
            result = result.convertTo(Value.TIMESTAMP_TZ, session);
        }
        return result;
    }

    private static long truncateToWeek(long dateValue, int firstDayOfWeek) {
        long absoluteDay = DateTimeUtils.absoluteDayFromDateValue(dateValue);
        int dayOfWeek = DateTimeUtils.getDayOfWeekFromAbsolute(absoluteDay, firstDayOfWeek);
        if (dayOfWeek != 1) {
            dateValue = DateTimeUtils.dateValueFromAbsoluteDay(absoluteDay - dayOfWeek + 1);
        }
        return dateValue;
    }

    private static long truncateToWeekYear(long dateValue, int firstDayOfWeek, int minimalDaysInFirstWeek) {
        long abs = DateTimeUtils.absoluteDayFromDateValue(dateValue);
        int year = DateTimeUtils.yearFromDateValue(dateValue);
        long base = DateTimeUtils.getWeekYearAbsoluteStart(year, firstDayOfWeek, minimalDaysInFirstWeek);
        if (abs < base) {
            base = DateTimeUtils.getWeekYearAbsoluteStart(year - 1, firstDayOfWeek, minimalDaysInFirstWeek);
        } else if (DateTimeUtils.monthFromDateValue(dateValue) == 12
                && 24 + minimalDaysInFirstWeek < DateTimeUtils.dayFromDateValue(dateValue)) {
            long next = DateTimeUtils.getWeekYearAbsoluteStart(year + 1, firstDayOfWeek, minimalDaysInFirstWeek);
            if (abs >= next) {
                base = next;
            }
        }
        return DateTimeUtils.dateValueFromAbsoluteDay(base);
    }

    /**
     * Formats a date using a format string.
     *
     * @param date
     *            the date to format
     * @param format
     *            the format string
     * @param locale
     *            the locale
     * @param timeZone
     *            the timezone
     * @return the formatted date
     */
    public static String formatDateTime(java.util.Date date, String format, String locale, String timeZone) {
        SimpleDateFormat dateFormat = getDateFormat(format, locale, timeZone);
        synchronized (dateFormat) {
            return dateFormat.format(date);
        }
    }

    private static SimpleDateFormat getDateFormat(String format, String locale, String timeZone) {
        try {
            // currently, a new instance is create for each call
            // however, could cache the last few instances
            SimpleDateFormat df;
            if (locale == null) {
                df = new SimpleDateFormat(format);
            } else {
                Locale l = new Locale(locale);
                df = new SimpleDateFormat(format, l);
            }
            if (timeZone != null) {
                df.setTimeZone(TimeZone.getTimeZone(timeZone));
            }
            return df;
        } catch (Exception e) {
            throw DbException.get(ErrorCode.PARSE_ERROR_1, e, format + "/" + locale + "/" + timeZone);
        }
    }

    /**
     * Get the specified field of a date, however with years normalized to positive
     * or negative, and month starting with 1.
     *
     * @param session
     *            the session
     * @param date
     *            the date value
     * @param field
     *            the field type, see {@link Function} for constants
     * @return the value
     */
    public static int extractIntegerField(Session session, Value date, int field) {
        if (date instanceof ValueInterval) {
            ValueInterval interval = (ValueInterval) date;
            IntervalQualifier qualifier = interval.getQualifier();
            boolean negative = interval.isNegative();
            long leading = interval.getLeading(), remaining = interval.getRemaining();
            long v;
            switch (field) {
            case YEAR:
                v = IntervalUtils.yearsFromInterval(qualifier, negative, leading, remaining);
                break;
            case MONTH:
                v = IntervalUtils.monthsFromInterval(qualifier, negative, leading, remaining);
                break;
            case DAY:
            case DAY_OF_YEAR:
                v = IntervalUtils.daysFromInterval(qualifier, negative, leading, remaining);
                break;
            case HOUR:
                v = IntervalUtils.hoursFromInterval(qualifier, negative, leading, remaining);
                break;
            case MINUTE:
                v = IntervalUtils.minutesFromInterval(qualifier, negative, leading, remaining);
                break;
            case SECOND:
                v = IntervalUtils.nanosFromInterval(qualifier, negative, leading, remaining) / NANOS_PER_SECOND;
                break;
            case MILLISECOND:
                v = IntervalUtils.nanosFromInterval(qualifier, negative, leading, remaining) / 1_000_000 % 1_000;
                break;
            case MICROSECOND:
                v = IntervalUtils.nanosFromInterval(qualifier, negative, leading, remaining) / 1_000 % 1_000_000;
                break;
            case NANOSECOND:
                v = IntervalUtils.nanosFromInterval(qualifier, negative, leading, remaining) % NANOS_PER_SECOND;
                break;
            default:
                throw DbException.getUnsupportedException("getDatePart(" + date + ", " + field + ')');
            }
            return (int) v;
        } else {
            long[] a = DateTimeUtils.dateAndTimeFromValue(date, session);
            long dateValue = a[0];
            long timeNanos = a[1];
            switch (field) {
            case YEAR:
                return DateTimeUtils.yearFromDateValue(dateValue);
            case MONTH:
                return DateTimeUtils.monthFromDateValue(dateValue);
            case DAY:
                return DateTimeUtils.dayFromDateValue(dateValue);
            case HOUR:
                return (int) (timeNanos / NANOS_PER_HOUR % 24);
            case MINUTE:
                return (int) (timeNanos / NANOS_PER_MINUTE % 60);
            case SECOND:
                return (int) (timeNanos / NANOS_PER_SECOND % 60);
            case MILLISECOND:
                return (int) (timeNanos / 1_000_000 % 1_000);
            case MICROSECOND:
                return (int) (timeNanos / 1_000 % 1_000_000);
            case NANOSECOND:
                return (int) (timeNanos % NANOS_PER_SECOND);
            case MILLENNIUM:
                return millennium(DateTimeUtils.yearFromDateValue(dateValue));
            case CENTURY:
                return century(DateTimeUtils.yearFromDateValue(dateValue));
            case DECADE:
                return decade(DateTimeUtils.yearFromDateValue(dateValue));
            case DAY_OF_YEAR:
                return DateTimeUtils.getDayOfYear(dateValue);
            case DOW:
                if (session.getMode().getEnum() == ModeEnum.PostgreSQL) {
                    return DateTimeUtils.getSundayDayOfWeek(dateValue) - 1;
                }
                //$FALL-THROUGH$
            case DAY_OF_WEEK:
                return getLocalDayOfWeek(dateValue);
            case WEEK:
                return getLocalWeekOfYear(dateValue);
            case WEEK_YEAR: {
                WeekFields wf = getWeekFields();
                return DateTimeUtils.getWeekYear(dateValue, wf.getFirstDayOfWeek().getValue(),
                        wf.getMinimalDaysInFirstWeek());
            }
            case QUARTER:
                return (DateTimeUtils.monthFromDateValue(dateValue) - 1) / 3 + 1;
            case ISO_WEEK_YEAR:
                return DateTimeUtils.getIsoWeekYear(dateValue);
            case ISO_WEEK:
                return DateTimeUtils.getIsoWeekOfYear(dateValue);
            case ISO_DAY_OF_WEEK:
                return DateTimeUtils.getIsoDayOfWeek(dateValue);
            case TIMEZONE_HOUR:
            case TIMEZONE_MINUTE:
            case TIMEZONE_SECOND: {
                int offsetSeconds;
                if (date instanceof ValueTimestampTimeZone) {
                    offsetSeconds = ((ValueTimestampTimeZone) date).getTimeZoneOffsetSeconds();
                } else if (date instanceof ValueTimeTimeZone) {
                    offsetSeconds = ((ValueTimeTimeZone) date).getTimeZoneOffsetSeconds();
                } else {
                    offsetSeconds = session.currentTimeZone().getTimeZoneOffsetLocal(dateValue, timeNanos);
                }
                if (field == TIMEZONE_HOUR) {
                    return offsetSeconds / 3_600;
                } else if (field == TIMEZONE_MINUTE) {
                    return offsetSeconds % 3_600 / 60;
                } else {
                    return offsetSeconds % 60;
                }
            }
            }
        }
        throw DbException.getUnsupportedException("EXTRACT(" + getFieldName(field) + " FROM " + date + ')');
    }

    private static int millennium(int year) {
        return year > 0 ? (year + 999) / 1_000 : year / 1_000;
    }

    private static int century(int year) {
        return year > 0 ? (year + 99) / 100 : year / 100;
    }

    private static int decade(int year) {
        return year >= 0 ? year / 10 : (year - 9) / 10;
    }

    private static int getLocalDayOfWeek(long dateValue) {
        return DateTimeUtils.getDayOfWeek(dateValue, getWeekFields().getFirstDayOfWeek().getValue());
    }

    private static int getLocalWeekOfYear(long dateValue) {
        WeekFields weekFields = getWeekFields();
        return DateTimeUtils.getWeekOfYear(dateValue, weekFields.getFirstDayOfWeek().getValue(),
                weekFields.getMinimalDaysInFirstWeek());
    }

    private static WeekFields getWeekFields() {
        WeekFields weekFields = WEEK_FIELDS;
        if (weekFields == null) {
            WEEK_FIELDS = weekFields = WeekFields.of(Locale.getDefault());
        }
        return weekFields;
    }

    /**
     * Return names of month or weeks.
     *
     * @param field
     *            0 for months, 1 for weekdays
     * @return names of month or weeks
     */
    public static String[] getMonthsAndWeeks(int field) {
        String[][] result = MONTHS_AND_WEEKS;
        if (result == null) {
            result = new String[2][];
            DateFormatSymbols dfs = DateFormatSymbols.getInstance(Locale.ENGLISH);
            result[0] = dfs.getMonths();
            result[1] = dfs.getWeekdays();
            MONTHS_AND_WEEKS = result;
        }
        return result[field];
    }

    /**
     * Parses a date using a format string.
     *
     * @param date
     *            the date to parse
     * @param format
     *            the parsing format
     * @param locale
     *            the locale
     * @param timeZone
     *            the timeZone
     * @return the parsed date
     */
    public static java.util.Date parseDateTime(String date, String format, String locale, String timeZone) {
        SimpleDateFormat dateFormat = getDateFormat(format, locale, timeZone);
        try {
            synchronized (dateFormat) {
                return dateFormat.parse(date);
            }
        } catch (Exception e) {
            // ParseException
            throw DbException.get(ErrorCode.PARSE_ERROR_1, e, date);
        }
    }

    private static long weekdiff(long absolute1, long absolute2, int firstDayOfWeek) {
        absolute1 += 4 - firstDayOfWeek;
        long r1 = absolute1 / 7;
        if (absolute1 < 0 && (r1 * 7 != absolute1)) {
            r1--;
        }
        absolute2 += 4 - firstDayOfWeek;
        long r2 = absolute2 / 7;
        if (absolute2 < 0 && (r2 * 7 != absolute2)) {
            r2--;
        }
        return r2 - r1;
    }

    private DateTimeFunctions() {
    }
}
