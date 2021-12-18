/*
 * Copyright 2004-2021 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Locale;

import org.h2.api.ErrorCode;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.TypedValueExpression;
import org.h2.message.DbException;
import org.h2.util.JSR310Utils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueTimestampTimeZone;
import org.h2.value.ValueVarchar;

/**
 * A date-time format function.
 */
public final class DateTimeFormatFunction extends FunctionN {

    /**
     * FORMATDATETIME() (non-standard).
     */
    public static final int FORMATDATETIME = 0;

    /**
     * PARSEDATETIME() (non-standard).
     */
    public static final int PARSEDATETIME = FORMATDATETIME + 1;

    private static final String[] NAMES = { //
            "FORMATDATETIME", "PARSEDATETIME" //
    };

    private final int function;

    public DateTimeFormatFunction(int function) {
        super(new Expression[4]);
        this.function = function;
    }

    @Override
    public Value getValue(SessionLocal session, Value v1, Value v2, Value v3) {
        String format = v2.getString(), locale, tz;
        if (v3 != null) {
            locale = v3.getString();
            tz = args.length > 3 ? args[3].getValue(session).getString() : null;
        } else {
            tz = locale = null;
        }
        switch (function) {
        case FORMATDATETIME:
            v1 = ValueVarchar.get(formatDateTime(session, v1, format, locale, tz));
            break;
        case PARSEDATETIME:
            v1 = parseDateTime(session, v1.getString(), format, locale, tz);
            break;
        default:
            throw DbException.getInternalError("function=" + function);
        }
        return v1;
    }

    /**
     * Formats a date using a format string.
     *
     * @param session
     *            the session
     * @param date
     *            the date to format
     * @param format
     *            the format string
     * @param locale
     *            the locale
     * @param timeZone
     *            the time zone
     * @return the formatted date
     */
    public static String formatDateTime(SessionLocal session, Value date, String format, String locale,
            String timeZone) {
        ZoneId zoneId = timeZone != null ? getZoneId(timeZone) : null;
        TemporalAccessor value;
        if (date instanceof ValueTimestampTimeZone) {
            OffsetDateTime dateTime = JSR310Utils.valueToOffsetDateTime(date, session);
            ZoneId zoneToSet;
            if (zoneId != null) {
                zoneToSet = zoneId;
            } else {
                ZoneOffset offset = dateTime.getOffset();
                zoneToSet = ZoneId.ofOffset(offset.getTotalSeconds() == 0 ? "UTC" : "GMT", offset);
            }
            value = dateTime.atZoneSameInstant(zoneToSet);
        } else {
            LocalDateTime dateTime = JSR310Utils.valueToLocalDateTime(date, session);
            value = dateTime.atZone(zoneId != null ? zoneId : ZoneId.of(session.currentTimeZone().getId()));
        }
        return getDateFormat(format, locale, zoneId).format(value);
    }

    /**
     * Parses a date using a format string.
     *
     * @param session
     *            the session
     * @param date
     *            the date to parse
     * @param format
     *            the parsing format
     * @param locale
     *            the locale
     * @param timeZone
     *            the time zone
     * @return the parsed date
     */
    public static ValueTimestampTimeZone parseDateTime(SessionLocal session, String date, String format, String locale,
            String timeZone) {
        ZoneId zoneId = timeZone != null ? getZoneId(timeZone) : null;
        try {
            ValueTimestampTimeZone result;
            TemporalAccessor parsed = getDateFormat(format, locale, zoneId).parse(date);
            ZoneId parsedZoneId = parsed.query(TemporalQueries.zoneId());
            if (parsed.isSupported(ChronoField.OFFSET_SECONDS)) {
                result = JSR310Utils.offsetDateTimeToValue(OffsetDateTime.from(parsed));
            } else {
                if (parsed.isSupported(ChronoField.INSTANT_SECONDS)) {
                    Instant instant = Instant.from(parsed);
                    if (parsedZoneId == null && zoneId != null) {
                        parsedZoneId = zoneId;
                    }
                    if (parsedZoneId != null) {
                        result = JSR310Utils.zonedDateTimeToValue(instant.atZone(parsedZoneId));
                    } else {
                        result = JSR310Utils.offsetDateTimeToValue(instant.atOffset(ZoneOffset.ofTotalSeconds( //
                                session.currentTimeZone().getTimeZoneOffsetUTC(instant.getEpochSecond()))));
                    }
                } else {
                    LocalDate localDate = parsed.query(TemporalQueries.localDate());
                    LocalTime localTime = parsed.query(TemporalQueries.localTime());
                    LocalDateTime localDateTime = localTime != null ? LocalDateTime.of(localDate, localTime)
                            : localDate.atStartOfDay();
                    if (parsedZoneId == null && zoneId != null) {
                        parsedZoneId = zoneId;
                    }
                    result = parsedZoneId != null ? JSR310Utils.zonedDateTimeToValue(localDateTime.atZone(parsedZoneId))
                            : (ValueTimestampTimeZone) JSR310Utils.localDateTimeToValue(localDateTime)
                                    .convertTo(Value.TIMESTAMP_TZ, session);
                }
            }
            return result;
        } catch (RuntimeException e) {
            throw DbException.get(ErrorCode.PARSE_ERROR_1, e, date);
        }
    }

    private static DateTimeFormatter getDateFormat(String format, String locale, ZoneId zoneId) {
        if (format.length() > 100) {
            throw DbException.get(ErrorCode.PARSE_ERROR_1, format);
        }
        try {
            // currently, a new instance is create for each call
            // however, could cache the last few instances
            DateTimeFormatter df;
            if (locale == null) {
                df = DateTimeFormatter.ofPattern(format);
            } else {
                df = DateTimeFormatter.ofPattern(format, new Locale(locale));
            }
            if (zoneId != null) {
                df.withZone(zoneId);
            }
            return df;
        } catch (Exception e) {
            throw DbException.get(ErrorCode.PARSE_ERROR_1, e, format + '/' + locale);
        }
    }

    private static ZoneId getZoneId(String timeZone) {
        try {
            return ZoneId.of(timeZone, ZoneId.SHORT_IDS);
        } catch (RuntimeException e) {
            throw DbException.getInvalidValueException("TIME ZONE", timeZone);
        }
    }

    @Override
    public Expression optimize(SessionLocal session) {
        boolean allConst = optimizeArguments(session, true);
        switch (function) {
        case FORMATDATETIME:
            type = TypeInfo.TYPE_VARCHAR;
            break;
        case PARSEDATETIME:
            type = TypeInfo.TYPE_TIMESTAMP_TZ;
            break;
        default:
            throw DbException.getInternalError("function=" + function);
        }
        if (allConst) {
            return TypedValueExpression.getTypedIfNull(getValue(session), type);
        }
        return this;
    }

    @Override
    public String getName() {
        return NAMES[function];
    }

}
