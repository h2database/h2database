/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: Jason Brittain (jason.brittain at gmail.com)
 */
package org.h2.mode;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;

import org.h2.api.ErrorCode;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ValueExpression;
import org.h2.message.DbException;
import org.h2.util.DateTimeUtils;
import org.h2.util.StringUtils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;
import org.h2.value.ValueVarchar;

/**
 * This class implements some MySQL-specific functions.
 *
 * @author Jason Brittain
 * @author Thomas Mueller
 */
public final class FunctionsMySQL extends ModeFunction {

    private static final int UNIX_TIMESTAMP = 1001, FROM_UNIXTIME = 1002, DATE = 1003, LAST_INSERT_ID = 1004;

    private static final HashMap<String, FunctionInfo> FUNCTIONS = new HashMap<>();

    static {
        FUNCTIONS.put("UNIX_TIMESTAMP",
                new FunctionInfo("UNIX_TIMESTAMP", UNIX_TIMESTAMP, VAR_ARGS, Value.INTEGER, true, false));
        FUNCTIONS.put("FROM_UNIXTIME",
                new FunctionInfo("FROM_UNIXTIME", FROM_UNIXTIME, VAR_ARGS, Value.VARCHAR, true, true));
        FUNCTIONS.put("DATE", new FunctionInfo("DATE", DATE, 1, Value.DATE, true, true));
        FUNCTIONS.put("LAST_INSERT_ID",
                new FunctionInfo("LAST_INSERT_ID", LAST_INSERT_ID, VAR_ARGS, Value.BIGINT, false, false));
    }

    /**
     * The date format of a MySQL formatted date/time.
     * Example: 2008-09-25 08:40:59
     */
    private static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    /**
     * Format replacements for MySQL date formats.
     * See
     * https://dev.mysql.com/doc/refman/5.1/en/date-and-time-functions.html#function_date-format
     */
    private static final String[] FORMAT_REPLACE = {
            "%a", "EEE",
            "%b", "MMM",
            "%c", "MM",
            "%d", "dd",
            "%e", "d",
            "%H", "HH",
            "%h", "hh",
            "%I", "hh",
            "%i", "mm",
            "%j", "DDD",
            "%k", "H",
            "%l", "h",
            "%M", "MMMM",
            "%m", "MM",
            "%p", "a",
            "%r", "hh:mm:ss a",
            "%S", "ss",
            "%s", "ss",
            "%T", "HH:mm:ss",
            "%W", "EEEE",
            "%w", "F",
            "%Y", "yyyy",
            "%y", "yy",
            "%%", "%",
    };

    /**
     * Get the seconds since 1970-01-01 00:00:00 UTC of the given timestamp.
     * See
     * https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_unix-timestamp
     *
     * @param session the session
     * @param value the timestamp
     * @return the timestamp in seconds since EPOCH
     */
    public static int unixTimestamp(SessionLocal session, Value value) {
        long seconds;
        if (value instanceof ValueTimestampTimeZone) {
            ValueTimestampTimeZone t = (ValueTimestampTimeZone) value;
            long timeNanos = t.getTimeNanos();
            seconds = DateTimeUtils.absoluteDayFromDateValue(t.getDateValue()) * DateTimeUtils.SECONDS_PER_DAY
                    + timeNanos / DateTimeUtils.NANOS_PER_SECOND - t.getTimeZoneOffsetSeconds();
        } else {
            ValueTimestamp t = (ValueTimestamp) value.convertTo(TypeInfo.TYPE_TIMESTAMP, session);
            long timeNanos = t.getTimeNanos();
            seconds = session.currentTimeZone().getEpochSecondsFromLocal(t.getDateValue(), timeNanos);
        }
        return (int) seconds;
    }

    /**
     * See
     * https://dev.mysql.com/doc/refman/5.1/en/date-and-time-functions.html#function_from-unixtime
     *
     * @param seconds The current timestamp in seconds.
     * @return a formatted date/time String in the format "yyyy-MM-dd HH:mm:ss".
     */
    public static String fromUnixTime(int seconds) {
        SimpleDateFormat formatter = new SimpleDateFormat(DATE_TIME_FORMAT,
                Locale.ENGLISH);
        return formatter.format(new Date(seconds * 1_000L));
    }

    /**
     * See
     * https://dev.mysql.com/doc/refman/5.1/en/date-and-time-functions.html#function_from-unixtime
     *
     * @param seconds The current timestamp in seconds.
     * @param format The format of the date/time String to return.
     * @return a formatted date/time String in the given format.
     */
    public static String fromUnixTime(int seconds, String format) {
        format = convertToSimpleDateFormat(format);
        SimpleDateFormat formatter = new SimpleDateFormat(format, Locale.ENGLISH);
        return formatter.format(new Date(seconds * 1_000L));
    }

    private static String convertToSimpleDateFormat(String format) {
        String[] replace = FORMAT_REPLACE;
        for (int i = 0; i < replace.length; i += 2) {
            format = StringUtils.replaceAll(format, replace[i], replace[i + 1]);
        }
        return format;
    }

    /**
     * Returns mode-specific function for a given name, or {@code null}.
     *
     * @param upperName
     *            the upper-case name of a function
     * @return the function with specified name or {@code null}
     */
    public static FunctionsMySQL getFunction(String upperName) {
        FunctionInfo info = FUNCTIONS.get(upperName);
        return info != null ? new FunctionsMySQL(info) : null;
    }

    FunctionsMySQL(FunctionInfo info) {
        super(info);
    }

    @Override
    protected void checkParameterCount(int len) {
        int min, max;
        switch (info.type) {
        case UNIX_TIMESTAMP:
            min = 0;
            max = 1;
            break;
        case FROM_UNIXTIME:
            min = 1;
            max = 2;
            break;
        case DATE:
            min = 1;
            max = 1;
            break;
        case LAST_INSERT_ID:
            min = 0;
            max = 1;
            break;
        default:
            throw DbException.getInternalError("type=" + info.type);
        }
        if (len < min || len > max) {
            throw DbException.get(ErrorCode.INVALID_PARAMETER_COUNT_2, info.name, min + ".." + max);
        }
    }

    @Override
    public Expression optimize(SessionLocal session) {
        boolean allConst = optimizeArguments(session);
        type = TypeInfo.getTypeInfo(info.returnDataType);
        if (allConst) {
            return ValueExpression.get(getValue(session));
        }
        return this;
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value[] values = getArgumentsValues(session, args);
        if (values == null) {
            return ValueNull.INSTANCE;
        }
        Value v0 = getNullOrValue(session, args, values, 0);
        Value v1 = getNullOrValue(session, args, values, 1);
        Value result;
        switch (info.type) {
        case UNIX_TIMESTAMP:
            result = ValueInteger.get(unixTimestamp(session, v0 == null ? session.currentTimestamp() : v0));
            break;
        case FROM_UNIXTIME:
            result = ValueVarchar.get(
                    v1 == null ? fromUnixTime(v0.getInt()) : fromUnixTime(v0.getInt(), v1.getString()));
            break;
        case DATE:
            switch (v0.getValueType()) {
            case Value.DATE:
                result = v0;
                break;
            default:
                try {
                    v0 = v0.convertTo(TypeInfo.TYPE_TIMESTAMP, session);
                } catch (DbException ex) {
                    result = ValueNull.INSTANCE;
                    break;
                }
                //$FALL-THROUGH$
            case Value.TIMESTAMP:
            case Value.TIMESTAMP_TZ:
                result = v0.convertToDate(session);
            }
            break;
        case LAST_INSERT_ID:
            if (args.length == 0) {
                result = session.getLastIdentity();
                if (result == ValueNull.INSTANCE) {
                    result = ValueBigint.get(0L);
                } else {
                    result = result.convertToBigint(null);
                }
            } else {
                result = v0;
                if (result == ValueNull.INSTANCE) {
                    session.setLastIdentity(ValueNull.INSTANCE);
                } else {
                    session.setLastIdentity(result = result.convertToBigint(null));
                }
            }
            break;
        default:
            throw DbException.getInternalError("type=" + info.type);
        }
        return result;
    }

}
