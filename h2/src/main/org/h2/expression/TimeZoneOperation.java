/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.util.DateTimeUtils;
import org.h2.util.TimeZoneProvider;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueInterval;
import org.h2.value.ValueNull;
import org.h2.value.ValueTimeTimeZone;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;

/**
 * A time zone specification (AT { TIME ZONE | LOCAL }).
 */
public final class TimeZoneOperation extends Operation1_2 {

    public TimeZoneOperation(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        left.getSQL(builder, sqlFlags, AUTO_PARENTHESES).append(" AT ");
        if (right != null) {
            right.getSQL(builder.append("TIME ZONE "), sqlFlags, AUTO_PARENTHESES);
        } else {
            builder.append("LOCAL");
        }
        return builder;
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value l = left.getValue(session);
        Value a = l.convertTo(type, session);
        if (a == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        Value b;
        if (right == null) {
            int t = l.getValueType();
            if (t == Value.TIME || t == Value.TIMESTAMP) {
                // Already in time zone of the session
                return a;
            }
            b = null;
        } else {
            b = right.getValue(session);
            if (b == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
        }
        if (a.getValueType() == Value.TIMESTAMP_TZ) {
            ValueTimestampTimeZone v = (ValueTimestampTimeZone) a;
            long dateValue = v.getDateValue();
            long timeNanos = v.getTimeNanos();
            int offsetSeconds = v.getTimeZoneOffsetSeconds();
            int newOffset = b != null //
                    ? parseTimeZone(b, dateValue, timeNanos, offsetSeconds, true)
                    : session.currentTimeZone()
                            .getTimeZoneOffsetUTC(DateTimeUtils.getEpochSeconds(dateValue, timeNanos, offsetSeconds));
            if (offsetSeconds != newOffset) {
                a = DateTimeUtils.timestampTimeZoneAtOffset(dateValue, timeNanos, offsetSeconds, newOffset);
            }
        } else {
            ValueTimeTimeZone v = (ValueTimeTimeZone) a;
            long timeNanos = v.getNanos();
            int offsetSeconds = v.getTimeZoneOffsetSeconds();
            int newOffset = b != null
                    ? parseTimeZone(b, DateTimeUtils.EPOCH_DATE_VALUE, timeNanos, offsetSeconds, false)
                    : session.currentTimeZone().getTimeZoneOffsetUTC(DateTimeUtils
                            .getEpochSeconds(session.currentTimestamp().getDateValue(), timeNanos, offsetSeconds));
            if (offsetSeconds != newOffset) {
                timeNanos += (newOffset - offsetSeconds) * DateTimeUtils.NANOS_PER_SECOND;
                a = ValueTimeTimeZone.fromNanos(DateTimeUtils.normalizeNanosOfDay(timeNanos), newOffset);
            }
        }
        return a;
    }

    private static int parseTimeZone(Value b, long dateValue, long timeNanos, int offsetSeconds,
            boolean allowTimeZoneName) {
        if (DataType.isCharacterStringType(b.getValueType())) {
            TimeZoneProvider timeZone;
            try {
                timeZone = TimeZoneProvider.ofId(b.getString());
            } catch (RuntimeException ex) {
                throw DbException.getInvalidValueException("time zone", b.getTraceSQL());
            }
            if (!allowTimeZoneName && !timeZone.hasFixedOffset()) {
                throw DbException.getInvalidValueException("time zone", b.getTraceSQL());
            }
            return timeZone.getTimeZoneOffsetUTC(DateTimeUtils.getEpochSeconds(dateValue, timeNanos, offsetSeconds));
        }
        return parseInterval(b);
    }

    /**
     * Parses a daytime interval as time zone offset.
     *
     * @param interval the interval
     * @return the time zone offset in seconds
     */
    public static int parseInterval(Value interval) {
        ValueInterval i = (ValueInterval) interval.convertTo(TypeInfo.TYPE_INTERVAL_HOUR_TO_SECOND);
        long h = i.getLeading(), seconds = i.getRemaining();
        if (h > 18 || h == 18 && seconds != 0 || seconds % DateTimeUtils.NANOS_PER_SECOND != 0) {
            throw DbException.getInvalidValueException("time zone", i.getTraceSQL());
        }
        int newOffset = (int) (h * 3_600 + seconds / DateTimeUtils.NANOS_PER_SECOND);
        if (i.isNegative()) {
            newOffset = -newOffset;
        }
        return newOffset;
    }

    @Override
    public Expression optimize(SessionLocal session) {
        left = left.optimize(session);
        if (right != null) {
            right = right.optimize(session);
        }
        TypeInfo type = left.getType();
        int valueType = Value.TIMESTAMP_TZ, scale = ValueTimestamp.MAXIMUM_SCALE;
        int lType = type.getValueType();
        switch (lType) {
        case Value.TIMESTAMP:
        case Value.TIMESTAMP_TZ:
            scale = type.getScale();
            break;
        case Value.TIME:
        case Value.TIME_TZ:
            valueType = Value.TIME_TZ;
            scale = type.getScale();
            break;
        default:
            StringBuilder builder = left.getSQL(new StringBuilder(), TRACE_SQL_FLAGS, AUTO_PARENTHESES);
            int offset = builder.length();
            builder.append(" AT ");
            if (right != null) {
                right.getSQL(builder.append("TIME ZONE "), TRACE_SQL_FLAGS, AUTO_PARENTHESES);
            } else {
                builder.append("LOCAL");
            }
            throw DbException.getSyntaxError(builder.toString(), offset, "time, timestamp");
        }
        this.type = TypeInfo.getTypeInfo(valueType, -1, scale, null);
        if (left.isConstant() && (lType == Value.TIME_TZ || lType == Value.TIMESTAMP_TZ) && right != null
                && right.isConstant()) {
            return ValueExpression.get(getValue(session));
        }
        return this;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        if (visitor.getType() == ExpressionVisitor.DETERMINISTIC) {
            if (right == null) {
                return false;
            }
            int lType = left.getType().getValueType();
            if (lType == Value.TIME || lType == Value.TIMESTAMP) {
                return false;
            }
        }
        return left.isEverything(visitor) && (right == null || right.isEverything(visitor));
    }

}
