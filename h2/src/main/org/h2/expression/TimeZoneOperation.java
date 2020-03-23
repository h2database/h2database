/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
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
public class TimeZoneOperation extends Expression {

    private Expression arg;
    private Expression timeZone;
    private TypeInfo type;

    public TimeZoneOperation(Expression arg) {
        this.arg = arg;
    }

    public TimeZoneOperation(Expression arg, Expression timeZone) {
        this.arg = arg;
        this.timeZone = timeZone;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        arg.getSQL(builder.append('('), sqlFlags).append(" AT ");
        if (timeZone != null) {
            timeZone.getSQL(builder.append("TIME ZONE "), sqlFlags);
        } else {
            builder.append("LOCAL");
        }
        return builder.append(')');
    }

    @Override
    public Value getValue(Session session) {
        Value a = arg.getValue(session).convertTo(type, session);
        int valueType = a.getValueType();
        if ((valueType == Value.TIMESTAMP_TZ || valueType == Value.TIME_TZ) && timeZone != null) {
            Value b = timeZone.getValue(session);
            if (b != ValueNull.INSTANCE) {
                if (valueType == Value.TIMESTAMP_TZ) {
                    ValueTimestampTimeZone v = (ValueTimestampTimeZone) a;
                    long dateValue = v.getDateValue();
                    long timeNanos = v.getTimeNanos();
                    int offsetSeconds = v.getTimeZoneOffsetSeconds();
                    int newOffset = parseTimeZone(b, dateValue, timeNanos, offsetSeconds, true);
                    if (offsetSeconds != newOffset) {
                        a = DateTimeUtils.timestampTimeZoneAtOffset(dateValue, timeNanos, offsetSeconds, newOffset);
                    }
                } else {
                    ValueTimeTimeZone v = (ValueTimeTimeZone) a;
                    long timeNanos = v.getNanos();
                    int offsetSeconds = v.getTimeZoneOffsetSeconds();
                    int newOffset = parseTimeZone(b, DateTimeUtils.EPOCH_DATE_VALUE, timeNanos, offsetSeconds, false);
                    if (offsetSeconds != newOffset) {
                        timeNanos += (newOffset - offsetSeconds) * DateTimeUtils.NANOS_PER_SECOND;
                        a = ValueTimeTimeZone.fromNanos(DateTimeUtils.normalizeNanosOfDay(timeNanos), newOffset);
                    }
                }
            } else {
                a = ValueNull.INSTANCE;
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
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        arg.mapColumns(resolver, level, state);
        if (timeZone != null) {
            timeZone.mapColumns(resolver, level, state);
        }
    }

    @Override
    public Expression optimize(Session session) {
        arg = arg.optimize(session);
        if (timeZone != null) {
            timeZone = timeZone.optimize(session);
        }
        TypeInfo type = arg.getType();
        int valueType = Value.TIMESTAMP_TZ, scale = ValueTimestamp.MAXIMUM_SCALE;
        switch (type.getValueType()) {
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
            StringBuilder builder = arg.getSQL(new StringBuilder(), TRACE_SQL_FLAGS);
            int offset = builder.length();
            builder.append(" AT ");
            if (timeZone != null) {
                timeZone.getSQL(builder.append("TIME ZONE "), TRACE_SQL_FLAGS);
            } else {
                builder.append("LOCAL");
            }
            throw DbException.getSyntaxError(builder.toString(), offset, "time, timestamp");
        }
        this.type = TypeInfo.getTypeInfo(valueType, -1, scale, null);
        if (arg.isConstant() && (timeZone == null || timeZone.isConstant())) {
            return ValueExpression.get(getValue(session));
        }
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        arg.setEvaluatable(tableFilter, b);
        if (timeZone != null) {
            timeZone.setEvaluatable(tableFilter, b);
        }
    }

    @Override
    public TypeInfo getType() {
        return type;
    }

    @Override
    public void updateAggregate(Session session, int stage) {
        arg.updateAggregate(session, stage);
        if (timeZone != null) {
            timeZone.updateAggregate(session, stage);
        }
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return arg.isEverything(visitor) && (timeZone == null || timeZone.isEverything(visitor));
    }

    @Override
    public int getCost() {
        int cost = arg.getCost() + 1;
        if (timeZone != null) {
            cost += timeZone.getCost();
        }
        return cost;
    }

    @Override
    public int getSubexpressionCount() {
        return timeZone != null ? 2 : 1;
    }

    @Override
    public Expression getSubexpression(int index) {
        if (index == 0) {
            return arg;
        }
        if (index == 1 && timeZone != null) {
            return timeZone;
        }
        throw new IndexOutOfBoundsException();
    }

}
