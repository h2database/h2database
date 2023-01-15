/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import static org.h2.util.DateTimeUtils.NANOS_PER_DAY;
import static org.h2.util.DateTimeUtils.NANOS_PER_HOUR;
import static org.h2.util.DateTimeUtils.NANOS_PER_SECOND;
import static org.h2.util.DateTimeUtils.absoluteDayFromDateValue;
import static org.h2.util.DateTimeUtils.dateAndTimeFromValue;
import static org.h2.util.DateTimeUtils.dateTimeToValue;
import static org.h2.util.DateTimeUtils.dateValueFromAbsoluteDay;
import static org.h2.util.IntervalUtils.NANOS_PER_DAY_BI;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.h2.api.ErrorCode;
import org.h2.api.IntervalQualifier;
import org.h2.engine.SessionLocal;
import org.h2.expression.function.DateTimeFunction;
import org.h2.message.DbException;
import org.h2.util.DateTimeUtils;
import org.h2.util.IntervalUtils;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueInterval;
import org.h2.value.ValueNull;
import org.h2.value.ValueNumeric;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimeTimeZone;
import org.h2.value.ValueTimestampTimeZone;

/**
 * A mathematical operation with intervals.
 */
public class IntervalOperation extends Operation2 {

    public enum IntervalOpType {
        /**
         * Interval plus interval.
         */
        INTERVAL_PLUS_INTERVAL,

        /**
         * Interval minus interval.
         */
        INTERVAL_MINUS_INTERVAL,

        /**
         * Interval divided by interval (non-standard).
         */
        INTERVAL_DIVIDE_INTERVAL,

        /**
         * Date-time plus interval.
         */
        DATETIME_PLUS_INTERVAL,

        /**
         * Date-time minus interval.
         */
        DATETIME_MINUS_INTERVAL,

        /**
         * Interval multiplied by numeric.
         */
        INTERVAL_MULTIPLY_NUMERIC,

        /**
         * Interval divided by numeric.
         */
        INTERVAL_DIVIDE_NUMERIC,

        /**
         * Date-time minus date-time.
         */
        DATETIME_MINUS_DATETIME
    }

    /**
     * Number of digits enough to hold
     * {@code INTERVAL '999999999999999999' YEAR / INTERVAL '1' MONTH}.
     */
    private static final int INTERVAL_YEAR_DIGITS = 20;

    /**
     * Number of digits enough to hold
     * {@code INTERVAL '999999999999999999' DAY / INTERVAL '0.000000001' SECOND}.
     */
    private static final int INTERVAL_DAY_DIGITS = 32;

    private static final TypeInfo INTERVAL_DIVIDE_INTERVAL_YEAR_TYPE = TypeInfo.getTypeInfo(Value.NUMERIC,
            INTERVAL_YEAR_DIGITS * 3, INTERVAL_YEAR_DIGITS * 2, null);

    private static final TypeInfo INTERVAL_DIVIDE_INTERVAL_DAY_TYPE = TypeInfo.getTypeInfo(Value.NUMERIC,
            INTERVAL_DAY_DIGITS * 3, INTERVAL_DAY_DIGITS * 2, null);

    private final IntervalOpType opType;

    private TypeInfo forcedType;

    private static BigInteger nanosFromValue(SessionLocal session, Value v) {
        long[] a = dateAndTimeFromValue(v, session);
        return BigInteger.valueOf(absoluteDayFromDateValue(a[0])).multiply(NANOS_PER_DAY_BI)
                .add(BigInteger.valueOf(a[1]));
    }

    public IntervalOperation(IntervalOpType opType, Expression left, Expression right, TypeInfo forcedType) {
        this(opType, left, right);
        this.forcedType = forcedType;
    }

    public IntervalOperation(IntervalOpType opType, Expression left, Expression right) {
        super(left, right);
        this.opType = opType;
        int l = left.getType().getValueType(), r = right.getType().getValueType();
        switch (opType) {
        case INTERVAL_PLUS_INTERVAL:
        case INTERVAL_MINUS_INTERVAL:
            type = TypeInfo.getTypeInfo(Value.getHigherOrder(l, r));
            break;
        case INTERVAL_DIVIDE_INTERVAL:
            type = DataType.isYearMonthIntervalType(l) ? INTERVAL_DIVIDE_INTERVAL_YEAR_TYPE
                    : INTERVAL_DIVIDE_INTERVAL_DAY_TYPE;
            break;
        case DATETIME_PLUS_INTERVAL:
        case DATETIME_MINUS_INTERVAL:
        case INTERVAL_MULTIPLY_NUMERIC:
        case INTERVAL_DIVIDE_NUMERIC:
            type = left.getType();
            break;
        case DATETIME_MINUS_DATETIME:
            if (forcedType != null) {
                type = forcedType;
            } else if ((l == Value.TIME || l == Value.TIME_TZ) && (r == Value.TIME || r == Value.TIME_TZ)) {
                type = TypeInfo.TYPE_INTERVAL_HOUR_TO_SECOND;
            } else if (l == Value.DATE && r == Value.DATE) {
                type = TypeInfo.TYPE_INTERVAL_DAY;
            } else {
                type = TypeInfo.TYPE_INTERVAL_DAY_TO_SECOND;
            }
        }
    }

    @Override
    public boolean needParentheses() {
        return forcedType == null;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        if (forcedType != null) {
            getInnerSQL2(builder.append('('), sqlFlags);
            getForcedTypeSQL(builder.append(") "), forcedType);
        } else {
            getInnerSQL2(builder, sqlFlags);
        }
        return builder;
    }

    private void getInnerSQL2(StringBuilder builder, int sqlFlags) {
        left.getSQL(builder, sqlFlags, AUTO_PARENTHESES).append(' ').append(getOperationToken()).append(' ');
        right.getSQL(builder, sqlFlags, AUTO_PARENTHESES);
    }

    static StringBuilder getForcedTypeSQL(StringBuilder builder, TypeInfo forcedType) {
        int precision = (int) forcedType.getPrecision();
        int scale = forcedType.getScale();
        return IntervalQualifier.valueOf(forcedType.getValueType() - Value.INTERVAL_YEAR).getTypeName(builder,
                precision == ValueInterval.DEFAULT_PRECISION ? -1 : (int) precision,
                scale == ValueInterval.DEFAULT_SCALE ? -1 : scale, true);
    }

    private char getOperationToken() {
        switch (opType) {
        case INTERVAL_PLUS_INTERVAL:
        case DATETIME_PLUS_INTERVAL:
            return '+';
        case INTERVAL_MINUS_INTERVAL:
        case DATETIME_MINUS_INTERVAL:
        case DATETIME_MINUS_DATETIME:
            return '-';
        case INTERVAL_MULTIPLY_NUMERIC:
            return '*';
        case INTERVAL_DIVIDE_INTERVAL:
        case INTERVAL_DIVIDE_NUMERIC:
            return '/';
        default:
            throw DbException.getInternalError("opType=" + opType);
        }
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value l = left.getValue(session);
        Value r = right.getValue(session);
        if (l == ValueNull.INSTANCE || r == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        int lType = l.getValueType(), rType = r.getValueType();
        switch (opType) {
        case INTERVAL_PLUS_INTERVAL:
        case INTERVAL_MINUS_INTERVAL: {
            BigInteger a1 = IntervalUtils.intervalToAbsolute((ValueInterval) l);
            BigInteger a2 = IntervalUtils.intervalToAbsolute((ValueInterval) r);
            return IntervalUtils.intervalFromAbsolute(
                    IntervalQualifier.valueOf(Value.getHigherOrder(lType, rType) - Value.INTERVAL_YEAR),
                    opType == IntervalOpType.INTERVAL_PLUS_INTERVAL ? a1.add(a2) : a1.subtract(a2));
        }
        case INTERVAL_DIVIDE_INTERVAL:
            return ValueNumeric.get(IntervalUtils.intervalToAbsolute((ValueInterval) l))
                    .divide(ValueNumeric.get(IntervalUtils.intervalToAbsolute((ValueInterval) r)), type);
        case DATETIME_PLUS_INTERVAL:
        case DATETIME_MINUS_INTERVAL:
            return getDateTimeWithInterval(session, l, r, lType, rType);
        case INTERVAL_MULTIPLY_NUMERIC:
        case INTERVAL_DIVIDE_NUMERIC: {
            BigDecimal a1 = new BigDecimal(IntervalUtils.intervalToAbsolute((ValueInterval) l));
            BigDecimal a2 = r.getBigDecimal();
            return IntervalUtils.intervalFromAbsolute(IntervalQualifier.valueOf(lType - Value.INTERVAL_YEAR),
                    (opType == IntervalOpType.INTERVAL_MULTIPLY_NUMERIC ? a1.multiply(a2) : a1.divide(a2))
                            .toBigInteger());
        }
        case DATETIME_MINUS_DATETIME: {
            Value result;
            if ((lType == Value.TIME || lType == Value.TIME_TZ) && (rType == Value.TIME || rType == Value.TIME_TZ)) {
                long diff;
                if (lType == Value.TIME && rType == Value.TIME) {
                    diff = ((ValueTime) l).getNanos() - ((ValueTime) r).getNanos();
                } else {
                    ValueTimeTimeZone left = (ValueTimeTimeZone) l.convertTo(TypeInfo.TYPE_TIME_TZ, session),
                            right = (ValueTimeTimeZone) r.convertTo(TypeInfo.TYPE_TIME_TZ, session);
                    diff = left.getNanos() - right.getNanos()
                            + (right.getTimeZoneOffsetSeconds() - left.getTimeZoneOffsetSeconds())
                            * DateTimeUtils.NANOS_PER_SECOND;
                }
                boolean negative = diff < 0;
                if (negative) {
                    diff = -diff;
                }
                result = ValueInterval.from(IntervalQualifier.HOUR_TO_SECOND, negative, diff / NANOS_PER_HOUR,
                        diff % NANOS_PER_HOUR);
            } else if (forcedType != null && DataType.isYearMonthIntervalType(forcedType.getValueType())) {
                long[] dt1 = dateAndTimeFromValue(l, session), dt2 = dateAndTimeFromValue(r, session);
                long dateValue1 = lType == Value.TIME || lType == Value.TIME_TZ
                        ? session.currentTimestamp().getDateValue()
                        : dt1[0];
                long dateValue2 = rType == Value.TIME || rType == Value.TIME_TZ
                        ? session.currentTimestamp().getDateValue()
                        : dt2[0];
                long leading = 12L
                        * (DateTimeUtils.yearFromDateValue(dateValue1) - DateTimeUtils.yearFromDateValue(dateValue2))
                        + DateTimeUtils.monthFromDateValue(dateValue1) - DateTimeUtils.monthFromDateValue(dateValue2);
                int d1 = DateTimeUtils.dayFromDateValue(dateValue1);
                int d2 = DateTimeUtils.dayFromDateValue(dateValue2);
                if (leading >= 0) {
                    if (d1 < d2 || d1 == d2 && dt1[1] < dt2[1]) {
                        leading--;
                    }
                } else if (d1 > d2 || d1 == d2 && dt1[1] > dt2[1]) {
                    leading++;
                }
                boolean negative;
                if (leading < 0) {
                    negative = true;
                    leading = -leading;
                } else {
                    negative = false;
                }
                result = ValueInterval.from(IntervalQualifier.MONTH, negative, leading, 0L);
            } else if (lType == Value.DATE && rType == Value.DATE) {
                long diff = absoluteDayFromDateValue(((ValueDate) l).getDateValue())
                        - absoluteDayFromDateValue(((ValueDate) r).getDateValue());
                boolean negative = diff < 0;
                if (negative) {
                    diff = -diff;
                }
                result = ValueInterval.from(IntervalQualifier.DAY, negative, diff, 0L);
            } else {
                BigInteger diff = nanosFromValue(session, l).subtract(nanosFromValue(session, r));
                if (lType == Value.TIMESTAMP_TZ || rType == Value.TIMESTAMP_TZ) {
                    l = l.convertTo(TypeInfo.TYPE_TIMESTAMP_TZ, session);
                    r = r.convertTo(TypeInfo.TYPE_TIMESTAMP_TZ, session);
                    diff = diff.add(BigInteger.valueOf((((ValueTimestampTimeZone) r).getTimeZoneOffsetSeconds()
                            - ((ValueTimestampTimeZone) l).getTimeZoneOffsetSeconds()) * NANOS_PER_SECOND));
                }
                result = IntervalUtils.intervalFromAbsolute(IntervalQualifier.DAY_TO_SECOND, diff);
            }
            if (forcedType != null) {
                result = result.castTo(forcedType, session);
            }
            return result;
        }
        }
        throw DbException.getInternalError("type=" + opType);
    }

    private Value getDateTimeWithInterval(SessionLocal session, Value l, Value r, int lType, int rType) {
        switch (lType) {
        case Value.TIME:
            if (DataType.isYearMonthIntervalType(rType)) {
                throw DbException.getInternalError("type=" + rType);
            }
            return ValueTime.fromNanos(getTimeWithInterval(r, ((ValueTime) l).getNanos()));
        case Value.TIME_TZ: {
            if (DataType.isYearMonthIntervalType(rType)) {
                throw DbException.getInternalError("type=" + rType);
            }
            ValueTimeTimeZone t = (ValueTimeTimeZone) l;
            return ValueTimeTimeZone.fromNanos(getTimeWithInterval(r, t.getNanos()), t.getTimeZoneOffsetSeconds());
        }
        case Value.DATE:
        case Value.TIMESTAMP:
        case Value.TIMESTAMP_TZ:
            if (DataType.isYearMonthIntervalType(rType)) {
                long m = IntervalUtils.intervalToAbsolute((ValueInterval) r).longValue();
                if (opType == IntervalOpType.DATETIME_MINUS_INTERVAL) {
                    m = -m;
                }
                return DateTimeFunction.dateadd(session, DateTimeFunction.MONTH, m, l);
            } else {
                BigInteger a2 = IntervalUtils.intervalToAbsolute((ValueInterval) r);
                if (lType == Value.DATE) {
                    BigInteger a1 = BigInteger.valueOf(absoluteDayFromDateValue(((ValueDate) l).getDateValue()));
                    a2 = a2.divide(NANOS_PER_DAY_BI);
                    BigInteger n = opType == IntervalOpType.DATETIME_PLUS_INTERVAL ? a1.add(a2) : a1.subtract(a2);
                    return ValueDate.fromDateValue(dateValueFromAbsoluteDay(n.longValue()));
                } else {
                    long[] a = dateAndTimeFromValue(l, session);
                    long absoluteDay = absoluteDayFromDateValue(a[0]);
                    long timeNanos = a[1];
                    BigInteger[] dr = a2.divideAndRemainder(NANOS_PER_DAY_BI);
                    if (opType == IntervalOpType.DATETIME_PLUS_INTERVAL) {
                        absoluteDay += dr[0].longValue();
                        timeNanos += dr[1].longValue();
                    } else {
                        absoluteDay -= dr[0].longValue();
                        timeNanos -= dr[1].longValue();
                    }
                    if (timeNanos >= NANOS_PER_DAY) {
                        timeNanos -= NANOS_PER_DAY;
                        absoluteDay++;
                    } else if (timeNanos < 0) {
                        timeNanos += NANOS_PER_DAY;
                        absoluteDay--;
                    }
                    return dateTimeToValue(l, dateValueFromAbsoluteDay(absoluteDay), timeNanos);
                }
            }
        }
        throw DbException.getInternalError("type=" + opType);
    }

    private long getTimeWithInterval(Value r, long nanos) {
        BigInteger a1 = BigInteger.valueOf(nanos);
        BigInteger a2 = IntervalUtils.intervalToAbsolute((ValueInterval) r);
        BigInteger n = opType == IntervalOpType.DATETIME_PLUS_INTERVAL ? a1.add(a2) : a1.subtract(a2);
        if (n.signum() < 0 || n.compareTo(NANOS_PER_DAY_BI) >= 0) {
            throw DbException.get(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_1, n.toString());
        }
        nanos = n.longValue();
        return nanos;
    }

    @Override
    public Expression optimize(SessionLocal session) {
        left = left.optimize(session);
        right = right.optimize(session);
        if (left.isConstant() && right.isConstant()) {
            return ValueExpression.get(getValue(session));
        }
        return this;
    }

}
