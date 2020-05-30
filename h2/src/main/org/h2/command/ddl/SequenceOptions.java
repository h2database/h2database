/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ValueExpression;
import org.h2.message.DbException;
import org.h2.schema.Sequence;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * Sequence options.
 */
public class SequenceOptions {

    private TypeInfo dataType;

    private Expression start;

    private Expression restart;

    private Expression increment;

    private Expression maxValue;

    private Expression minValue;

    private Boolean cycle;

    private Expression cacheSize;

    private long[] bounds;

    private static Long getLong(Session session, Expression expr) {
        if (expr != null) {
            Value value = expr.optimize(session).getValue(session);
            if (value != ValueNull.INSTANCE) {
                return value.getLong();
            }
        }
        return null;
    }

    public TypeInfo getDataType() {
        return dataType;
    }

    public void setDataType(TypeInfo dataType) {
        this.dataType = dataType;
    }

    /**
     * Gets start value.
     *
     * @param session The session to calculate the value.
     * @return start value or {@code null} if value is not defined.
     */
    public Long getStartValue(Session session) {
        return check(getLong(session, start));
    }

    /**
     * Sets start value expression.
     *
     * @param start START WITH value expression.
     */
    public void setStartValue(Expression start) {
        this.start = start;
    }

    /**
     * Gets restart value.
     *
     * @param session
     *            the session to calculate the value
     * @param startValue
     *            the start value to use if restart without value is specified
     * @return restart value or {@code null} if value is not defined.
     */
    public Long getRestartValue(Session session, long startValue) {
        return check(restart == ValueExpression.DEFAULT ? (Long) startValue : getLong(session, restart));
    }

    /**
     * Sets restart value expression, or {@link ValueExpression#DEFAULT}.
     *
     * @param restart
     *            RESTART WITH value expression, or
     *            {@link ValueExpression#DEFAULT} for simple RESTART
     */
    public void setRestartValue(Expression restart) {
        this.restart = restart;
    }

    /**
     * Gets increment value.
     *
     * @param session The session to calculate the value.
     * @return increment value or {@code null} if value is not defined.
     */
    public Long getIncrement(Session session) {
        return check(getLong(session, increment));
    }

    /**
     * Sets increment value expression.
     *
     * @param increment INCREMENT BY value expression.
     */
    public void setIncrement(Expression increment) {
        this.increment = increment;
    }

    /**
     * Gets max value.
     *
     * @param sequence the sequence to get default max value.
     * @param session The session to calculate the value.
     * @return max value when the MAXVALUE expression is set, otherwise returns default max value.
     */
    public Long getMaxValue(Sequence sequence, Session session) {
        Long v;
        if (maxValue == ValueExpression.NULL && sequence != null) {
            v = Sequence.getDefaultMaxValue(getCurrentStart(sequence, session),
                    increment != null ? getIncrement(session) : sequence.getIncrement(), getBounds());
        } else {
            v = getLong(session, maxValue);
        }
        return check(v);
    }

    /**
     * Sets max value expression.
     *
     * @param maxValue MAXVALUE expression.
     */
    public void setMaxValue(Expression maxValue) {
        this.maxValue = maxValue;
    }

    /**
     * Gets min value.
     *
     * @param sequence the sequence to get default min value.
     * @param session The session to calculate the value.
     * @return min value when the MINVALUE expression is set, otherwise returns default min value.
     */
    public Long getMinValue(Sequence sequence, Session session) {
        Long v;
        if (minValue == ValueExpression.NULL && sequence != null) {
            v = Sequence.getDefaultMinValue(getCurrentStart(sequence, session),
                    increment != null ? getIncrement(session) : sequence.getIncrement(), getBounds());
        } else {
            v = getLong(session, minValue);
        }
        return check(v);
    }

    /**
     * Sets min value expression.
     *
     * @param minValue MINVALUE expression.
     */
    public void setMinValue(Expression minValue) {
        this.minValue = minValue;
    }

    private Long check(Long value) {
        if (value == null) {
            return null;
        } else {
            long[] bounds = getBounds();
            long v = value;
            if (v < bounds[0] || v > bounds[1]) {
                throw DbException.get(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_1, Long.toString(v));
            }
        }
        return value;
    }

    public long[] getBounds() {
        long[] bounds = this.bounds;
        if (bounds == null) {
            this.bounds = bounds = getBounds(dataType);
        }
        return bounds;
    }

    public static long[] getBounds(TypeInfo dataType) {
        long min, max;
        switch (dataType.getValueType()) {
        case Value.TINYINT:
            min = Byte.MIN_VALUE;
            max = Byte.MAX_VALUE;
            break;
        case Value.SMALLINT:
            min = Short.MIN_VALUE;
            max = Short.MAX_VALUE;
            break;
        case Value.INTEGER:
            min = Integer.MIN_VALUE;
            max = Integer.MAX_VALUE;
            break;
        case Value.BIGINT:
            min = Long.MIN_VALUE;
            max = Long.MAX_VALUE;
            break;
        case Value.REAL:
            min = -0x100_0000;
            max = 0x100_0000;
            break;
        case Value.DOUBLE:
            min = -0x20_0000_0000_0000L;
            max = 0x20_0000_0000_0000L;
            break;
        case Value.NUMERIC: {
            long p = (dataType.getPrecision() - dataType.getScale());
            if (p <= 0) {
                throw DbException.getUnsupportedException(dataType.getTraceSQL());
            } else if (p > 18) {
                min = Long.MIN_VALUE;
                max = Long.MAX_VALUE;
            } else {
                max = 10;
                for (int i = 1; i < p; i++) {
                    max *= 10;
                }
                min = - --max;
            }
            break;
        }
        default:
            throw DbException.getUnsupportedException(dataType.getTraceSQL());
        }
        long bounds[] = { min, max };
        return bounds;
    }

    /**
     * Gets cycle flag.
     *
     * @return cycle flag value or {@code null} if value is not defined.
     */
    public Boolean getCycle() {
        return cycle;
    }

    /**
     * Sets cycle flag.
     *
     * @param cycle flag value.
     */
    public void setCycle(Boolean cycle) {
        this.cycle = cycle;
    }

    /**
     * Gets cache size.
     *
     * @param session The session to calculate the value.
     * @return cache size or {@code null} if value is not defined.
     */
    public Long getCacheSize(Session session) {
        return getLong(session, cacheSize);
    }

    /**
     * Sets cache size.
     *
     * @param cacheSize cache size.
     */
    public void setCacheSize(Expression cacheSize) {
        this.cacheSize = cacheSize;
    }

    boolean isRangeSet() {
        return restart != null || start != null || minValue != null || maxValue != null || increment != null;
    }

    private long getCurrentStart(Sequence sequence, Session session) {
        return start != null ? getStartValue(session) : sequence.getCurrentValue() + sequence.getIncrement();
    }
}
