/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import static org.h2.util.DateTimeUtils.NANOS_PER_DAY;

import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.util.DateTimeUtils;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueNull;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimeTimeZone;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;

/**
 * A compatibility mathematical operation with datetime values.
 */
public class CompatibilityDatePlusTimeOperation extends Expression {

    private Expression left, right;

    private TypeInfo type;

    public CompatibilityDatePlusTimeOperation(Expression left, Expression right) {
        this.left = left;
        this.right = right;
        TypeInfo l = left.getType(), r = right.getType();
        int t;
        switch (l.getValueType()) {
        case Value.TIMESTAMP_TZ:
            if (r.getValueType() == Value.TIME_TZ) {
                throw DbException.getUnsupportedException("TIMESTAMP WITH TIME ZONE + TIME WITH TIME ZONE");
            }
            //$FALL-THROUGH$
        case Value.TIME:
            t = l.getValueType();
            break;
        case Value.TIME_TZ:
            if (r.getValueType() == Value.TIME_TZ) {
                throw DbException.getUnsupportedException("TIME WITH TIME ZONE + TIME WITH TIME ZONE");
            }
            t = l.getValueType();
            break;
        case Value.DATE:
        case Value.TIMESTAMP:
            t = r.getValueType() == Value.TIME_TZ ? Value.TIMESTAMP_TZ : Value.TIMESTAMP;
            break;
        default:
            throw DbException.getUnsupportedException(
                    DataType.getDataType(l.getValueType()).name + " + " + DataType.getDataType(r.getValueType()).name);
        }
        type = TypeInfo.getTypeInfo(t, 0L, Math.max(l.getScale(), r.getScale()), null);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, boolean alwaysQuote) {
        left.getSQL(builder.append('('), alwaysQuote).append(" + ");
        return right.getSQL(builder, alwaysQuote).append(')');
    }

    @Override
    public Value getValue(Session session) {
        Value l = left.getValue(session);
        Value r = right.getValue(session);
        if (l == ValueNull.INSTANCE || r == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        boolean withTimeZone = r.getValueType() == Value.TIME_TZ;
        switch (l.getValueType()) {
        case Value.DATE: {
            long dateValue = ((ValueDate) l).getDateValue();
            if (withTimeZone) {
                ValueTimeTimeZone t = (ValueTimeTimeZone) r;
                return ValueTimestampTimeZone.fromDateValueAndNanos(dateValue, t.getNanos(),
                        t.getTimeZoneOffsetSeconds());
            } else {
                return ValueTimestamp.fromDateValueAndNanos(dateValue, ((ValueTime) r).getNanos());
            }
        }
        case Value.TIMESTAMP: {
            if (withTimeZone) {
                ValueTimestamp ts = (ValueTimestamp) l;
                l = ValueTimestampTimeZone.fromDateValueAndNanos(ts.getDateValue(), ts.getTimeNanos(),
                        ((ValueTimeTimeZone) r).getTimeZoneOffsetSeconds());
            }
            break;
        }
        }
        long[] a = DateTimeUtils.dateAndTimeFromValue(l, session);
        long dateValue = a[0], timeNanos = a[1]
                + (r instanceof ValueTime ? ((ValueTime) r).getNanos() : ((ValueTimeTimeZone) r).getNanos());
        if (timeNanos >= NANOS_PER_DAY) {
            timeNanos -= NANOS_PER_DAY;
            dateValue = DateTimeUtils.incrementDateValue(dateValue);
        }
        return DateTimeUtils.dateTimeToValue(l, dateValue, timeNanos);
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        left.mapColumns(resolver, level, state);
        right.mapColumns(resolver, level, state);
    }

    @Override
    public Expression optimize(Session session) {
        left = left.optimize(session);
        right = right.optimize(session);
        if (left.isConstant() && right.isConstant()) {
            return ValueExpression.get(getValue(session));
        }
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
        right.setEvaluatable(tableFilter, b);
    }

    @Override
    public TypeInfo getType() {
        return type;
    }

    @Override
    public void updateAggregate(Session session, int stage) {
        left.updateAggregate(session, stage);
        right.updateAggregate(session, stage);
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor) && right.isEverything(visitor);
    }

    @Override
    public int getCost() {
        return left.getCost() + 1 + right.getCost();
    }

    @Override
    public int getSubexpressionCount() {
        return 2;
    }

    @Override
    public Expression getSubexpression(int index) {
        switch (index) {
        case 0:
            return left;
        case 1:
            return right;
        default:
            throw new IndexOutOfBoundsException();
        }
    }

}
