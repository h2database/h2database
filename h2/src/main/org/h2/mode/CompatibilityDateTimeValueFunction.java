/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mode;

import org.h2.engine.SessionLocal;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Operation0;
import org.h2.expression.function.NamedExpression;
import org.h2.util.DateTimeUtils;
import org.h2.util.TimeZoneProvider;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;

/**
 * Current datetime value function.
 */
final class CompatibilityDateTimeValueFunction extends Operation0 implements NamedExpression {

    /**
     * The function "SYSDATE"
     */
    static final int SYSDATE = 0;

    /**
     * The function "SYSTIMESTAMP"
     */
    static final int SYSTIMESTAMP = 1;

    private static final String[] NAMES = { "SYSDATE", "SYSTIMESTAMP" };

    private final int function, scale;

    private final TypeInfo type;

    CompatibilityDateTimeValueFunction(int function, int scale) {
        this.function = function;
        this.scale = scale;
        if (function == SYSDATE) {
            type = TypeInfo.getTypeInfo(Value.TIMESTAMP, 0L, 0, null);
        } else {
            type = TypeInfo.getTypeInfo(Value.TIMESTAMP_TZ, 0L, scale, null);
        }
    }

    @Override
    public Value getValue(SessionLocal session) {
        ValueTimestampTimeZone v = session.currentTimestamp();
        long dateValue = v.getDateValue();
        long timeNanos = v.getTimeNanos();
        int offsetSeconds = v.getTimeZoneOffsetSeconds();
        int newOffset = TimeZoneProvider.getDefault()
                .getTimeZoneOffsetUTC(DateTimeUtils.getEpochSeconds(dateValue, timeNanos, offsetSeconds));
        if (offsetSeconds != newOffset) {
            v = DateTimeUtils.timestampTimeZoneAtOffset(dateValue, timeNanos, offsetSeconds, newOffset);
        }
        if (function == SYSDATE) {
            return ValueTimestamp.fromDateValueAndNanos(v.getDateValue(),
                    v.getTimeNanos() / DateTimeUtils.NANOS_PER_SECOND * DateTimeUtils.NANOS_PER_SECOND);
        }
        return v.castTo(type, session);
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        builder.append(getName());
        if (scale >= 0) {
            builder.append('(').append(scale).append(')');
        }
        return builder;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.DETERMINISTIC:
            return false;
        }
        return true;
    }

    @Override
    public TypeInfo getType() {
        return type;
    }

    @Override
    public int getCost() {
        return 1;
    }

    @Override
    public String getName() {
        return NAMES[function];
    }

}
