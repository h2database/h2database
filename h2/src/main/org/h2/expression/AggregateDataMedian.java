/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;

import org.h2.engine.Database;
import org.h2.util.DateTimeUtils;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueDecimal;
import org.h2.value.ValueDouble;
import org.h2.value.ValueFloat;
import org.h2.value.ValueInt;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;

/**
 * Data stored while calculating a MEDIAN aggregate.
 */
class AggregateDataMedian extends AggregateData {
    private Collection<Value> values;

    @Override
    void add(Database database, int dataType, boolean distinct, Value v) {
        if (v == ValueNull.INSTANCE) {
            return;
        }
        Collection<Value> c = values;
        if (c == null) {
            values = c = distinct ? new HashSet<Value>() : new ArrayList<Value>();
        }
        c.add(v);
    }

    @Override
    Value getValue(Database database, int dataType, boolean distinct) {
        Collection<Value> c = values;
        if (c == null) {
            return ValueNull.INSTANCE;
        }
        if (distinct && c instanceof ArrayList) {
            c = new HashSet<>(c);
        }
        Value[] a = c.toArray(new Value[0]);
        final CompareMode mode = database.getCompareMode();
        Arrays.sort(a, new Comparator<Value>() {
            @Override
            public int compare(Value o1, Value o2) {
                return o1.compareTo(o2, mode);
            }
        });
        int len = a.length;
        int idx = len / 2;
        Value v1 = a[idx];
        if ((len & 1) == 1) {
            return v1.convertTo(dataType);
        }
        return getMedian(a[idx - 1], v1, dataType, mode);
    }

    static Value getMedian(Value v0, Value v1, int dataType, CompareMode mode) {
        if (v0.compareTo(v1, mode) == 0) {
            return v0.convertTo(dataType);
        }
        switch (dataType) {
        case Value.BYTE:
        case Value.SHORT:
        case Value.INT:
            return ValueInt.get((v0.getInt() + v1.getInt()) / 2).convertTo(dataType);
        case Value.LONG:
            return ValueLong.get((v0.getLong() + v1.getLong()) / 2);
        case Value.DECIMAL:
            return ValueDecimal.get(v0.getBigDecimal().add(v1.getBigDecimal()).divide(BigDecimal.valueOf(2)));
        case Value.FLOAT:
            return ValueFloat.get((v0.getFloat() + v1.getFloat()) / 2);
        case Value.DOUBLE:
            return ValueDouble.get((v0.getFloat() + v1.getDouble()) / 2);
        case Value.TIME: {
            return ValueTime.fromMillis((v0.getTime().getTime() + v1.getTime().getTime()) / 2);
        }
        case Value.DATE: {
            ValueDate d0 = (ValueDate) v0.convertTo(Value.DATE), d1 = (ValueDate) v1.convertTo(Value.DATE);
            return ValueDate.fromDateValue(
                    DateTimeUtils.dateValueFromAbsoluteDay((DateTimeUtils.absoluteDayFromDateValue(d0.getDateValue())
                            + DateTimeUtils.absoluteDayFromDateValue(d1.getDateValue())) / 2));
        }
        case Value.TIMESTAMP: {
            ValueTimestamp ts0 = (ValueTimestamp) v0.convertTo(Value.TIMESTAMP),
                    ts1 = (ValueTimestamp) v1.convertTo(Value.TIMESTAMP);
            long dateSum = DateTimeUtils.absoluteDayFromDateValue(ts0.getDateValue())
                    + DateTimeUtils.absoluteDayFromDateValue(ts1.getDateValue());
            long nanos = (ts0.getTimeNanos() + ts1.getTimeNanos()) / 2;
            if ((dateSum & 1) != 0) {
                nanos += (DateTimeUtils.NANOS_PER_DAY / 2);
                if (nanos >= DateTimeUtils.NANOS_PER_DAY) {
                    nanos -= DateTimeUtils.NANOS_PER_DAY;
                    dateSum++;
                }
            }
            return ValueTimestamp.fromDateValueAndNanos(DateTimeUtils.dateValueFromAbsoluteDay(dateSum / 2), nanos);
        }
        case Value.TIMESTAMP_TZ: {
            ValueTimestampTimeZone ts0 = (ValueTimestampTimeZone) v0.convertTo(Value.TIMESTAMP_TZ),
                    ts1 = (ValueTimestampTimeZone) v1.convertTo(Value.TIMESTAMP_TZ);
            long dateSum = DateTimeUtils.absoluteDayFromDateValue(ts0.getDateValue())
                    + DateTimeUtils.absoluteDayFromDateValue(ts1.getDateValue());
            long nanos = (ts0.getTimeNanos() + ts1.getTimeNanos()) / 2;
            if ((dateSum & 1) != 0) {
                nanos += (DateTimeUtils.NANOS_PER_DAY / 2);
                if (nanos >= DateTimeUtils.NANOS_PER_DAY) {
                    nanos -= DateTimeUtils.NANOS_PER_DAY;
                    dateSum++;
                }
            }
            return ValueTimestampTimeZone.fromDateValueAndNanos(DateTimeUtils.dateValueFromAbsoluteDay(dateSum / 2),
                    nanos, (short) ((ts0.getTimeZoneOffsetMins() + ts1.getTimeZoneOffsetMins()) / 2));
        }
        default:
            // Just return first
            return v0.convertTo(dataType);
        }
    }

}
