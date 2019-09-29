/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.TimeZone;

import org.h2.api.ErrorCode;
import org.h2.engine.CastDataProvider;
import org.h2.message.DbException;
import org.h2.util.DateTimeUtils;
import org.h2.util.JSR310;
import org.h2.util.JSR310Utils;

/**
 * Implementation of the DATE data type.
 */
public class ValueDate extends Value {

    /**
     * The default precision and display size of the textual representation of a date.
     * Example: 2000-01-02
     */
    public static final int PRECISION = 10;

    private final long dateValue;

    private ValueDate(long dateValue) {
        if (dateValue < DateTimeUtils.MIN_DATE_VALUE || dateValue > DateTimeUtils.MAX_DATE_VALUE) {
            throw new IllegalArgumentException("dateValue out of range " + dateValue);
        }
        this.dateValue = dateValue;
    }

    /**
     * Get or create a date value for the given date.
     *
     * @param dateValue the date value
     * @return the value
     */
    public static ValueDate fromDateValue(long dateValue) {
        return (ValueDate) Value.cache(new ValueDate(dateValue));
    }

    /**
     * Get or create a date value for the given date.
     *
     * @param timeZone time zone, or {@code null} for default
     * @param date the date
     * @return the value
     */
    public static ValueDate get(TimeZone timeZone, Date date) {
        long ms = date.getTime();
        return fromDateValue(DateTimeUtils.dateValueFromLocalMillis(
                ms + (timeZone == null ? DateTimeUtils.getTimeZoneOffsetMillis(ms) : timeZone.getOffset(ms))));
    }

    /**
     * Parse a string to a ValueDate.
     *
     * @param s the string to parse
     * @return the date
     */
    public static ValueDate parse(String s) {
        try {
            return fromDateValue(DateTimeUtils.parseDateValue(s, 0, s.length()));
        } catch (Exception e) {
            throw DbException.get(ErrorCode.INVALID_DATETIME_CONSTANT_2,
                    e, "DATE", s);
        }
    }

    public long getDateValue() {
        return dateValue;
    }

    @Override
    public Date getDate(TimeZone timeZone) {
        return new Date(DateTimeUtils.getMillis(timeZone, dateValue, 0));
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_DATE;
    }

    @Override
    public int getValueType() {
        return DATE;
    }

    @Override
    public String getString() {
        StringBuilder buff = new StringBuilder(PRECISION);
        DateTimeUtils.appendDate(buff, dateValue);
        return buff.toString();
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder) {
        builder.append("DATE '");
        DateTimeUtils.appendDate(builder, dateValue);
        return builder.append('\'');
    }

    @Override
    public int compareTypeSafe(Value o, CompareMode mode, CastDataProvider provider) {
        return Long.compare(dateValue, ((ValueDate) o).dateValue);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof ValueDate
                && dateValue == (((ValueDate) other).dateValue);
    }

    @Override
    public int hashCode() {
        return (int) (dateValue ^ (dateValue >>> 32));
    }

    @Override
    public Object getObject() {
        return getDate(null);
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        if (JSR310.PRESENT) {
            try {
                prep.setObject(parameterIndex, JSR310Utils.valueToLocalDate(this), Types.DATE);
                return;
            } catch (SQLException ignore) {
                // Nothing to do
            }
        }
        prep.setDate(parameterIndex, getDate(null));
    }

}
