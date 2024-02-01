/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import org.h2.api.ErrorCode;
import org.h2.engine.CastDataProvider;
import org.h2.message.DbException;
import org.h2.util.DateTimeUtils;

/**
 * Implementation of the DATE data type.
 */
public final class ValueDate extends Value {

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
    public TypeInfo getType() {
        return TypeInfo.TYPE_DATE;
    }

    @Override
    public int getValueType() {
        return DATE;
    }

    @Override
    public String getString() {
        return DateTimeUtils.appendDate(new StringBuilder(PRECISION), dateValue).toString();
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return DateTimeUtils.appendDate(builder.append("DATE '"), dateValue).append('\'');
    }

    @Override
    public int compareTypeSafe(Value o, CompareMode mode, CastDataProvider provider) {
        return Long.compare(dateValue, ((ValueDate) o).dateValue);
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof ValueDate && dateValue == ((ValueDate) other).dateValue;
    }

    @Override
    public int hashCode() {
        return (int) (dateValue ^ (dateValue >>> 32));
    }

}
