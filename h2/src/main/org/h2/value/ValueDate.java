/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.h2.util.DateTimeUtils;
import org.h2.util.MathUtils;

/**
 * Implementation of the DATE data type.
 */
public class ValueDate extends Value {

    /**
     * The precision in digits.
     */
    public static final int PRECISION = 8;

    /**
     * The display size of the textual representation of a date.
     * Example: 2000-01-02
     */
    static final int DISPLAY_SIZE = 10;

    private final long dateValue;

    private ValueDate(long dateValue) {
        this.dateValue = dateValue;
    }

    /**
     * Parse a string to a ValueDate.
     *
     * @param s the string to parse
     * @return the date
     */
    public static ValueDate parse(String s) {
        Value x = DateTimeUtils.parse(s, Value.DATE);
        return (ValueDate) Value.cache(x);
    }

    public Date getDate() {
        return DateTimeUtils.convertDateValueToDate(dateValue);
    }

    public long getDateValue() {
        return dateValue;
    }

    public String getSQL() {
        return "DATE '" + getString() + "'";
    }

    public int getType() {
        return Value.DATE;
    }

    protected int compareSecure(Value o, CompareMode mode) {
        return MathUtils.compareLong(dateValue, ((ValueDate) o).dateValue);
    }

    public String getString() {
        StringBuilder buff = new StringBuilder(DISPLAY_SIZE);
        DateTimeUtils.appendDate(buff, dateValue);
        return buff.toString();
    }

    public long getPrecision() {
        return PRECISION;
    }

    public int hashCode() {
        return (int) (dateValue ^ (dateValue >>> 32));
    }

    public Object getObject() {
        return getDate();
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setDate(parameterIndex, getDate());
    }

    /**
     * Get or create a date value for the given date.
     *
     * @param date the date
     * @return the value
     */
    public static ValueDate get(Date date) {
        long x = DateTimeUtils.dateValueFromDate(date.getTime());
        return get(x);
    }

    /**
     * Get or create a date value for the given date.
     *
     * @param dateValue the date value
     * @return the value
     */
    public static ValueDate get(long dateValue) {
        return (ValueDate) Value.cache(new ValueDate(dateValue));
    }

    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof ValueDate && dateValue == (((ValueDate) other).dateValue);
    }

}
