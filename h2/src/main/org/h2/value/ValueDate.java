/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Calendar;

import org.h2.constant.ErrorCode;
import org.h2.util.DateTimeUtils;

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

    private final Date value;

    private ValueDate(Date value) {
        this.value = value;
    }

    /**
     * Parse a string to a java.sql.Date object.
     *
     * @param s the string to parse
     * @return the date
     */
    public static Date parseDate(String s) throws SQLException {
        return (Date) DateTimeUtils.parseDateTime(s, Value.DATE, ErrorCode.DATE_CONSTANT_2);
    }

    public Date getDate() {
        // this class is mutable - must copy the object
        return (Date) value.clone();
    }

    public Date getDateNoCopy() {
        return value;
    }

    public String getSQL() {
        return "DATE '" + getString() + "'";
    }

    public int getType() {
        return Value.DATE;
    }

    protected int compareSecure(Value o, CompareMode mode) {
        ValueDate v = (ValueDate) o;
        int c = value.compareTo(v.value);
        return c == 0 ? 0 : (c < 0 ? -1 : 1);
    }

    public String getString() {
        String s = value.toString();
        long time = value.getTime();
        // special case: java.sql.Date doesn't format
        // years below year 1 (BC) correctly
        if (time < ValueTimestamp.YEAR_ONE) {
            int year = DateTimeUtils.getDatePart(value, Calendar.YEAR);
            if (year < 1) {
                s = year + s.substring(s.indexOf('-'));
            }
        }
        return s;
    }

    public long getPrecision() {
        return PRECISION;
    }

    public int hashCode() {
        return value.hashCode();
    }

    public Object getObject() {
        // this class is mutable - must copy the object
        return getDate();
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setDate(parameterIndex, value);
    }

    /**
     * Get or create a date value for the given date.
     * Clone the date.
     *
     * @param date the date
     * @return the value
     */
    public static ValueDate get(Date date) {
        date = DateTimeUtils.cloneAndNormalizeDate(date);
        return getNoCopy(date);
    }

    /**
     * Get or create a date value for the given date.
     * Do not clone the date.
     *
     * @param date the date
     * @return the value
     */
    public static ValueDate getNoCopy(Date date) {
        return (ValueDate) Value.cache(new ValueDate(date));
    }

    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    public boolean equals(Object other) {
        return other instanceof ValueDate && value.equals(((ValueDate) other).value);
    }

}
