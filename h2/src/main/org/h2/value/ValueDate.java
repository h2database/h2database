/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.util.DateTimeUtils;

/**
 * Implementation of the DATE data type.
 */
public class ValueDate extends Value {
    public static final int PRECISION = 8;
    public static final int DISPLAY_SIZE = 10; // 2000-01-02

    private final Date value;

    private ValueDate(Date value) {
        this.value = value;
    }

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
        return value.toString();
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

    public static ValueDate get(Date date) {
        date = DateTimeUtils.cloneAndNormalizeDate(date);
        return getNoCopy(date);
    }

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
