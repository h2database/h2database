/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Implementation of the BOOLEAN data type.
 */
public class ValueBoolean extends Value {
    public static final int PRECISION = 1;
    public static final int DISPLAY_SIZE = 5; // false

    private final Boolean value;

    private static final ValueBoolean TRUE = new ValueBoolean(true);
    private static final ValueBoolean FALSE = new ValueBoolean(false);

    private ValueBoolean(boolean value) {
        this.value = Boolean.valueOf(""+value);
    }

    public int getType() {
        return Value.BOOLEAN;
    }

    public String getSQL() {
        return getString();
    }

    public String getString() {
        return value.booleanValue() ? "TRUE" : "FALSE";
    }

    public Value negate() throws SQLException {
        return value.booleanValue() ? FALSE : TRUE;
    }

    public Boolean getBoolean() {
        return value;
    }

    protected int compareSecure(Value o, CompareMode mode) {
        boolean v2 = ((ValueBoolean) o).value.booleanValue();
        boolean v = value.booleanValue();
        return (v == v2) ? 0 : (v ? 1 : -1);
    }

    public long getPrecision() {
        return PRECISION;
    }

    public int hashCode() {
        return value.booleanValue() ? 1 : 0;
    }

    public Object getObject() {
        return value;
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setBoolean(parameterIndex, value.booleanValue());
    }

    public static ValueBoolean get(boolean b) {
        return b ? TRUE : FALSE;
    }

    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    public boolean equals(Object other) {
        // there are only ever two instances, so the instance must match
        return this == other;
    }

}
