/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Thomas
 */
public class ValueBoolean extends Value {
    public static final int PRECISION = 1;

    private Boolean value;

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

//    public String getJavaString() {
//        return value.booleanValue() ? "true" : "false";
//    }
    
    public int getDisplaySize() {
        return "FALSE".length();
    }

    protected boolean isEqual(Value v) {
        return v instanceof ValueBoolean && value == ((ValueBoolean)v).value;
    }    

}
