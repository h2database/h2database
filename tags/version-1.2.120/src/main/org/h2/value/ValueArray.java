/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.h2.util.MathUtils;
import org.h2.util.StatementBuilder;

/**
 * Implementation of the ARRAY data type.
 */
public class ValueArray extends Value {
    private final Value[] values;
    private int hash;

    private ValueArray(Value[] list) {
        this.values = list;
    }

    /**
     * Get or create a array value for the given value array.
     * Do not clone the data.
     *
     * @param list the value array
     * @return the value
     */
    public static ValueArray get(Value[] list) {
        return new ValueArray(list);
    }

    public int hashCode() {
        if (hash != 0) {
            return hash;
        }
        int h = 1;
        for (Value v : values) {
            h = h * 31 + v.hashCode();
        }
        hash = h;
        return h;
    }

    public Value[] getList() {
        return values;
    }

    public int getType() {
        return Value.ARRAY;
    }

    public long getPrecision() {
        return 0;
    }

    public String getString() {
        StatementBuilder buff = new StatementBuilder("(");
        for (Value v : values) {
            buff.appendExceptFirst(", ");
            buff.append(v.getString());
        }
        return buff.append(')').toString();
    }

    protected int compareSecure(Value o, CompareMode mode) throws SQLException {
        ValueArray v = (ValueArray) o;
        if (values == v.values) {
            return 0;
        }
        int l = values.length;
        int ol = v.values.length;
        int len = Math.min(l, ol);
        for (int i = 0; i < len; i++) {
            Value v1 = values[i];
            Value v2 = v.values[i];
            int comp = v1.compareTo(v2, mode);
            if (comp != 0) {
                return comp;
            }
        }
        return l > ol ? 1 : l == ol ? 0 : -1;
    }

    public Object getObject() {
        Object[] list = new Object[values.length];
        for (int i = 0; i < values.length; i++) {
            list[i] = values[i].getObject();
        }
        return list;
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        throw throwUnsupportedExceptionForType();
    }

    public String getSQL() {
        StatementBuilder buff = new StatementBuilder("(");
        for (Value v : values) {
            buff.appendExceptFirst(", ");
            buff.append(v.getSQL());
        }
        return buff.append(')').toString();
    }

    public String getTraceSQL() {
        StatementBuilder buff = new StatementBuilder("(");
        for (Value v : values) {
            buff.appendExceptFirst(", ");
            buff.append(v.getTraceSQL());
        }
        return buff.append(')').toString();
    }

    public int getDisplaySize() {
        long size = 0;
        for (Value v : values) {
            size += v.getDisplaySize();
        }
        return MathUtils.convertLongToInt(size);
    }

    public boolean equals(Object other) {
        if (!(other instanceof ValueArray)) {
            return false;
        }
        ValueArray v = (ValueArray) other;
        if (values == v.values) {
            return true;
        }
        if (values.length != v.values.length) {
            return false;
        }
        for (int i = 0; i < values.length; i++) {
            if (!values[i].equals(v.values[i])) {
                return false;
            }
        }
        return true;
    }

    public int getMemory() {
        int memory = 0;
        for (Value v : values) {
            memory += v.getMemory();
        }
        return memory;
    }

}
