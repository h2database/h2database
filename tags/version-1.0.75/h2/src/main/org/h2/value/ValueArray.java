/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.h2.message.Message;
import org.h2.util.MathUtils;

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
        for (int i = 0; i < values.length;) {
            h = h * 31 + values[i++].hashCode();
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
        StringBuffer buff = new StringBuffer();
        buff.append('(');
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(values[i].getString());
        }
        buff.append(')');
        return buff.toString();
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
        throw Message.getUnsupportedException();
    }

    public String getSQL() {
        StringBuffer buff = new StringBuffer();
        buff.append('(');
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(values[i].getSQL());
        }
        buff.append(')');
        return buff.toString();
    }

    public int getDisplaySize() {
        long size = 0;
        for (int i = 0; i < values.length; i++) {
            size += values[i].getDisplaySize();
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
        for (int i = 0; i < values.length; i++) {
            memory += values[i].getMemory();
        }
        return memory;
    }

}
