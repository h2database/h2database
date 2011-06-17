/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.value.Value;

/**
 * Represents a simple row that is not cached separately.
 */
public class SimpleRow implements SearchRow {

    private int pos;
    private int version;
    private Value[] data;

    public SimpleRow(Value[] data) {
        this.data = data;
    }

    public int getColumnCount() {
        return data.length;
    }

    public int getPos() {
        return pos;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }

    public void setPosAndVersion(SearchRow row) {
        pos = row.getPos();
        version = row.getVersion();
    }

    public int getVersion() {
        return version;
    }

    public void setValue(int i, Value v) {
        data[i] = v;
    }

    public Value getValue(int i) {
        return data[i];
    }

    public String toString() {
        StringBuffer buff = new StringBuffer(data.length * 5);
        buff.append("( /* pos:");
        buff.append(getPos());
        if (version != 0) {
            buff.append(" v:" + version);
        }
        buff.append(" */ ");
        for (int i = 0; i < data.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            Value v = data[i];
            buff.append(v == null ? "null" : v.getTraceSQL());
        }
        buff.append(')');
        return buff.toString();
    }

}
