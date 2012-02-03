/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.util.StatementBuilder;
import org.h2.value.Value;

/**
 * Represents a simple row without state.
 */
public class SimpleRow implements SearchRow {

    private long key;
    private int version;
    private Value[] data;

    public SimpleRow(Value[] data) {
        this.data = data;
    }

    public int getColumnCount() {
        return data.length;
    }

    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public void setKeyAndVersion(SearchRow row) {
        key = row.getKey();
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
        StatementBuilder buff = new StatementBuilder("( /* key:");
        buff.append(getKey());
        if (version != 0) {
            buff.append(" v:" + version);
        }
        buff.append(" */ ");
        for (Value v : data) {
            buff.appendExceptFirst(", ");
            buff.append(v == null ? "null" : v.getTraceSQL());
        }
        return buff.append(')').toString();
    }

}
