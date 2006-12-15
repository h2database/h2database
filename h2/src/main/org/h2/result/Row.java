/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.sql.SQLException;

import org.h2.store.DataPage;
import org.h2.store.Record;
import org.h2.value.Value;

/**
 * @author Thomas
 */
public class Row extends Record implements SearchRow {
    private Value[] data;

    public Row(Value[] data) {
        this.data = data;
    }

    public Row(Row old) {
        this.data = old.data;
    }

    public Row() {
        // empty constructor
    }

    public Value getValue(int i) {
        return data[i];
    }

    public void write(DataPage buff) throws SQLException {
        buff.writeInt(data.length);
        for (int i = 0; i < data.length; i++) {
            Value v = data[i];
            buff.writeValue(v);
        }
    }

    public int getByteCount(DataPage dummy) throws SQLException {
        int len = data.length;
        int size = dummy.getIntLen();
        for (int i = 0; i < len; i++) {
            Value v = data[i];
            size += dummy.getValueLen(v);
        }
        return size;
    }

    public void setValue(int i, Value v) {
        data[i] = v;
    }

    public boolean isEmpty() {
        return data == null;
    }

    public int getColumnCount() {
        return data.length;
    }

}
