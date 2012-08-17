/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.nio.ByteBuffer;
import org.h2.dev.store.btree.DataType;
import org.h2.dev.store.btree.MapFactory;
import org.h2.dev.store.btree.DataUtils;
import org.h2.util.StringUtils;

/**
 * A row type.
 */
public class RowType implements DataType {

    private final DataType[] types;

    private RowType(DataType[] types) {
        this.types = types;
    }

    public int compare(Object a, Object b) {
        if (a == b) {
            return 0;
        }
        Object[] ax = (Object[]) a;
        Object[] bx = (Object[]) b;
        int al = ax.length;
        int bl = bx.length;
        int len = Math.min(al, bl);
        for (int i = 0; i < len; i++) {
            int comp = types[i].compare(ax[i], bx[i]);
            if (comp != 0) {
                return comp;
            }
        }
        if (len < al) {
            return -1;
        } else if (len < bl) {
            return 1;
        }
        return 0;
    }

    public int length(Object obj) {
        Object[] x = (Object[]) obj;
        int len = x.length;
        int result = DataUtils.getVarIntLen(len);
        for (int i = 0; i < len; i++) {
            result += types[i].length(x[i]);
        }
        return result;
    }

    public int getMaxLength(Object obj) {
        Object[] x = (Object[]) obj;
        int len = x.length;
        int result = DataUtils.MAX_VAR_INT_LEN;
        for (int i = 0; i < len; i++) {
            result += types[i].getMaxLength(x[i]);
        }
        return result;
    }

    public int getMemory(Object obj) {
        Object[] x = (Object[]) obj;
        int len = x.length;
        int memory = 0;
        for (int i = 0; i < len; i++) {
            memory += types[i].getMemory(x[i]);
        }
        return memory;
    }

    public Object[] read(ByteBuffer buff) {
        int len = DataUtils.readVarInt(buff);
        Object[] x = new Object[len];
        for (int i = 0; i < len; i++) {
            x[i] = types[i].read(buff);
        }
        return x;
    }

    public void write(ByteBuffer buff, Object obj) {
        Object[] x = (Object[]) obj;
        int len = x.length;
        DataUtils.writeVarInt(buff, len);
        for (int i = 0; i < len; i++) {
            types[i].write(buff, x[i]);
        }
    }

    public String asString() {
        StringBuilder buff = new StringBuilder();
        buff.append('r');
        buff.append('(');
        for (int i = 0; i < types.length; i++) {
            if (i > 0) {
                buff.append(',');
            }
            buff.append(types[i].asString());
        }
        buff.append(')');
        return buff.toString();
    }

    /**
     * Convert a row type to a row.
     *
     * @param t the type string
     * @param factory the data type factory
     * @return the row type
     */
    static RowType fromString(String t, MapFactory factory) {
        if (!t.startsWith("r(") || !t.endsWith(")")) {
            throw new RuntimeException("Unknown type: " + t);
        }
        t = t.substring(2, t.length() - 1);
        String[] array = StringUtils.arraySplit(t, ',', false);
        DataType[] types = new DataType[array.length];
        for (int i = 0; i < array.length; i++) {
            types[i] = factory.buildDataType(array[i]);
        }
        return new RowType(types);
    }

}
