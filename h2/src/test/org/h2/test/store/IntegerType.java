/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.nio.ByteBuffer;
import org.h2.dev.store.btree.DataType;
import org.h2.dev.store.btree.DataUtils;

/**
 * An integer type.
 */
class IntegerType implements DataType {

    public int compare(Object a, Object b) {
        return ((Integer) a).compareTo((Integer) b);
    }

    public int length(Object obj) {
        return DataUtils.getVarIntLen((Integer) obj);
    }

    public int getMaxLength(Object obj) {
        return DataUtils.MAX_VAR_INT_LEN;
    }

    public int getMemory(Object obj) {
        return 20;
    }

    public Integer read(ByteBuffer buff) {
        return DataUtils.readVarInt(buff);
    }

    public void write(ByteBuffer buff, Object x) {
        DataUtils.writeVarInt(buff, (Integer) x);
    }

    public String asString() {
        return "i";
    }

}

