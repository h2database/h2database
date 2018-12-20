/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import org.h2.engine.Constants;
import org.h2.util.MathUtils;

/**
 * Base class for collection values.
 */
abstract class ValueCollectionBase extends Value {

    final Value[] values;

    private int hash;

    ValueCollectionBase(Value[] values) {
        this.values = values;
    }

    @Override
    public int hashCode() {
        if (hash != 0) {
            return hash;
        }
        int h = getType();
        for (Value v : values) {
            h = h * 31 + v.hashCode();
        }
        hash = h;
        return h;
    }

    @Override
    public long getPrecision() {
        long p = 0;
        for (Value v : values) {
            p += v.getPrecision();
        }
        return p;
    }

    @Override
    public int getDisplaySize() {
        long size = 0;
        for (Value v : values) {
            size += v.getDisplaySize();
        }
        return MathUtils.convertLongToInt(size);
    }

    @Override
    public int getMemory() {
        int memory = 32;
        for (Value v : values) {
            memory += v.getMemory() + Constants.MEMORY_POINTER;
        }
        return memory;
    }

}
