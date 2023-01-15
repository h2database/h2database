/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * Dummy data type used when no value is required. This data type doesn't use
 * any disk space and always returns SQL NULL value.
 */
public final class NullValueDataType implements DataType<Value> {

    /**
     * Dummy data type instance.
     */
    public static final NullValueDataType INSTANCE = new NullValueDataType();

    private NullValueDataType() {
    }

    @Override
    public int compare(Value a, Value b) {
        return 0;
    }

    @Override
    public int binarySearch(Value key, Object storage, int size, int initialGuess) {
        return 0;
    }

    @Override
    public int getMemory(Value obj) {
        return 0;
    }

    @Override
    public boolean isMemoryEstimationAllowed() {
        return true;
    }

    @Override
    public void write(WriteBuffer buff, Value obj) {
    }

    @Override
    public void write(WriteBuffer buff, Object storage, int len) {
    }

    @Override
    public Value read(ByteBuffer buff) {
        return ValueNull.INSTANCE;
    }

    @Override
    public void read(ByteBuffer buff, Object storage, int len) {
        Arrays.fill((Value[]) storage, 0, len, ValueNull.INSTANCE);
    }

    @Override
    public Value[] createStorage(int size) {
        return new Value[size];
    }

}
