/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.type;

import java.nio.ByteBuffer;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;

/**
 * The base class for data type implementations.
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public abstract class BasicDataType<T> implements DataType<T> {

    @Override
    public abstract int getMemory(T obj);

    @Override
    public abstract void write(WriteBuffer buff, T obj);

    @Override
    public abstract T read(ByteBuffer buff);

    @Override
    public int compare(T a, T b) {
        throw DataUtils.newUnsupportedOperationException("Can not compare");
    }

    @Override
    public boolean isMemoryEstimationAllowed() {
        return true;
    }

    @Override
    public int binarySearch(T key, Object storageObj, int size, int initialGuess) {
        T[] storage = cast(storageObj);
        int low = 0;
        int high = size - 1;
        // the cached index minus one, so that
        // for the first time (when cachedCompare is 0),
        // the default value is used
        int x = initialGuess - 1;
        if (x < 0 || x > high) {
            x = high >>> 1;
        }
        while (low <= high) {
            int compare = compare(key, storage[x]);
            if (compare > 0) {
                low = x + 1;
            } else if (compare < 0) {
                high = x - 1;
            } else {
                return x;
            }
            x = (low + high) >>> 1;
        }
        return ~low;
    }

    @Override
    public void write(WriteBuffer buff, Object storage, int len) {
        for (int i = 0; i < len; i++) {
            write(buff, cast(storage)[i]);
        }
    }

    @Override
    public void read(ByteBuffer buff, Object storage, int len) {
        for (int i = 0; i < len; i++) {
            cast(storage)[i] = read(buff);
        }
    }

    @Override
    public int hashCode() {
        return getClass().getName().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj != null && getClass().equals(obj.getClass());
    }

    /**
     * Cast the storage object to an array of type T.
     *
     * @param storage the storage object
     * @return the array
     */
    @SuppressWarnings("unchecked")
    protected final T[] cast(Object storage) {
        return (T[])storage;
    }
}
