/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.type;

import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;

import java.nio.ByteBuffer;

/**
 * Class IntegerDataType.
 * <UL>
 * <LI> 4/10/24 1:18 PM initial creation
 * </UL>
 *
 * @author <a href="mailto:andrei.tokar@gmail.com">Andrei Tokar</a>
 */
public class IntegerDataType extends BasicDataType<Integer> {

    public static final IntegerDataType INSTANCE = new IntegerDataType();

    private static final Integer[] EMPTY_INTEGER_ARR = new Integer[0];

    private IntegerDataType() {}

    @Override
    public int getMemory(Integer obj) {
        return 4;
    }

    @Override
    public void write(WriteBuffer buff, Integer data) {
        buff.putVarInt(data);
    }

    @Override
    public Integer read(ByteBuffer buff) {
        return DataUtils.readVarInt(buff);
    }

    @Override
    public Integer[] createStorage(int size) {
        return size == 0 ? EMPTY_INTEGER_ARR : new Integer[size];
    }

    @Override
    public int compare(Integer one, Integer two) {
        return Integer.compare(one, two);
    }

    @Override
    public int binarySearch(Integer keyObj, Object storageObj, int size, int initialGuess) {
        int key = keyObj;
        Integer[] storage = cast(storageObj);
        int low = 0;
        int high = size - 1;
        // the cached index minus one, so that
        // for the first time (when cachedCompare is 0),
        // the default value is used
        int x = initialGuess - 1;
        if (x < 0 || x > high) {
            x = high >>> 1;
        }
        return binarySearch(key, storage, low, high, x);
    }

    private static int binarySearch(int key, Integer[] storage, int low, int high, int x) {
        while (low <= high) {
            int midVal = storage[x];
            if (key > midVal) {
                low = x + 1;
            } else if (key < midVal) {
                high = x - 1;
            } else {
                return x;
            }
            x = (low + high) >>> 1;
        }
        return ~low;
    }
}
