/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.type;

import org.h2.mvstore.WriteBuffer;
import java.nio.ByteBuffer;

/**
 * Class BasicDataType.
 * <UL>
 * <LI> 8/7/19 5:28 PM initial creation
 * </UL>
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public abstract class BasicDataType<T> implements DataType {

    @Override
    public abstract int getMemory(Object obj);

    @Override
    public abstract void write(WriteBuffer buff, Object obj);

    @Override
    public abstract Object read(ByteBuffer buff);


    @Override
    public int compare(Object a, Object b) {
        if (a == b) {
            return 0;
        } else if (a == null) {
            return -1;
        } else if (b == null) {
            return 1;
        }
        throw new UnsupportedOperationException();
    }


    @Override
    public final void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            write(buff, obj[i]);
        }
    }

    @Override
    public final void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            obj[i] = read(buff);
        }
    }
}
