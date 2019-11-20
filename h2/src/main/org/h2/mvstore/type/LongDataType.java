package org.h2.mvstore.type;

import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;

import java.nio.ByteBuffer;

/**
 * Class LongDataType.
 * <UL>
 * <LI> 8/21/17 6:52 PM initial creation
 * </UL>
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public final class LongDataType implements DataType {

    public static final LongDataType INSTANCE = new LongDataType();

    public LongDataType() {}

    @Override
    public int getMemory(Object obj) {
        return 8;
    }

    @Override
    public void write(WriteBuffer buff, Object obj) {
        Long data = (Long)obj;
        buff.putVarLong(data);
    }

    @Override
    public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            write(buff, obj[i]);
        }
    }

    @Override
    public Object read(ByteBuffer buff) {
        return DataUtils.readVarLong(buff);
    }

    @Override
    public void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            obj[i] = read(buff);
        }
    }

    @Override
    public int compare(Object aObj, Object bObj) {
        if (aObj instanceof Long && bObj instanceof Long) {
            Long a = (Long) aObj;
            Long b = (Long) bObj;
            return Long.compare(a,b);
        }
        return compareWithNulls(aObj, bObj);
    }

    @SuppressWarnings("unchecked")
    private static long[] cast(Object storage) {
        return (long[])storage;
    }

    private static int compareWithNulls(Object a, Object b) {
        if (a == b) {
            return 0;
        } else if (a == null) {
            return -1;
        } else if (b == null) {
            return 1;
        }
        throw new UnsupportedOperationException();
    }
}
