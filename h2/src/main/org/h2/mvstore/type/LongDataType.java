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
public final class LongDataType extends BasicDataType<Long>
{
    public static final LongDataType INSTANCE = new LongDataType();

    public LongDataType() {}

    @Override
    public int getMemory(Long obj) {
        return 8;
    }

    @Override
    public void write(WriteBuffer buff, Long data) {
        buff.putVarLong(data);
    }

    @Override
    public Long read(ByteBuffer buff) {
        return DataUtils.readVarLong(buff);
    }

    @Override
    public Long[] createStorage(int size) {
        return new Long[size];
    }

    @Override
    public int compare(Long one, Long two) {
        return Long.compare(one, two);
    }
}
