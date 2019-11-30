package org.h2.mvstore.type;

import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.db.ValueDataType;

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
    private static final ValueDataType DUMMY = new ValueDataType();

    public LongDataType() {}

    @Override
    public int getMemory(Long obj) {
        return 8;
    }

    @Override
    public void write(WriteBuffer buff, Long data) {
        //TODO: switch to compact format when format backward-compatibility is not required
//        buff.putVarLong(data);
        ValueDataType.writeLong(buff, data);
    }

    @Override
    public Long read(ByteBuffer buff) {
        //TODO: switch to compact format when format backward-compatibility is not required
//        return DataUtils.readVarLong(buff);
        return DUMMY.read(buff).getLong();
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
