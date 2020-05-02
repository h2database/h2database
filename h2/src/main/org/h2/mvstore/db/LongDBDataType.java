/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.LongDataType;
import java.nio.ByteBuffer;

/**
 * Class LongDBDataType provides version of LongDataType which is backward compatible
 * with the way ValueDataType serializes Long values.
 * Backward compatibility aside, LongDataType could have been used instead.
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public class LongDBDataType extends LongDataType {

    public static final LongDBDataType INSTANCE = new LongDBDataType();
    private static final ValueDataType DUMMY = new ValueDataType();

    public LongDBDataType() {}

    @Override
    public void write(WriteBuffer buff, Long data) {
        ValueDataType.writeLong(buff, data);
    }

    @Override
    public Long read(ByteBuffer buff) {
        return DUMMY.read(buff).getLong();
    }
}
