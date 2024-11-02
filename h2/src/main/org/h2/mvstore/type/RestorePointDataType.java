/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: Enno Thieleke
 */
package org.h2.mvstore.type;

import java.nio.ByteBuffer;

import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;
import org.h2.value.ValueBigint;
import org.h2.value.ValueTimestampTimeZone;

public class RestorePointDataType extends BasicDataType<RestorePoint> {

    public static final RestorePointDataType INSTANCE = new RestorePointDataType();

    private static final RestorePoint[] EMPTY_ARRAY = new RestorePoint[0];

    @Override
    public RestorePoint[] createStorage(int size) {
        return size == 0 ? EMPTY_ARRAY : new RestorePoint[size];
    }

    @Override
    public int compare(RestorePoint a, RestorePoint b) {
        return a.compareTo(b);
    }

    @Override
    public int getMemory(RestorePoint obj) {
        // String + long + long + int + long + long
        return StringDataType.INSTANCE.getMemory(obj.getName()) + 36;
    }

    @Override
    public RestorePoint read(ByteBuffer buff) {
        return new RestorePoint(
                DataUtils.readString(buff),
                ValueTimestampTimeZone.fromDateValueAndNanos(buff.getLong(), buff.getLong(), buff.getInt()),
                ValueBigint.get(buff.getLong()),
                ValueBigint.get(buff.getLong())
        );
    }

    @Override
    public void write(WriteBuffer buff, RestorePoint v) {
        StringDataType.INSTANCE.write(buff, v.getName());
        ValueTimestampTimeZone createdAt = v.getCreatedAt();
        buff.putLong(createdAt.getDateValue())
                .putLong(createdAt.getTimeNanos())
                .putInt(createdAt.getTimeZoneOffsetSeconds())
                .putLong(v.getOldestDatabaseVersionToKeep().getLong())
                .putLong(v.getDatabaseVersion().getLong());
    }
}
