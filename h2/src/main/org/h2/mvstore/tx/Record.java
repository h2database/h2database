/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import org.h2.engine.Constants;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.BasicDataType;
import org.h2.value.VersionedValue;
import java.nio.ByteBuffer;

/**
 * Class Record is a value for undoLog.
 * It contains information about a single change of some map.
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
final class Record
{
    /**
     * Map id for this change is related to
     */
    final int mapId;

    /**
     * Key of the changed map entry key
     */
    final Object key;

    /**
     * Value of the entry before change.
     * It is null if entry did not exist before the change (addition).
     */
    final VersionedValue<Object> oldValue;

    @SuppressWarnings("unchecked")
    Record(int mapId, Object key, VersionedValue<?> oldValue) {
        this.mapId = mapId;
        this.key = key;
        this.oldValue = (VersionedValue<Object>)oldValue;
    }

    @Override
    public String toString() {
        return "mapId=" + mapId + ", key=" + key + ", value=" + oldValue;
    }

    /**
     * A data type for undo log values
     */
    static final class Type extends BasicDataType<Record>
    {
        private final TransactionStore transactionStore;

        Type(TransactionStore transactionStore) {
            this.transactionStore = transactionStore;
        }

        @Override
        public int getMemory(Record record) {
            int result = Constants.MEMORY_OBJECT + 4 + 3 * Constants.MEMORY_POINTER;
            if (record.mapId >= 0) {
                MVMap<Object, VersionedValue<Object>> map = transactionStore.getMap(record.mapId);
                result += map.getKeyType().getMemory(record.key) +
                        map.getValueType().getMemory(record.oldValue);
            }
            return result;
        }

        @Override
        public int compare(Record aObj, Record bObj) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void write(WriteBuffer buff, Record record) {
            buff.putVarInt(record.mapId);
            if (record.mapId >= 0) {
                MVMap<Object, VersionedValue<Object>> map = transactionStore.getMap(record.mapId);
                map.getKeyType().write(buff, record.key);
                VersionedValue<Object> oldValue = record.oldValue;
                if (oldValue == null) {
                    buff.put((byte) 0);
                } else {
                    buff.put((byte) 1);
                    map.getValueType().write(buff, oldValue);
                }
            }
        }

        @Override
        public Record read(ByteBuffer buff) {
            int mapId = DataUtils.readVarInt(buff);
            if (mapId < 0) {
                return TransactionStore.COMMIT_MARKER;
            }
            MVMap<Object, VersionedValue<Object>> map = transactionStore.getMap(mapId);
            Object key = map.getKeyType().read(buff);
            VersionedValue<Object> oldValue = null;
            if (buff.get() == 1) {
                oldValue = map.getValueType().read(buff);
            }
            return new Record(mapId, key, oldValue);
        }

        @Override
        public Record[] createStorage(int size) {
            return new Record[size];
        }
    }
}
