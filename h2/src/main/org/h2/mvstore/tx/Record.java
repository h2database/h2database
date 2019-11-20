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
import org.h2.mvstore.type.DataType;
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
    final VersionedValue oldValue;

    Record(int mapId, Object key, VersionedValue oldValue) {
        this.mapId = mapId;
        this.key = key;
        this.oldValue = oldValue;
    }

    @Override
    public String toString() {
        return "mapId=" + mapId + ", key=" + key + ", value=" + oldValue;
    }

    /**
     * A data type for undo log values
     */
    static final class Type implements DataType
    {
        private final TransactionStore transactionStore;

        Type(TransactionStore transactionStore) {
            this.transactionStore = transactionStore;
        }

        @Override
        public int getMemory(Object obj) {
            int result = Constants.MEMORY_OBJECT + 4 + 3 * Constants.MEMORY_POINTER;
            Record record = (Record) obj;
            if (record.mapId >= 0) {
                MVMap<Object, VersionedValue> map = transactionStore.getMap(record.mapId);
                result += map.getKeyType().getMemory(record.key) +
                            map.getValueType().getMemory(record.oldValue);
            }
            return result;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            Record record = (Record) obj;
            buff.putVarInt(record.mapId);
            if (record.mapId >= 0) {
                MVMap<Object, VersionedValue> map = transactionStore.getMap(record.mapId);
                map.getKeyType().write(buff, record.key);
                VersionedValue oldValue = record.oldValue;
                if (oldValue == null) {
                    buff.put((byte) 0);
                } else {
                    buff.put((byte) 1);
                    map.getValueType().write(buff, oldValue);
                }
            }
        }

        @Override
        public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
            for (int i = 0; i < len; i++) {
                write(buff, obj[i]);
            }
        }

        @Override
        public Record read(ByteBuffer buff) {
            int mapId = DataUtils.readVarInt(buff);
            if (mapId < 0) {
                return new Record(-1, null, null);
            }
            MVMap<Object, VersionedValue> map = transactionStore.getMap(mapId);
            Object key = map.getKeyType().read(buff);
            VersionedValue oldValue = null;
            if (buff.get() == 1) {
                oldValue = (VersionedValue)map.getValueType().read(buff);
            }
            return new Record(mapId, key, oldValue);
        }

        @Override
        public void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
            for (int i = 0; i < len; i++) {
                obj[i] = read(buff);
            }
        }
    }
}
