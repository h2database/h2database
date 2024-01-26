/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import java.nio.ByteBuffer;
import org.h2.engine.Constants;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.BasicDataType;
import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.MetaType;
import org.h2.mvstore.type.StatefulDataType;
import org.h2.value.VersionedValue;

/**
 * The value type for a versioned value.
 */
public class VersionedValueType<T,D> extends BasicDataType<VersionedValue<T>> implements StatefulDataType<D> {

    private final DataType<T> valueType;
    private final Factory<D> factory = new Factory<>();


    public VersionedValueType(DataType<T> valueType) {
        this.valueType = valueType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public VersionedValue<T>[] createStorage(int size) {
        return new VersionedValue[size];
    }

    @Override
    public int getMemory(VersionedValue<T> v) {
        if(v == null) return 0;
        int res = Constants.MEMORY_OBJECT + 8 + 2 * Constants.MEMORY_POINTER +
                getValMemory(v.getCurrentValue());
        if (v.getOperationId() != 0) {
            res += getValMemory(v.getCommittedValue());
        }
        return res;
    }

    private int getValMemory(T obj) {
        return obj == null ? 0 : valueType.getMemory(obj);
    }

    @Override
    public void read(ByteBuffer buff, Object storage, int len) {
        if (buff.get() == 0) {
            // fast path (no op ids or null entries)
            for (int i = 0; i < len; i++) {
                cast(storage)[i] = VersionedValueCommitted.getInstance(valueType.read(buff));
            }
        } else {
            // slow path (some entries may be null)
            for (int i = 0; i < len; i++) {
                cast(storage)[i] = read(buff);
            }
        }
    }

    @Override
    public VersionedValue<T> read(ByteBuffer buff) {
        long operationId = DataUtils.readVarLong(buff);
        if (operationId == 0) {
            return VersionedValueCommitted.getInstance(valueType.read(buff));
        } else {
            byte flags = buff.get();
            T value = (flags & 1) != 0 ? valueType.read(buff) : null;
            T committedValue = (flags & 2) != 0 ? valueType.read(buff) : null;
            return VersionedValueUncommitted.getInstance(operationId, value, committedValue);
        }
    }

    @Override
    public void write(WriteBuffer buff, Object storage, int len) {
        boolean fastPath = true;
        for (int i = 0; i < len; i++) {
            VersionedValue<T> v = cast(storage)[i];
            if (v.getOperationId() != 0 || v.getCurrentValue() == null) {
                fastPath = false;
            }
        }
        if (fastPath) {
            buff.put((byte) 0);
            for (int i = 0; i < len; i++) {
                VersionedValue<T> v = cast(storage)[i];
                valueType.write(buff, v.getCurrentValue());
            }
        } else {
            // slow path:
            // store op ids, and some entries may be null
            buff.put((byte) 1);
            for (int i = 0; i < len; i++) {
                write(buff, cast(storage)[i]);
            }
        }
    }

    @Override
    public void write(WriteBuffer buff, VersionedValue<T> v) {
        long operationId = v.getOperationId();
        buff.putVarLong(operationId);
        if (operationId == 0) {
            valueType.write(buff, v.getCurrentValue());
        } else {
            T committedValue = v.getCommittedValue();
            int flags = (v.getCurrentValue() == null ? 0 : 1) | (committedValue == null ? 0 : 2);
            buff.put((byte) flags);
            if (v.getCurrentValue() != null) {
                valueType.write(buff, v.getCurrentValue());
            }
            if (committedValue != null) {
                valueType.write(buff, committedValue);
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (!(obj instanceof VersionedValueType)) {
            return false;
        }
        VersionedValueType<T,D> other = (VersionedValueType<T,D>) obj;
        return valueType.equals(other.valueType);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ valueType.hashCode();
    }

    @Override
    public void save(WriteBuffer buff, MetaType<D> metaType) {
        metaType.write(buff, valueType);
    }

    @Override
    public int compare(VersionedValue<T> a, VersionedValue<T> b) {
        return valueType.compare(a.getCurrentValue(), b.getCurrentValue());
    }

    @Override
    public Factory<D> getFactory() {
        return factory;
    }

    public static final class Factory<D> implements StatefulDataType.Factory<D> {
        @SuppressWarnings("unchecked")
        @Override
        public DataType<?> create(ByteBuffer buff, MetaType<D> metaType, D database) {
            DataType<VersionedValue<?>> valueType = (DataType<VersionedValue<?>>)metaType.read(buff);
            return new VersionedValueType<VersionedValue<?>,D>(valueType);
        }
    }
}
