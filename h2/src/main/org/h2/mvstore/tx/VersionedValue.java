/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import org.h2.engine.Constants;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;
import java.nio.ByteBuffer;

/**
 * A versioned value (possibly null).
 * It contains current value and latest committed value if current one is uncommitted.
 * Also for uncommitted values it contains operationId - a combination of
 * transactionId and logId.
 */
public class VersionedValue {

    public static final VersionedValue DUMMY = new VersionedValue(new Object());

    /**
     * The current value.
     */
    public final Object value;

    static VersionedValue getInstance(Object value) {
        assert value != null;
        return new VersionedValue(value);
    }

    public static VersionedValue getInstance(long operationId, Object value, Object committedValue) {
        return new Uncommitted(operationId, value, committedValue);
    }

    VersionedValue(Object value) {
        this.value = value;
    }

    public boolean isCommitted() {
        return true;
    }

    public long getOperationId() {
        return 0L;
    }

    public Object getCommittedValue() {
        return value;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }

    private static class Uncommitted extends VersionedValue
    {
        private final long   operationId;
        private final Object committedValue;

        Uncommitted(long operationId, Object value, Object committedValue) {
            super(value);
            assert operationId != 0;
            this.operationId = operationId;
            this.committedValue = committedValue;
        }

        @Override
        public boolean isCommitted() {
            return false;
        }

        @Override
        public long getOperationId() {
            return operationId;
        }

        @Override
        public Object getCommittedValue() {
            return committedValue;
        }

        @Override
        public String toString() {
            return super.toString() +
                    " " + TransactionStore.getTransactionId(operationId) + "/" +
                    TransactionStore.getLogId(operationId) + " " + committedValue;
        }
    }

    /**
     * The value type for a versioned value.
     */
    public static class Type implements DataType {

        private final DataType valueType;

        public Type(DataType valueType) {
            this.valueType = valueType;
        }

        @Override
        public int getMemory(Object obj) {
            if(obj == null) return 0;
            VersionedValue v = (VersionedValue) obj;
            int res = Constants.MEMORY_OBJECT + 8 + 2 * Constants.MEMORY_POINTER +
                    getValMemory(v.value);
            if (v.getOperationId() != 0) {
                res += getValMemory(v.getCommittedValue());
            }
            return res;
        }

        private int getValMemory(Object obj) {
            return obj == null ? 0 : valueType.getMemory(obj);
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj == bObj) {
                return 0;
            }
            VersionedValue a = (VersionedValue) aObj;
            VersionedValue b = (VersionedValue) bObj;
            long comp = a.getOperationId() - b.getOperationId();
            if (comp == 0) {
                return valueType.compare(a.value, b.value);
            }
            return Long.signum(comp);
        }

        @Override
        public void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
            if (buff.get() == 0) {
                // fast path (no op ids or null entries)
                for (int i = 0; i < len; i++) {
                    obj[i] = new VersionedValue(valueType.read(buff));
                }
            } else {
                // slow path (some entries may be null)
                for (int i = 0; i < len; i++) {
                    obj[i] = read(buff);
                }
            }
        }

        @Override
        public Object read(ByteBuffer buff) {
            long operationId = DataUtils.readVarLong(buff);
            if (operationId == 0) {
                return new VersionedValue(valueType.read(buff));
            } else {
                byte flags = buff.get();
                Object value = (flags & 1) != 0 ? valueType.read(buff) : null;
                Object committedValue = (flags & 2) != 0 ? valueType.read(buff) : null;
                return new Uncommitted(operationId, value, committedValue);
            }
        }

        @Override
        public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
            boolean fastPath = true;
            for (int i = 0; i < len; i++) {
                VersionedValue v = (VersionedValue) obj[i];
                if (v.getOperationId() != 0 || v.value == null) {
                    fastPath = false;
                }
            }
            if (fastPath) {
                buff.put((byte) 0);
                for (int i = 0; i < len; i++) {
                    VersionedValue v = (VersionedValue) obj[i];
                    valueType.write(buff, v.value);
                }
            } else {
                // slow path:
                // store op ids, and some entries may be null
                buff.put((byte) 1);
                for (int i = 0; i < len; i++) {
                    write(buff, obj[i]);
                }
            }
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            VersionedValue v = (VersionedValue) obj;
            long operationId = v.getOperationId();
            buff.putVarLong(operationId);
            if (operationId == 0) {
                valueType.write(buff, v.value);
            } else {
                Object committedValue = v.getCommittedValue();
                int flags = (v.value == null ? 0 : 1) | (committedValue == null ? 0 : 2);
                buff.put((byte) flags);
                if (v.value != null) {
                    valueType.write(buff, v.value);
                }
                if (committedValue != null) {
                    valueType.write(buff, committedValue);
                }
            }
        }
    }
}
