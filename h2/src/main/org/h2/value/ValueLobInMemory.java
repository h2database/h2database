/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (https://h2database.com/html/license.html). Initial Developer: H2
 * Group
 */
package org.h2.value;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.h2.engine.CastDataProvider;
import org.h2.message.DbException;
import org.h2.store.DataHandler;
import org.h2.store.LobStorageInterface;
import org.h2.util.Bits;
import org.h2.util.IOUtils;
import org.h2.util.MathUtils;
import org.h2.util.Utils;

/**
 * A implementation of the BLOB and CLOB data types. Small objects are kept in
 * memory and stored in the record. Large objects are either stored in the
 * database, or in temporary files.
 */
public final class ValueLobInMemory extends ValueLob {

    /**
     * If the LOB is below the inline size, we just store/load it directly here.
     */
    private final byte[] small;

    private ValueLobInMemory(int type, byte[] small, long precision) {
        super(type, precision);
        if (small == null) {
            throw new IllegalStateException();
        }
        this.small = small;
    }

    /**
     * Copy a large value, to be used in the given table. For values that are
     * kept fully in memory this method has no effect.
     *
     * @param database the data handler
     * @param tableId the table where this object is used
     * @return the new value or itself
     */
    @Override
    public ValueLob copy(DataHandler database, int tableId) {
        if (small.length > database.getMaxLengthInplaceLob()) {
            LobStorageInterface s = database.getLobStorage();
            ValueLob v;
            if (valueType == Value.BLOB) {
                v = s.createBlob(getInputStream(), precision);
            } else {
                v = s.createClob(getReader(), precision);
            }
            ValueLob v2 = v.copy(database, tableId);
            v.remove();
            return v2;
        }
        return this;
    }

    @Override
    public String getString() {
        return new String(small, StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getBytes() {
        return Utils.cloneByteArray(small);
    }

    @Override
    public byte[] getBytesNoCopy() {
        return small;
    }

    @Override
    public int compareTypeSafe(Value v, CompareMode mode, CastDataProvider provider) {
        if (v == this) {
            return 0;
        }
        ValueLobInMemory v2 = (ValueLobInMemory) v;
        if (v2 != null) {
            if (valueType == Value.BLOB) {
                return Bits.compareNotNullSigned(small, v2.small);
            } else {
                return Integer.signum(getString().compareTo(v2.getString()));
            }
        }
        return compare(this, v2);
    }

    @Override
    public InputStream getInputStream() {
        return new ByteArrayInputStream(small);
    }

    @Override
    public InputStream getInputStream(long oneBasedOffset, long length) {
        final long byteCount = (valueType == Value.BLOB) ? precision : -1;
        return rangeInputStream(getInputStream(), oneBasedOffset, length, byteCount);
    }

    /**
     * Get the data if this a small lob value.
     *
     * @return the data
     */
    public byte[] getSmall() {
        return small;
    }

    @Override
    public int getMemory() {
        /*
         * Java 11 with -XX:-UseCompressedOops 0 bytes: 120 bytes 1 byte: 128
         * bytes
         */
        return small.length + 127;
    }

    /**
     * Convert the precision to the requested value.
     *
     * @param precision the new precision
     * @return the truncated or this value
     */
    @Override
    ValueLob convertPrecision(long precision) {
        if (this.precision <= precision) {
            return this;
        }
        ValueLob lob;
        if (valueType == CLOB) {
            try {
                int p = MathUtils.convertLongToInt(precision);
                String s = IOUtils.readStringAndClose(getReader(), p);
                byte[] data = s.getBytes(StandardCharsets.UTF_8);
                lob = createSmallLob(valueType, data, s.length());
            } catch (IOException e) {
                throw DbException.convertIOException(e, null);
            }
        } else {
            try {
                int p = MathUtils.convertLongToInt(precision);
                byte[] data = IOUtils.readBytesAndClose(getInputStream(), p);
                lob = createSmallLob(valueType, data, data.length);
            } catch (IOException e) {
                throw DbException.convertIOException(e, null);
            }
        }
        return lob;
    }

    /**
     * Create a LOB object that fits in memory.
     *
     * @param type the type (Value.BLOB or CLOB)
     * @param small the byte array
     * @return the LOB
     */
    public static ValueLobInMemory createSmallLob(int type, byte[] small) {
        int precision;
        if (type == Value.CLOB) {
            precision = new String(small, StandardCharsets.UTF_8).length();
        } else {
            precision = small.length;
        }
        return createSmallLob(type, small, precision);
    }

    /**
     * Create a LOB object that fits in memory.
     *
     * @param type the type (Value.BLOB or CLOB)
     * @param small the byte array
     * @param precision the precision
     * @return the LOB
     */
    public static ValueLobInMemory createSmallLob(int type, byte[] small, long precision) {
        return new ValueLobInMemory(type, small, precision);
    }
}
