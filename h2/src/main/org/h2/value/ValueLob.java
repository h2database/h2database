/*
 * Copyright 2004-2021 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (https://h2database.com/html/license.html). Initial Developer: H2
 * Group
 */
package org.h2.value;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import org.h2.engine.Constants;
import org.h2.message.DbException;
import org.h2.store.DataHandler;
import org.h2.store.LobStorageFrontend;
import org.h2.store.LobStorageInterface;
import org.h2.store.RangeInputStream;
import org.h2.store.RangeReader;
import org.h2.util.IOUtils;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.value.lob.LobData;
import org.h2.value.lob.LobDataDatabase;
import org.h2.value.lob.LobDataInMemory;

/**
 * A implementation of the BINARY LARGE OBJECT and CHARACTER LARGE OBJECT data
 * types. Small objects are kept in memory and stored in the record. Large
 * objects are either stored in the database, or in temporary files.
 */
public abstract class ValueLob extends Value {

    static final int BLOCK_COMPARISON_SIZE = 512;

    private static void rangeCheckUnknown(long zeroBasedOffset, long length) {
        if (zeroBasedOffset < 0) {
            throw DbException.getInvalidValueException("offset", zeroBasedOffset + 1);
        }
        if (length < 0) {
            throw DbException.getInvalidValueException("length", length);
        }
    }

    /**
     * Create an input stream that is s subset of the given stream.
     *
     * @param inputStream the source input stream
     * @param oneBasedOffset the offset (1 means no offset)
     * @param length the length of the result, in bytes
     * @param dataSize the length of the input, in bytes
     * @return the smaller input stream
     */
    protected static InputStream rangeInputStream(InputStream inputStream, long oneBasedOffset, long length,
            long dataSize) {
        if (dataSize > 0) {
            rangeCheck(oneBasedOffset - 1, length, dataSize);
        } else {
            rangeCheckUnknown(oneBasedOffset - 1, length);
        }
        try {
            return new RangeInputStream(inputStream, oneBasedOffset - 1, length);
        } catch (IOException e) {
            throw DbException.getInvalidValueException("offset", oneBasedOffset);
        }
    }

    /**
     * Create a reader that is s subset of the given reader.
     *
     * @param reader the input reader
     * @param oneBasedOffset the offset (1 means no offset)
     * @param length the length of the result, in bytes
     * @param dataSize the length of the input, in bytes
     * @return the smaller input stream
     */
    static Reader rangeReader(Reader reader, long oneBasedOffset, long length, long dataSize) {
        if (dataSize > 0) {
            rangeCheck(oneBasedOffset - 1, length, dataSize);
        } else {
            rangeCheckUnknown(oneBasedOffset - 1, length);
        }
        try {
            return new RangeReader(reader, oneBasedOffset - 1, length);
        } catch (IOException e) {
            throw DbException.getInvalidValueException("offset", oneBasedOffset);
        }
    }

    private TypeInfo type;

    /**
     * Length in characters for character large objects or length in bytes for
     * binary large objects.
     */
    protected long precision;

    final LobData lobData;

    /**
     * Length in characters for binary large objects or length in bytes for
     * character large objects.
     */
    volatile long otherPrecision = -1L;

    /**
     * Cache the hashCode because it can be expensive to compute.
     */
    private int hash;

    ValueLob(long precision, LobData lobData) {
        this.precision = precision;
        this.lobData = lobData;
    }

    /**
     * Check if this value is linked to a specific table. For values that are
     * kept fully in memory, this method returns false.
     *
     * @return true if it is
     */
    public boolean isLinkedToTable() {
        return lobData.isLinkedToTable();
    }

    /**
     * Remove the underlying resource, if any. For values that are kept fully in
     * memory this method has no effect.
     */
    public void remove() {
        lobData.remove(this);
    }

    /**
     * Copy a large value, to be used in the given table. For values that are
     * kept fully in memory this method has no effect.
     *
     * @param database the data handler
     * @param tableId the table where this object is used
     * @return the new value or itself
     */
    public abstract ValueLob copy(DataHandler database, int tableId);

    @Override
    public TypeInfo getType() {
        TypeInfo type = this.type;
        if (type == null) {
            this.type = type = new TypeInfo(getValueType(), precision, 0, null);
        }
        return type;
    }

    DbException getStringTooLong(long precision) {
        return DbException.getValueTooLongException("CHARACTER VARYING", readString(81), precision);
    }

    String readString(int len) {
        try {
            return IOUtils.readStringAndClose(getReader(), len);
        } catch (IOException e) {
            throw DbException.convertIOException(e, toString());
        }
    }

    @Override
    public Reader getReader() {
        return IOUtils.getReader(getInputStream());
    }

    @Override
    public byte[] getBytes() {
        if (lobData instanceof LobDataInMemory) {
            return Utils.cloneByteArray(getSmall());
        }
        return getBytesInternal();
    }

    @Override
    public byte[] getBytesNoCopy() {
        if (lobData instanceof LobDataInMemory) {
            return getSmall();
        }
        return getBytesInternal();
    }

    private byte[] getSmall() {
        byte[] small = ((LobDataInMemory) lobData).getSmall();
        int p = small.length;
        if (p > Constants.MAX_STRING_LENGTH) {
            throw DbException.getValueTooLongException("BINARY VARYING", StringUtils.convertBytesToHex(small, 41), p);
        }
        return small;
    }

    abstract byte[] getBytesInternal();

    DbException getBinaryTooLong(long precision) {
        return DbException.getValueTooLongException("BINARY VARYING", StringUtils.convertBytesToHex(readBytes(41)),
                precision);
    }

    byte[] readBytes(int len) {
        try {
            return IOUtils.readBytesAndClose(getInputStream(), len);
        } catch (IOException e) {
            throw DbException.convertIOException(e, toString());
        }
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            if (precision > 4096) {
                // TODO: should calculate the hash code when saving, and store
                // it in the database file
                return (int) (precision ^ (precision >>> 32));
            }
            hash = Utils.getByteArrayHash(getBytesNoCopy());
        }
        return hash;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ValueLob))
            return false;
        ValueLob otherLob = (ValueLob) other;
        if (hashCode() != otherLob.hashCode())
            return false;
        return compareTypeSafe((Value) other, null, null) == 0;
    }

    @Override
    public int getMemory() {
        return lobData.getMemory();
    }

    public LobData getLobData() {
        return lobData;
    }

    /**
     * Create an independent copy of this value, that will be bound to a result.
     *
     * @return the value (this for small objects)
     */
    public ValueLob copyToResult() {
        if (lobData instanceof LobDataDatabase) {
            LobStorageInterface s = lobData.getDataHandler().getLobStorage();
            if (!s.isReadOnly()) {
                return s.copyLob(this, LobStorageFrontend.TABLE_RESULT, precision);
            }
        }
        return this;
    }

}
