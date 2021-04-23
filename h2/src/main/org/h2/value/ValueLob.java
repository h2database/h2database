/*
 * Copyright 2004-2021 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (https://h2database.com/html/license.html). Initial Developer: H2
 * Group
 */
package org.h2.value;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import org.h2.engine.CastDataProvider;
import org.h2.engine.Constants;
import org.h2.message.DbException;
import org.h2.store.DataHandler;
import org.h2.store.RangeInputStream;
import org.h2.store.RangeReader;
import org.h2.util.Bits;
import org.h2.util.IOUtils;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * A implementation of the BINARY LARGE OBJECT and CHARACTER LARGE OBJECT data
 * types. Small objects are kept in memory and stored in the record. Large
 * objects are either stored in the database, or in temporary files.
 */
public final class ValueLob extends Value {

    private static final int BLOCK_COMPARISON_SIZE = 512;

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
    private static Reader rangeReader(Reader reader, long oneBasedOffset, long length, long dataSize) {
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

    /**
     * Compares LOBs of the same type.
     *
     * @param v1 first LOB value
     * @param v2 second LOB value
     * @return result of comparison
     */
    protected static int compare(ValueLob v1, ValueLob v2) {
        int valueType = v1.getValueType();
        assert valueType == v2.getValueType();
        long minPrec = Math.min(v1.getType().getPrecision(), v2.getType().getPrecision());
        if (valueType == Value.BLOB) {
            try (InputStream is1 = v1.getInputStream(); InputStream is2 = v2.getInputStream()) {
                byte[] buf1 = new byte[BLOCK_COMPARISON_SIZE];
                byte[] buf2 = new byte[BLOCK_COMPARISON_SIZE];
                for (; minPrec >= BLOCK_COMPARISON_SIZE; minPrec -= BLOCK_COMPARISON_SIZE) {
                    if (IOUtils.readFully(is1, buf1, BLOCK_COMPARISON_SIZE) != BLOCK_COMPARISON_SIZE
                            || IOUtils.readFully(is2, buf2, BLOCK_COMPARISON_SIZE) != BLOCK_COMPARISON_SIZE) {
                        throw DbException.getUnsupportedException("Invalid LOB");
                    }
                    int cmp = Bits.compareNotNullUnsigned(buf1, buf2);
                    if (cmp != 0) {
                        return cmp;
                    }
                }
                for (;;) {
                    int c1 = is1.read(), c2 = is2.read();
                    if (c1 < 0) {
                        return c2 < 0 ? 0 : -1;
                    }
                    if (c2 < 0) {
                        return 1;
                    }
                    if (c1 != c2) {
                        return (c1 & 0xFF) < (c2 & 0xFF) ? -1 : 1;
                    }
                }
            } catch (IOException ex) {
                throw DbException.convert(ex);
            }
        } else {
            try (Reader reader1 = v1.getReader(); Reader reader2 = v2.getReader()) {
                char[] buf1 = new char[BLOCK_COMPARISON_SIZE];
                char[] buf2 = new char[BLOCK_COMPARISON_SIZE];
                for (; minPrec >= BLOCK_COMPARISON_SIZE; minPrec -= BLOCK_COMPARISON_SIZE) {
                    if (IOUtils.readFully(reader1, buf1, BLOCK_COMPARISON_SIZE) != BLOCK_COMPARISON_SIZE
                            || IOUtils.readFully(reader2, buf2, BLOCK_COMPARISON_SIZE) != BLOCK_COMPARISON_SIZE) {
                        throw DbException.getUnsupportedException("Invalid LOB");
                    }
                    int cmp = Bits.compareNotNull(buf1, buf2);
                    if (cmp != 0) {
                        return cmp;
                    }
                }
                for (;;) {
                    int c1 = reader1.read(), c2 = reader2.read();
                    if (c1 < 0) {
                        return c2 < 0 ? 0 : -1;
                    }
                    if (c2 < 0) {
                        return 1;
                    }
                    if (c1 != c2) {
                        return c1 < c2 ? -1 : 1;
                    }
                }
            } catch (IOException ex) {
                throw DbException.convert(ex);
            }
        }
    }

    /**
     * the value type (Value.BLOB or CLOB)
     */
    protected final int valueType;

    private TypeInfo type;

    /**
     * Length in characters for character large objects or length in bytes for
     * binary large objects.
     */
    protected long precision;

    /**
     * Length in characters for binary large objects or length in bytes for
     * character large objects.
     */
    volatile long otherPrecision = -1L;

    /**
     * Cache the hashCode because it can be expensive to compute.
     */
    private int hash;
    
    final ValueLobStrategy fetchStrategy;

    protected ValueLob(int type, long precision, ValueLobStrategy fetchStrategy) {
        this.valueType = type;
        this.precision = precision;
        this.fetchStrategy = fetchStrategy;
    }

    /**
     * Check if this value is linked to a specific table. For values that are
     * kept fully in memory, this method returns false.
     *
     * @return true if it is
     */
    public boolean isLinkedToTable() {
        return fetchStrategy.isLinkedToTable();
    }

    /**
     * Remove the underlying resource, if any. For values that are kept fully in
     * memory this method has no effect.
     */
    public void remove() {
        fetchStrategy.remove(this);
    }

    /**
     * Copy a large value, to be used in the given table. For values that are
     * kept fully in memory this method has no effect.
     *
     * @param database the data handler
     * @param tableId the table where this object is used
     * @return the new value or itself
     */
    public ValueLob copy(DataHandler database, int tableId) {
        return fetchStrategy.copy(this, database, tableId);
    }

    @Override
    public TypeInfo getType() {
        TypeInfo type = this.type;
        if (type == null) {
            this.type = type = new TypeInfo(valueType, precision, 0, null);
        }
        return type;
    }

    @Override
    public int getValueType() {
        return valueType;
    }

    @Override
    public String getString() {
        return fetchStrategy.getString(this);
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
    public Reader getReader(long oneBasedOffset, long length) {
        return rangeReader(getReader(), oneBasedOffset, length, valueType == Value.CLOB ? precision : -1);
    }

    @Override
    public byte[] getBytes() {
        byte[] b = fetchStrategy.getBytes();
        if (b != null) {
            return b;
        }
        if (valueType == ValueLob.BLOB) {
            if (precision > Constants.MAX_STRING_LENGTH) {
                throw getBinaryTooLong(precision);
            }
            return readBytes((int) precision);
        }
        long p = otherPrecision;
        if (p >= 0L) {
            if (p > Constants.MAX_STRING_LENGTH) {
                throw getBinaryTooLong(p);
            }
            return readBytes((int) p);
        }
        if (precision > Constants.MAX_STRING_LENGTH) {
            throw getBinaryTooLong(octetLength());
        }
        b = readBytes(Integer.MAX_VALUE);
        otherPrecision = p = b.length;
        if (p > Constants.MAX_STRING_LENGTH) {
            throw getBinaryTooLong(p);
        }
        return b;
    }

    @Override
    public byte[] getBytesNoCopy() {
        byte[] b = fetchStrategy.getBytesNoCopy();
        if (b != null) {
            return b;
        }
        return super.getBytesNoCopy();
    }

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
    public InputStream getInputStream() {
        return fetchStrategy.getInputStream(this);
    }

    @Override
    public InputStream getInputStream(long oneBasedOffset, long length) {
        return fetchStrategy.getInputStream(this, oneBasedOffset, length);
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
    public int compareTypeSafe(Value v, CompareMode mode, CastDataProvider provider) {
        return fetchStrategy.compareTypeSafe(this, v, mode, provider);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return fetchStrategy.getSQL(this, builder, sqlFlags);
    }

    /**
     * Returns the precision.
     *
     * @return the precision
     */
    public long getPrecision() {
        return precision;
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
        return fetchStrategy.getMemory();
    }

    /**
     * Returns the data handler.
     *
     * @return the data handler, or {@code null}
     */
    public DataHandler getDataHandler() {
        return fetchStrategy.getDataHandler();
    }

    /**
     * Create an independent copy of this temporary value. The file will not be
     * deleted automatically.
     *
     * @return the value
     */
    public ValueLob copyToTemp() {
        return this;
    }

    /**
     * Create an independent copy of this value, that will be bound to a result.
     *
     * @return the value (this for small objects)
     */
    public ValueLob copyToResult() {
        return fetchStrategy.copyToResult(this);
    }

    /**
     * Convert the precision to the requested value.
     *
     * @param precision the new precision
     * @return the truncated or this value
     */
    ValueLob convertPrecision(long precision) {
        return fetchStrategy.convertPrecision(this, precision);
    }

    @Override
    public long charLength() {
        return fetchStrategy.charLength(this);
    }

    @Override
    public long octetLength() {
        return fetchStrategy.octetLength(this);
    }
    
    @Override
    public String toString() {
        return fetchStrategy.toString();
    }
    
    public ValueLobStrategy getFetchStrategy() {
        return fetchStrategy;
    }

}
