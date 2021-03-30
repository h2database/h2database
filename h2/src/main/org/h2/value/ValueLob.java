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
import org.h2.engine.SysProperties;
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
public abstract class ValueLob extends Value {

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

    protected ValueLob(int type, long precision) {
        this.valueType = type;
        this.precision = precision;
    }

    /**
     * Check if this value is linked to a specific table. For values that are
     * kept fully in memory, this method returns false.
     *
     * @return true if it is
     */
    public boolean isLinkedToTable() {
        return false;
    }

    /**
     * Remove the underlying resource, if any. For values that are kept fully in
     * memory this method has no effect.
     */
    public void remove() {}

    /**
     * Copy a large value, to be used in the given table. For values that are
     * kept fully in memory this method has no effect.
     *
     * @param database the data handler
     * @param tableId the table where this object is used
     * @return the new value or itself
     */
    public ValueLob copy(DataHandler database, int tableId) {
        throw new UnsupportedOperationException();
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
        if (valueType == CLOB) {
            if (precision > Constants.MAX_STRING_LENGTH) {
                throw getStringTooLong(precision);
            }
            return readString((int) precision);
        }
        long p = otherPrecision;
        if (p >= 0L) {
            if (p > Constants.MAX_STRING_LENGTH) {
                throw getStringTooLong(p);
            }
            return readString((int) p);
        }
        // 1 Java character may be encoded with up to 3 bytes
        if (precision > Constants.MAX_STRING_LENGTH * 3) {
            throw getStringTooLong(charLength());
        }
        String s = readString(Integer.MAX_VALUE);
        otherPrecision = p = s.length();
        if (p > Constants.MAX_STRING_LENGTH) {
            throw getStringTooLong(p);
        }
        return s;
    }

    private DbException getStringTooLong(long precision) {
        return DbException.getValueTooLongException("CHARACTER VARYING", readString(81), precision);
    }

    private String readString(int len) {
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
        if (valueType == BLOB) {
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
        byte[] b = readBytes(Integer.MAX_VALUE);
        otherPrecision = p = b.length;
        if (p > Constants.MAX_STRING_LENGTH) {
            throw getBinaryTooLong(p);
        }
        return b;
    }

    private DbException getBinaryTooLong(long precision) {
        return DbException.getValueTooLongException("BINARY VARYING", StringUtils.convertBytesToHex(readBytes(41)),
                precision);
    }

    private byte[] readBytes(int len) {
        try {
            return IOUtils.readBytesAndClose(getInputStream(), len);
        } catch (IOException e) {
            throw DbException.convertIOException(e, toString());
        }
    }

    @Override
    public abstract InputStream getInputStream();

    @Override
    public abstract InputStream getInputStream(long oneBasedOffset, long length);

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
        if (v == this) {
            return 0;
        }
        ValueLob v2 = (ValueLob) v;
        return compare(this, v2);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        if ((sqlFlags & REPLACE_LOBS_FOR_TRACE) != 0
                && (!(this instanceof ValueLobInMemory) || precision > SysProperties.MAX_TRACE_DATA_LENGTH)) {
            if (valueType == Value.CLOB) {
                builder.append("SPACE(").append(precision);
            } else {
                builder.append("CAST(REPEAT(CHAR(0), ").append(precision).append(") AS BINARY VARYING");
            }
            ValueLobDatabase lobDb = (ValueLobDatabase) this;
            builder.append(" /* table: ").append(lobDb.getTableId()).append(" id: ").append(lobDb.getLobId())
                    .append(" */)");
        } else {
            if (valueType == Value.CLOB) {
                if ((sqlFlags & (REPLACE_LOBS_FOR_TRACE | NO_CASTS)) == 0) {
                    StringUtils.quoteStringSQL(builder.append("CAST("), getString())
                            .append(" AS CHARACTER LARGE OBJECT(").append(precision).append("))");
                } else {
                    StringUtils.quoteStringSQL(builder, getString());
                }
            } else {
                if ((sqlFlags & (REPLACE_LOBS_FOR_TRACE | NO_CASTS)) == 0) {
                    builder.append("CAST(X'");
                    StringUtils.convertBytesToHex(builder, getBytesNoCopy()).append("' AS BINARY LARGE OBJECT(")
                            .append(precision).append("))");
                } else {
                    builder.append("X'");
                    StringUtils.convertBytesToHex(builder, getBytesNoCopy()).append('\'');
                }
            }
        }
        return builder;
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
        return 140;
    }

    /**
     * Returns the data handler.
     *
     * @return the data handler, or {@code null}
     */
    public DataHandler getDataHandler() {
        return null;
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
        return this;
    }

    /**
     * Convert the precision to the requested value.
     *
     * @param precision the new precision
     * @return the truncated or this value
     */
    ValueLob convertPrecision(long precision) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long charLength() {
        if (valueType == CLOB) {
            return precision;
        }
        long p = otherPrecision;
        if (p < 0L) {
            try (Reader r = getReader()) {
                p = 0L;
                for (;;) {
                    p += r.skip(Long.MAX_VALUE);
                    if (r.read() < 0) {
                        break;
                    }
                    p++;
                }
            } catch (IOException e) {
                throw DbException.convertIOException(e, null);
            }
            otherPrecision = p;
        }
        return p;
    }

    @Override
    public long octetLength() {
        if (valueType == BLOB) {
            return precision;
        }
        long p = otherPrecision;
        if (p < 0L) {
            try (InputStream is = getInputStream()) {
                p = 0L;
                for (;;) {
                    p += is.skip(Long.MAX_VALUE);
                    if (is.read() < 0) {
                        break;
                    }
                    p++;
                }
            } catch (IOException e) {
                throw DbException.convertIOException(e, null);
            }
            otherPrecision = p;
        }
        return p;
    }

}
