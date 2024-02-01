/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (https://h2database.com/html/license.html). Initial Developer: H2
 * Group
 */
package org.h2.value;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import org.h2.engine.Constants;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.store.DataHandler;
import org.h2.store.LobStorageFrontend;
import org.h2.store.LobStorageInterface;
import org.h2.store.RangeInputStream;
import org.h2.store.RangeReader;
import org.h2.store.fs.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.value.lob.LobData;
import org.h2.value.lob.LobDataDatabase;
import org.h2.value.lob.LobDataFetchOnDemand;
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

    final LobData lobData;

    /**
     * Length in bytes.
     */
    long octetLength;

    /**
     * Length in characters.
     */
    long charLength;

    /**
     * Cache the hashCode because it can be expensive to compute.
     */
    private int hash;

    ValueLob(LobData lobData, long octetLength, long charLength) {
        this.lobData = lobData;
        this.octetLength = octetLength;
        this.charLength = charLength;
    }

    /**
     * Create file name for temporary LOB storage
     * @param handler to get path from
     * @return full path and name of the created file
     * @throws IOException if file creation fails
     */
    static String createTempLobFileName(DataHandler handler) throws IOException {
        String path = handler.getDatabasePath();
        if (path.isEmpty()) {
            path = SysProperties.PREFIX_TEMP_FILE;
        }
        return FileUtils.createTempFile(path, Constants.SUFFIX_TEMP_FILE, true);
    }

    static int getBufferSize(DataHandler handler, long remaining) {
        if (remaining < 0 || remaining > Integer.MAX_VALUE) {
            remaining = Integer.MAX_VALUE;
        }
        int inplace = handler.getMaxLengthInplaceLob();
        long m = Constants.IO_BUFFER_SIZE;
        if (m < remaining && m <= inplace) {
            // using "1L" to force long arithmetic because
            // inplace could be Integer.MAX_VALUE
            m = Math.min(remaining, inplace + 1L);
            // the buffer size must be bigger than the inplace lob, otherwise we
            // can't know if it must be stored in-place or not
            m = MathUtils.roundUpLong(m, Constants.IO_BUFFER_SIZE);
        }
        m = Math.min(remaining, m);
        m = MathUtils.convertLongToInt(m);
        if (m < 0) {
            m = Integer.MAX_VALUE;
        }
        return (int) m;
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
            int valueType = getValueType();
            this.type = type = new TypeInfo(valueType, valueType == CLOB ? charLength : octetLength, 0, null);
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
            int valueType = getValueType();
            long length = valueType == Value.CLOB ? charLength : octetLength;
            if (length > 4096) {
                // TODO: should calculate the hash code when saving, and store
                // it in the database file
                return (int) (length ^ (length >>> 32));
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
                return s.copyLob(this, LobStorageFrontend.TABLE_RESULT);
            }
        }
        return this;
    }

    final void formatLobDataComment(StringBuilder builder) {
        if (lobData instanceof LobDataDatabase) {
            LobDataDatabase lobDb = (LobDataDatabase) lobData;
            builder.append(" /* table: ").append(lobDb.getTableId()).append(" id: ").append(lobDb.getLobId())
                    .append(" */)");
        } else if (lobData instanceof LobDataFetchOnDemand) {
            LobDataFetchOnDemand lobDemand = (LobDataFetchOnDemand) lobData;
            builder.append(" /* table: ").append(lobDemand.getTableId()).append(" id: ")
                    .append(lobDemand.getLobId()).append(" */)");
        } else {
            builder.append(" /* ").append(lobData.toString().replaceAll("\\*/", "\\\\*\\\\/"))
                    .append(" */");
        }
    }

}
