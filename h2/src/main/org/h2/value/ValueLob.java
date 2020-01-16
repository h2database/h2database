/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

import org.h2.engine.CastDataProvider;
import org.h2.engine.Constants;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.store.DataHandler;
import org.h2.store.FileStore;
import org.h2.store.FileStoreInputStream;
import org.h2.store.FileStoreOutputStream;
import org.h2.store.LobStorageFrontend;
import org.h2.store.LobStorageInterface;
import org.h2.store.RangeInputStream;
import org.h2.store.RangeReader;
import org.h2.store.fs.FileUtils;
import org.h2.util.Bits;
import org.h2.util.IOUtils;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * A implementation of the BLOB and CLOB data types.
 *
 * Small objects are kept in memory and stored in the record.
 * Large objects are either stored in the database, or in temporary files.
 */
public class ValueLob extends Value {

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
    private static InputStream rangeInputStream(InputStream inputStream, long oneBasedOffset, long length, long dataSize) {
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
    private static int compare(Value v1, Value v2) {
        int valueType = v1.getValueType();
        assert valueType == v2.getValueType();
        byte[] small1 = v1.getSmall(), small2 = v2.getSmall();
        if (small1 != null && small2 != null) {
            if (valueType == Value.BLOB) {
                return Bits.compareNotNullSigned(small1, small2);
            } else {
                return Integer.signum(v1.getString().compareTo(v2.getString()));
            }
        }
        long minPrec = Math.min(v1.getType().getPrecision(), v2.getType().getPrecision());
        if (valueType == Value.BLOB) {
            try (InputStream is1 = v1.getInputStream();
                    InputStream is2 = v2.getInputStream()) {
                byte[] buf1 = new byte[BLOCK_COMPARISON_SIZE];
                byte[] buf2 = new byte[BLOCK_COMPARISON_SIZE];
                for (; minPrec >= BLOCK_COMPARISON_SIZE; minPrec -= BLOCK_COMPARISON_SIZE) {
                    if (IOUtils.readFully(is1, buf1, BLOCK_COMPARISON_SIZE) != BLOCK_COMPARISON_SIZE
                            || IOUtils.readFully(is2, buf2, BLOCK_COMPARISON_SIZE) != BLOCK_COMPARISON_SIZE) {
                        throw DbException.getUnsupportedException("Invalid LOB");
                    }
                    int cmp = Bits.compareNotNullSigned(buf1, buf2);
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
                        return Integer.compare(c1, c2);
                    }
                }
            } catch (IOException ex) {
                throw DbException.convert(ex);
            }
        } else {
            try (Reader reader1 = v1.getReader();
                    Reader reader2 = v2.getReader()) {
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
                        return Integer.compare(c1, c2);
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
    private final int valueType;

    private TypeInfo type;
    /**
     * If the LOB is managed by the one the LobStorageBackend classes, these are the
     * unique key inside that storage.
     */
    private final int tableId;
    private final long lobId;
    /**
     * If this is a client-side ValueLobDb object returned by a ResultSet, the
     * hmac acts a security cookie that the client can send back to the server
     * to ask for data related to this LOB.
     */
    private final byte[] hmac;
    /**
     * If the LOB is below the inline size, we just store/load it directly
     * here.
     */
    private final byte[] small;
    private final DataHandler handler;
    /**
     * For a BLOB, precision is length in bytes.
     * For a CLOB, precision is length in chars.
     */
    private final long precision;
    /**
     * If the LOB is a temporary LOB being managed by a temporary ResultSet,
     * it is stored in a temporary file.
     */
    private final String fileName;
    private final FileStore tempFile;
    /**
     * Cache the hashCode because it can be expensive to compute.
     */
    private int hash;

    //Arbonaut: 13.07.2016
    // Fix for recovery tool.

    private boolean isRecoveryReference;

    private ValueLob(int type, DataHandler handler, int tableId, long lobId,
            byte[] hmac, long precision) {
        this.valueType = type;
        this.handler = handler;
        this.tableId = tableId;
        this.lobId = lobId;
        this.hmac = hmac;
        this.precision = precision;
        this.small = null;
        this.fileName = null;
        this.tempFile = null;
    }

    private ValueLob(int type, byte[] small, long precision) {
        this.valueType = type;
        this.small = small;
        this.precision = precision;
        this.lobId = 0;
        this.hmac = null;
        this.handler = null;
        this.fileName = null;
        this.tempFile = null;
        this.tableId = 0;
    }

    /**
     * Create a CLOB in a temporary file.
     */
    private ValueLob(DataHandler handler, Reader in, long remaining)
            throws IOException {
        this.valueType = Value.CLOB;
        this.handler = handler;
        this.small = null;
        this.lobId = 0;
        this.hmac = null;
        this.fileName = createTempLobFileName(handler);
        this.tempFile = handler.openFile(fileName, "rw", false);
        this.tempFile.autoDelete();

        long tmpPrecision = 0;
        try (FileStoreOutputStream out = new FileStoreOutputStream(tempFile, null, null)) {
            char[] buff = new char[Constants.IO_BUFFER_SIZE];
            while (true) {
                int len = getBufferSize(handler, false, remaining);
                len = IOUtils.readFully(in, buff, len);
                if (len == 0) {
                    break;
                }
                byte[] data = new String(buff, 0, len).getBytes(StandardCharsets.UTF_8);
                out.write(data);
                tmpPrecision += len;
            }
        }
        this.precision = tmpPrecision;
        this.tableId = 0;
    }

    /**
     * Create a BLOB in a temporary file.
     */
    private ValueLob(DataHandler handler, byte[] buff, int len, InputStream in,
            long remaining) throws IOException {
        this.valueType = Value.BLOB;
        this.handler = handler;
        this.small = null;
        this.lobId = 0;
        this.hmac = null;
        this.fileName = createTempLobFileName(handler);
        this.tempFile = handler.openFile(fileName, "rw", false);
        this.tempFile.autoDelete();
        long tmpPrecision = 0;
        boolean compress = handler.getLobCompressionAlgorithm(Value.BLOB) != null;
        try (FileStoreOutputStream out = new FileStoreOutputStream(tempFile, null, null)) {
            while (true) {
                tmpPrecision += len;
                out.write(buff, 0, len);
                remaining -= len;
                if (remaining <= 0) {
                    break;
                }
                len = getBufferSize(handler, compress, remaining);
                len = IOUtils.readFully(in, buff, len);
                if (len <= 0) {
                    break;
                }
            }
        }
        this.precision = tmpPrecision;
        this.tableId = 0;
    }

    private static String createTempLobFileName(DataHandler handler)
            throws IOException {
        String path = handler.getDatabasePath();
        if (path.isEmpty()) {
            path = SysProperties.PREFIX_TEMP_FILE;
        }
        return FileUtils.createTempFile(path, Constants.SUFFIX_TEMP_FILE, true);
    }

    /**
     * Create a LOB value.
     *
     * @param type the type (Value.BLOB or CLOB)
     * @param handler the data handler
     * @param tableId the table id
     * @param id the lob id
     * @param hmac the message authentication code
     * @param precision the precision (number of bytes / characters)
     * @return the value
     */
    public static ValueLob create(int type, DataHandler handler,
            int tableId, long id, byte[] hmac, long precision) {
        return new ValueLob(type, handler, tableId, id, hmac, precision);
    }

    @Override
    protected Value convertToBlob() {
        if (handler != null) {
            return handler.getLobStorage().createBlob(getInputStream(), -1);
        } else if (small != null) {
            return ValueLob.createSmallLob(BLOB, small);
        }
        return ValueLob.createSmallLob(BLOB, getBytesNoCopy());
    }

    @Override
    protected Value convertToClob() {
        if (handler != null) {
            return handler.getLobStorage().createClob(getReader(), -1);
        } else if (small != null) {
            byte[] bytes = new String(small, StandardCharsets.UTF_8).getBytes(StandardCharsets.UTF_8);
            if (Arrays.equals(bytes, small)) {
                bytes = small;
            }
            return ValueLob.createSmallLob(CLOB, bytes);
        }
        return ValueLob.createSmallLob(CLOB, getString().getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public boolean isLinkedToTable() {
        return small == null &&
                tableId >= 0;
    }

    public boolean isStored() {
        return small == null && fileName == null;
    }

    @Override
    public void remove() {
        if (fileName != null) {
            if (tempFile != null) {
                tempFile.stopAutoDelete();
            }
            // synchronize on the database, to avoid concurrent temp file
            // creation / deletion / backup
            synchronized (handler.getLobSyncObject()) {
                FileUtils.delete(fileName);
            }
        }
        if (handler != null) {
            handler.getLobStorage().removeLob(this);
        }
    }

    @Override
    public Value copy(DataHandler database, int tableId) {
        if (small == null) {
            return handler.getLobStorage().copyLob(this, tableId, precision);
        } else if (small.length > database.getMaxLengthInplaceLob()) {
            LobStorageInterface s = database.getLobStorage();
            Value v;
            if (valueType == Value.BLOB) {
                v = s.createBlob(getInputStream(), precision);
            } else {
                v = s.createClob(getReader(), precision);
            }
            Value v2 = v.copy(database, tableId);
            v.remove();
            return v2;
        }
        return this;
    }

    /**
     * Get the current table id of this lob.
     *
     * @return the table id
     */
    @Override
    public int getTableId() {
        return tableId;
    }

    @Override
    public TypeInfo getType() {
        TypeInfo type = this.type;
        if (type == null) {
            this.type = type = new TypeInfo(valueType, precision, 0, MathUtils.convertLongToInt(precision), null);
        }
        return type;
    }

    @Override
    public int getValueType() {
        return valueType;
    }

    @Override
    public String getString() {
        int len = precision > Integer.MAX_VALUE || precision == 0 ?
                Integer.MAX_VALUE : (int) precision;
        try {
            if (small != null) {
                return new String(small, StandardCharsets.UTF_8);
            }
            return IOUtils.readStringAndClose(getReader(), len);
        } catch (IOException e) {
            throw DbException.convertIOException(e, toString());
        }
    }

    @Override
    public byte[] getBytes() {
        if (small != null) {
            return Utils.cloneByteArray(small);
        }
        try {
            return IOUtils.readBytesAndClose(getInputStream(), Integer.MAX_VALUE);
        } catch (IOException e) {
            throw DbException.convertIOException(e, toString());
        }
    }

    @Override
    public byte[] getBytesNoCopy() {
        if (small != null) {
            return small;
        }
        try {
            return IOUtils.readBytesAndClose(getInputStream(), Integer.MAX_VALUE);
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
    public int compareTypeSafe(Value v, CompareMode mode, CastDataProvider provider) {
        ValueLob v2 = (ValueLob) v;
        if (v == this) {
            return 0;
        }
        if (lobId == v2.lobId && small == null && v2.small == null) {
            return 0;
        }
        return compare(this, v);
    }

    @Override
    public Object getObject() {
        if (valueType == Value.CLOB) {
            return getReader();
        }
        return getInputStream();
    }

    @Override
    public Reader getReader() {
        return IOUtils.getBufferedReader(getInputStream());
    }

    @Override
    public Reader getReader(long oneBasedOffset, long length) {
        return rangeReader(getReader(), oneBasedOffset, length, valueType == Value.CLOB ? precision : -1);
    }

    @Override
    public InputStream getInputStream() {
        if (small != null) {
            return new ByteArrayInputStream(small);
        } else if (fileName != null) {
            FileStore store = handler.openFile(fileName, "r", true);
            boolean alwaysClose = SysProperties.lobCloseBetweenReads;
            return new BufferedInputStream(new FileStoreInputStream(store,
                    handler, false, alwaysClose), Constants.IO_BUFFER_SIZE);
        }
        long byteCount = (valueType == Value.BLOB) ? precision : -1;
        try {
            return handler.getLobStorage().getInputStream(this, hmac, byteCount);
        } catch (IOException e) {
            throw DbException.convertIOException(e, toString());
        }
    }

    @Override
    public InputStream getInputStream(long oneBasedOffset, long length) {
        long byteCount;
        InputStream inputStream;
        if (small != null) {
            return super.getInputStream(oneBasedOffset, length);
        } else if (fileName != null) {
            FileStore store = handler.openFile(fileName, "r", true);
            boolean alwaysClose = SysProperties.lobCloseBetweenReads;
            byteCount = store.length();
            inputStream = new BufferedInputStream(new FileStoreInputStream(store,
                    handler, false, alwaysClose), Constants.IO_BUFFER_SIZE);
        } else {
            byteCount = (valueType == Value.BLOB) ? precision : -1;
            try {
                inputStream = handler.getLobStorage().getInputStream(this, hmac, byteCount);
            } catch (IOException e) {
                throw DbException.convertIOException(e, toString());
            }
        }
        return rangeInputStream(inputStream, oneBasedOffset, length, byteCount);
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex)
            throws SQLException {
        long p = precision;
        if (p > Integer.MAX_VALUE || p <= 0) {
            p = -1;
        }
        if (valueType == Value.BLOB) {
            prep.setBinaryStream(parameterIndex, getInputStream(), (int) p);
        } else {
            prep.setCharacterStream(parameterIndex, getReader(), (int) p);
        }
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        if ((sqlFlags & REPLACE_LOBS_FOR_TRACE) != 0
                && (small == null || precision > SysProperties.MAX_TRACE_DATA_LENGTH)) {
            if (valueType == Value.CLOB) {
                builder.append("SPACE(").append(precision);
            } else {
                builder.append("CAST(REPEAT('00', ").append(precision).append(") AS BINARY");
            }
            builder.append(" /* table: ").append(tableId).append(" id: ").append(lobId).append(" */)");
        }
        if (valueType == Value.CLOB) {
            StringUtils.quoteStringSQL(builder, getString());
        } else {
            builder.append("X'");
            StringUtils.convertBytesToHex(builder, getBytesNoCopy()).append('\'');
        }
        return builder;
    }

    /**
     * Get the data if this a small lob value.
     *
     * @return the data
     */
    @Override
    public byte[] getSmall() {
        return small;
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
        if (small != null) {
            /*
             * Java 11 with -XX:-UseCompressedOops
             * 0 bytes: 120 bytes
             * 1 byte: 128 bytes
             */
            return small.length + 127;
        }
        return 140;
    }

    /**
     * Create an independent copy of this temporary value.
     * The file will not be deleted automatically.
     *
     * @return the value
     */
    @Override
    public ValueLob copyToTemp() {
        return this;
    }

    /**
     * Create an independent copy of this value,
     * that will be bound to a result.
     *
     * @return the value (this for small objects)
     */
    @Override
    public ValueLob copyToResult() {
        if (handler == null) {
            return this;
        }
        LobStorageInterface s = handler.getLobStorage();
        if (s.isReadOnly()) {
            return this;
        }
        return s.copyLob(this, LobStorageFrontend.TABLE_RESULT, precision);
    }

    public long getLobId() {
        return lobId;
    }

    @Override
    public String toString() {
        return "lob: " + fileName + " table: " + tableId + " id: " + lobId;
    }

    /**
     * Create a temporary CLOB value from a stream.
     *
     * @param in the reader
     * @param length the number of characters to read, or -1 for no limit
     * @param handler the data handler
     * @return the lob value
     */
    public static ValueLob createTempClob(Reader in, long length,
            DataHandler handler) {
        if (length >= 0) {
            // Otherwise BufferedReader may try to read more data than needed and that
            // blocks the network level
            try {
                in = new RangeReader(in, 0, length);
            } catch (IOException e) {
                throw DbException.convert(e);
            }
        }
        BufferedReader reader;
        if (in instanceof BufferedReader) {
            reader = (BufferedReader) in;
        } else {
            reader = new BufferedReader(in, Constants.IO_BUFFER_SIZE);
        }
        try {
            boolean compress = handler.getLobCompressionAlgorithm(Value.CLOB) != null;
            long remaining = Long.MAX_VALUE;
            if (length >= 0 && length < remaining) {
                remaining = length;
            }
            int len = getBufferSize(handler, compress, remaining);
            char[] buff;
            if (len >= Integer.MAX_VALUE) {
                String data = IOUtils.readStringAndClose(reader, -1);
                buff = data.toCharArray();
                len = buff.length;
            } else {
                buff = new char[len];
                reader.mark(len);
                len = IOUtils.readFully(reader, buff, len);
            }
            if (len <= handler.getMaxLengthInplaceLob()) {
                byte[] small = new String(buff, 0, len).getBytes(StandardCharsets.UTF_8);
                return ValueLob.createSmallLob(Value.CLOB, small, len);
            }
            reader.reset();
            return new ValueLob(handler, reader, remaining);
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    /**
     * Create a temporary BLOB value from a stream.
     *
     * @param in the input stream
     * @param length the number of characters to read, or -1 for no limit
     * @param handler the data handler
     * @return the lob value
     */
    public static ValueLob createTempBlob(InputStream in, long length,
            DataHandler handler) {
        try {
            long remaining = Long.MAX_VALUE;
            boolean compress = handler.getLobCompressionAlgorithm(Value.BLOB) != null;
            if (length >= 0 && length < remaining) {
                remaining = length;
            }
            int len = getBufferSize(handler, compress, remaining);
            byte[] buff;
            if (len >= Integer.MAX_VALUE) {
                buff = IOUtils.readBytesAndClose(in, -1);
                len = buff.length;
            } else {
                buff = Utils.newBytes(len);
                len = IOUtils.readFully(in, buff, len);
            }
            if (len <= handler.getMaxLengthInplaceLob()) {
                byte[] small = Utils.copyBytes(buff, len);
                return ValueLob.createSmallLob(Value.BLOB, small, small.length);
            }
            return new ValueLob(handler, buff, len, in, remaining);
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    private static int getBufferSize(DataHandler handler, boolean compress,
            long remaining) {
        if (remaining < 0 || remaining > Integer.MAX_VALUE) {
            remaining = Integer.MAX_VALUE;
        }
        int inplace = handler.getMaxLengthInplaceLob();
        long m = compress ? Constants.IO_BUFFER_SIZE_COMPRESS
                : Constants.IO_BUFFER_SIZE;
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

    @Override
    public Value convertPrecision(long precision) {
        if (this.precision <= precision) {
            return this;
        }
        ValueLob lob;
        if (valueType == CLOB) {
            if (handler == null) {
                try {
                    int p = MathUtils.convertLongToInt(precision);
                    String s = IOUtils.readStringAndClose(getReader(), p);
                    byte[] data = s.getBytes(StandardCharsets.UTF_8);
                    lob = ValueLob.createSmallLob(valueType, data, s.length());
                } catch (IOException e) {
                    throw DbException.convertIOException(e, null);
                }
            } else {
                lob = ValueLob.createTempClob(getReader(), precision, handler);
            }
        } else {
            if (handler == null) {
                try {
                    int p = MathUtils.convertLongToInt(precision);
                    byte[] data = IOUtils.readBytesAndClose(getInputStream(), p);
                    lob = ValueLob.createSmallLob(valueType, data, data.length);
                } catch (IOException e) {
                    throw DbException.convertIOException(e, null);
                }
            } else {
                lob = ValueLob.createTempBlob(getInputStream(), precision, handler);
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
    public static ValueLob createSmallLob(int type, byte[] small) {
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
    public static ValueLob createSmallLob(int type, byte[] small,
            long precision) {
        return new ValueLob(type, small, precision);
    }


    public void setRecoveryReference(boolean isRecoveryReference) {
        this.isRecoveryReference = isRecoveryReference;
    }

    public boolean isRecoveryReference() {
        return isRecoveryReference;
    }
}
