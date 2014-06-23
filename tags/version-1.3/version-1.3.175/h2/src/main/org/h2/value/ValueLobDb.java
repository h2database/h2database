/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.message.DbException;
import org.h2.mvstore.DataUtils;
import org.h2.store.DataHandler;
import org.h2.store.FileStore;
import org.h2.store.FileStoreInputStream;
import org.h2.store.FileStoreOutputStream;
import org.h2.store.LobStorageFrontend;
import org.h2.store.LobStorageInterface;
import org.h2.store.fs.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * An alternate LOB implementation, where LOB data is stored inside the
 * database, instead of in external files.
 */
public class ValueLobDb extends Value implements Value.ValueClob, Value.ValueBlob {

    private final int type;
    private final long lobId;
    private final byte[] hmac;
    private final byte[] small;
    private final DataHandler handler;

    /**
     * For a BLOB, precision is length in bytes.
     * For a CLOB, precision is length in chars.
     */
    private final long precision;

    private final String fileName;
    private final FileStore tempFile;
    private int tableId;
    private int hash;

    private ValueLobDb(int type, DataHandler handler, int tableId, long lobId, byte[] hmac, long precision) {
        this.type = type;
        this.handler = handler;
        this.tableId = tableId;
        this.lobId = lobId;
        this.hmac = hmac;
        this.precision = precision;
        this.small = null;
        this.fileName = null;
        this.tempFile = null;
    }

    private ValueLobDb(int type, byte[] small, long precision) {
        this.type = type;
        this.small = small;
        this.precision = precision;
        this.lobId = 0;
        this.hmac = null;
        this.handler = null;
        this.fileName = null;
        this.tempFile = null;
    }

    private ValueLobDb(int type, DataHandler handler) {
        this.type = type;
        this.handler = handler;
        this.small = null;
        this.precision = 0;
        this.lobId = 0;
        this.hmac = null;
        this.fileName = null;
        this.tempFile = null;
    }

    /**
     * Create temporary CLOB from Reader.
     */
    private ValueLobDb(DataHandler handler, Reader in, long remaining) throws IOException {
        this.type = Value.CLOB;
        this.handler = handler;
        this.small = null;
        this.lobId = 0;
        this.hmac = null;
        this.fileName = createTempLobFileName(handler);
        this.tempFile = this.handler.openFile(fileName, "rw", false);
        this.tempFile.autoDelete();
        FileStoreOutputStream out = new FileStoreOutputStream(tempFile, null, null);
        long tmpPrecision = 0;
        try {
            char[] buff = new char[Constants.IO_BUFFER_SIZE];
            while (true) {
                int len = getBufferSize(this.handler, false, remaining);
                len = IOUtils.readFully(in, buff, len);
                if (len <= 0) {
                    break;
                }
            }
        } finally {
            out.close();
        }
        this.precision = tmpPrecision;
    }

    /**
     * Create temporary BLOB from InputStream.
     */
    private ValueLobDb(DataHandler handler, byte[] buff, int len, InputStream in,
            long remaining) throws IOException {
        this.type = Value.BLOB;
        this.handler = handler;
        this.small = null;
        this.lobId = 0;
        this.hmac = null;
        this.fileName = createTempLobFileName(handler);
        this.tempFile = this.handler.openFile(fileName, "rw", false);
        this.tempFile.autoDelete();
        FileStoreOutputStream out = new FileStoreOutputStream(tempFile, null, null);
        long tmpPrecision = 0;
        boolean compress = this.handler.getLobCompressionAlgorithm(Value.BLOB) != null;
        try {
            while (true) {
                tmpPrecision += len;
                out.write(buff, 0, len);
                remaining -= len;
                if (remaining <= 0) {
                    break;
                }
                len = getBufferSize(this.handler, compress, remaining);
                len = IOUtils.readFully(in, buff, 0, len);
                if (len <= 0) {
                    break;
                }
            }
        } finally {
            out.close();
        }
        this.precision = tmpPrecision;
    }

    private static String createTempLobFileName(DataHandler handler) throws IOException {
        String path = handler.getDatabasePath();
        if (path.length() == 0) {
            path = SysProperties.PREFIX_TEMP_FILE;
        }
        return FileUtils.createTempFile(path, Constants.SUFFIX_TEMP_FILE, true, true);
    }

    /**
     * Create a LOB value.
     *
     * @param type the type
     * @param handler the data handler
     * @param tableId the table id
     * @param id the lob id
     * @param hmac the message authentication code
     * @param precision the precision (number of bytes / characters)
     * @return the value
     */
    public static ValueLobDb create(int type, DataHandler handler,
            int tableId, long id, byte[] hmac, long precision) {
        return new ValueLobDb(type, handler, tableId, id, hmac, precision);
    }

    /**
     * Create a small lob using the given byte array.
     *
     * @param type the type (Value.BLOB or CLOB)
     * @param small the byte array
     * @param precision the precision
     * @return the lob value
     */
    public static ValueLobDb createSmallLob(int type, byte[] small, long precision) {
        return new ValueLobDb(type, small, precision);
    }

    /**
     * Convert a lob to another data type. The data is fully read in memory
     * except when converting to BLOB or CLOB.
     *
     * @param t the new type
     * @return the converted value
     */
    @Override
    public Value convertTo(int t) {
        if (t == type) {
            return this;
        } else if (t == Value.CLOB) {
            if (handler != null) {
                Value copy = handler.getLobStorage().createClob(getReader(), -1);
                return copy;
            } else if (small != null) {
                return LobStorageFrontend.createSmallLob(t, small);
            }
        } else if (t == Value.BLOB) {
            if (handler != null) {
                Value copy = handler.getLobStorage().createBlob(getInputStream(), -1);
                return copy;
            } else if (small != null) {
                return LobStorageFrontend.createSmallLob(t, small);
            }
        }
        return super.convertTo(t);
    }

    @Override
    public boolean isLinked() {
        return tableId != LobStorageFrontend.TABLE_ID_SESSION_VARIABLE && small == null;
    }

    public boolean isStored() {
        return small == null && fileName == null;
    }

    @Override
    public void close() {
        if (fileName != null) {
            if (tempFile != null) {
                tempFile.stopAutoDelete();
            }
            // synchronize on the database, to avoid concurrent temp file creation /
            // deletion / backup
            synchronized (handler.getLobSyncObject()) {
                FileUtils.delete(fileName);
            }
        }
        if (handler != null) {
            handler.getLobStorage().removeLob(lobId);
        }
    }

    @Override
    public void unlink(DataHandler database) {
        if (small == null && tableId != LobStorageFrontend.TABLE_ID_SESSION_VARIABLE) {
            database.getLobStorage().setTable(lobId, LobStorageFrontend.TABLE_ID_SESSION_VARIABLE);
            tableId = LobStorageFrontend.TABLE_ID_SESSION_VARIABLE;
        }
    }

    @Override
    public Value link(DataHandler database, int tabId) {
        if (small == null) {
            if (tableId == LobStorageFrontend.TABLE_TEMP) {
                database.getLobStorage().setTable(lobId, tabId);
                this.tableId = tabId;
            } else {
                return handler.getLobStorage().copyLob(type, lobId, tabId, getPrecision());
            }
        } else if (small.length > database.getMaxLengthInplaceLob()) {
            LobStorageInterface s = database.getLobStorage();
            Value v;
            if (type == Value.BLOB) {
                v = s.createBlob(getInputStream(), getPrecision());
            } else {
                v = s.createClob(getReader(), getPrecision());
            }
            return v.link(database, tabId);
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
    public int getType() {
        return type;
    }

    @Override
    public long getPrecision() {
        return precision;
    }

    @Override
    public String getString() {
        int len = precision > Integer.MAX_VALUE || precision == 0 ? Integer.MAX_VALUE : (int) precision;
        try {
            if (type == Value.CLOB) {
                if (small != null) {
                    return new String(small, Constants.UTF8);
                }
                return IOUtils.readStringAndClose(getReader(), len);
            }
            byte[] buff;
            if (small != null) {
                buff = small;
            } else {
                buff = IOUtils.readBytesAndClose(getInputStream(), len);
            }
            return StringUtils.convertBytesToHex(buff);
        } catch (IOException e) {
            throw DbException.convertIOException(e, toString());
        }
    }

    @Override
    public byte[] getBytes() {
        if (type == CLOB) {
            // convert hex to string
            return super.getBytes();
        }
        byte[] data = getBytesNoCopy();
        return Utils.cloneByteArray(data);
    }

    @Override
    public byte[] getBytesNoCopy() {
        if (type == CLOB) {
            // convert hex to string
            return super.getBytesNoCopy();
        }
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
            if (type == CLOB) {
                hash = getString().hashCode();
            } else {
                hash = Utils.getByteArrayHash(getBytes());
            }
        }
        return hash;
    }

    @Override
    protected int compareSecure(Value v, CompareMode mode) {
        if (v instanceof ValueLobDb) {
            ValueLobDb v2 = (ValueLobDb) v;
            if (v == this) {
                return 0;
            }
            if (lobId == v2.lobId && small == null && v2.small == null) {
                return 0;
            }
        }
        if (type == Value.CLOB) {
            return Integer.signum(getString().compareTo(v.getString()));
        }
        byte[] v2 = v.getBytesNoCopy();
        return Utils.compareNotNullSigned(getBytes(), v2);
    }

    @Override
    public Object getObject() {
        if (type == Value.CLOB) {
            return getReader();
        }
        return getInputStream();
    }

    @Override
    public Reader getReader() {
        return IOUtils.getBufferedReader(getInputStream());
    }

    @Override
    public InputStream getInputStream() {
        if (small != null) {
            return new ByteArrayInputStream(small);
        } else if (fileName != null) {
            FileStore store = handler.openFile(fileName, "r", true);
            boolean alwaysClose = SysProperties.lobCloseBetweenReads;
            return new BufferedInputStream(new FileStoreInputStream(store, handler, false, alwaysClose),
                    Constants.IO_BUFFER_SIZE);
        }
        long byteCount = (type == Value.BLOB) ? precision : -1;
        try {
            return handler.getLobStorage().getInputStream(lobId, hmac, byteCount);
        } catch (IOException e) {
            throw DbException.convertIOException(e, toString());
        }
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        long p = getPrecision();
        if (p > Integer.MAX_VALUE || p <= 0) {
            p = -1;
        }
        if (type == Value.BLOB) {
            prep.setBinaryStream(parameterIndex, getInputStream(), (int) p);
        } else {
            prep.setCharacterStream(parameterIndex, getReader(), (int) p);
        }
    }

    @Override
    public String getSQL() {
        String s;
        if (type == Value.CLOB) {
            s = getString();
            return StringUtils.quoteStringSQL(s);
        }
        byte[] buff = getBytes();
        s = StringUtils.convertBytesToHex(buff);
        return "X'" + s + "'";
    }

    @Override
    public String getTraceSQL() {
        if (small != null && getPrecision() <= SysProperties.MAX_TRACE_DATA_LENGTH) {
            return getSQL();
        }
        StringBuilder buff = new StringBuilder();
        if (type == Value.CLOB) {
            buff.append("SPACE(").append(getPrecision());
        } else {
            buff.append("CAST(REPEAT('00', ").append(getPrecision()).append(") AS BINARY");
        }
        buff.append(" /* table: ").append(tableId).append(" id: ").append(lobId).append(" */)");
        return buff.toString();
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
    public int getDisplaySize() {
        return MathUtils.convertLongToInt(getPrecision());
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueLobDb && compareSecure((Value) other, null) == 0;
    }

    @Override
    public int getMemory() {
        if (small != null) {
            return small.length + 104;
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
    public ValueLobDb copyToTemp() {
        return this;
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
    public static ValueLobDb createTempClob(Reader in, long length, DataHandler handler) {
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
                len = len < 0 ? 0 : len;
            }
            if (len <= handler.getMaxLengthInplaceLob()) {
                byte[] small = new String(buff, 0, len).getBytes(Constants.UTF8);
                return ValueLobDb.createSmallLob(Value.CLOB, small, len);
            }
            reader.reset();
            ValueLobDb lob = new ValueLobDb(handler, reader, remaining);
            return lob;
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
    public static ValueLobDb createTempBlob(InputStream in, long length, DataHandler handler) {
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
                buff = DataUtils.newBytes(len);
                len = IOUtils.readFully(in, buff, 0, len);
            }
            if (len <= handler.getMaxLengthInplaceLob()) {
                byte[] small = DataUtils.newBytes(len);
                System.arraycopy(buff, 0, small, 0, len);
                return ValueLobDb.createSmallLob(Value.BLOB, small, small.length);
            }
            ValueLobDb lob = new ValueLobDb(handler, buff, len, in, remaining);
            return lob;
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    private static int getBufferSize(DataHandler handler, boolean compress, long remaining) {
        if (remaining < 0 || remaining > Integer.MAX_VALUE) {
            remaining = Integer.MAX_VALUE;
        }
        int inplace = handler.getMaxLengthInplaceLob();
        long m = compress ? Constants.IO_BUFFER_SIZE_COMPRESS : Constants.IO_BUFFER_SIZE;
        if (m < remaining && m <= inplace) {
            // using "1L" to force long arithmetic
            m = Math.min(remaining, inplace + 1L);
            // the buffer size must be bigger than the inplace lob, otherwise we can't
            // know if it must be stored in-place or not
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
    public Value convertPrecision(long precision, boolean force) {
        if (this.precision <= precision) {
            return this;
        }
        ValueLob lob;
        if (type == CLOB) {
            lob = ValueLob.createClob(getReader(), precision, handler);
        } else {
            lob = ValueLob.createBlob(getInputStream(), precision, handler);
        }
        return lob;
    }

}
