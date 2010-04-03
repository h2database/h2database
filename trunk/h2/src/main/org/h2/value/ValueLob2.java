/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.message.DbException;
import org.h2.store.DataHandler;
import org.h2.store.FileStore;
import org.h2.store.FileStoreInputStream;
import org.h2.store.FileStoreOutputStream;
import org.h2.store.LobStorage;
import org.h2.util.IOUtils;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * An alternate LOB implementation.
 */
public class ValueLob2 extends Value {

    private final int type;
    private long precision;
    private int tableId;
    private int hash;

    private LobStorage lobStorage;
    private long lobId;

    private byte[] small;

    private DataHandler handler;
    private FileStore tempFile;
    private String fileName;

    private ValueLob2(int type, LobStorage lobStorage, String fileName, int tableId, long lobId, long precision) {
        this.type = type;
        this.lobStorage = lobStorage;
        this.fileName = fileName;
        this.tableId = tableId;
        this.lobId = lobId;
        this.precision = precision;
    }

    private ValueLob2(int type, byte[] small) {
        this.type = type;
        this.small = small;
        if (small != null) {
            if (type == Value.BLOB) {
                this.precision = small.length;
            } else {
                this.precision = getString().length();
            }
        }
    }

    /**
     * Create a LOB value.
     *
     * @param type the type
     * @param lobStorage the storage
     * @param fileName the file name (may be null)
     * @param id the lob id
     * @param precision the precision (number of bytes / characters)
     * @return the value
     */
    public static ValueLob2 create(int type, LobStorage lobStorage, String fileName, long id, long precision) {
        return new ValueLob2(type, lobStorage, fileName, LobStorage.TABLE_ID_SESSION_VARIABLE, id, precision);
    }

    /**
     * Create a small lob using the given byte array.
     *
     * @param type the type (Value.BLOB or CLOB)
     * @param small the byte array
     * @return the lob value
     */
    public static ValueLob2 createSmallLob(int type, byte[] small) {
        return new ValueLob2(type, small);
    }

    /**
     * Convert a lob to another data type. The data is fully read in memory
     * except when converting to BLOB or CLOB.
     *
     * @param t the new type
     * @return the converted value
     */
    public Value convertTo(int t) {
        if (t == type) {
            return this;
        } else if (t == Value.CLOB) {
            Value copy = handler.getLobStorage().createClob(getReader(), -1);
            return copy;
        } else if (t == Value.BLOB) {
            Value copy = handler.getLobStorage().createBlob(getInputStream(), -1);
            return copy;
        }
        return super.convertTo(t);
    }

    public boolean isLinked() {
        return tableId != LobStorage.TABLE_ID_SESSION_VARIABLE;
    }

    public void close() {
        if (fileName != null) {
            if (tempFile != null) {
                tempFile.stopAutoDelete();
            }
            deleteFile(handler, fileName);
        }
    }

    private static synchronized void deleteFile(DataHandler handler, String fileName) {
        // synchronize on the database, to avoid concurrent temp file creation /
        // deletion / backup
        synchronized (handler.getLobSyncObject()) {
            IOUtils.delete(fileName);
        }
    }

    public void unlink() {
    }

    public Value link(DataHandler h, int tabId) {
        int todo;
        return this;
    }

    private int getNewObjectId(DataHandler h) {
        return 0;
    }

    /**
     * Get the current table id of this lob.
     *
     * @return the table id
     */
    public int getTableId() {
        return tableId;
    }

    public int getType() {
        return type;
    }

    public long getPrecision() {
        return precision;
    }

    public String getString() {
        int len = precision > Integer.MAX_VALUE || precision == 0 ? Integer.MAX_VALUE : (int) precision;
        try {
            if (type == Value.CLOB) {
                if (small != null) {
                    return StringUtils.utf8Decode(small);
                }
                return IOUtils.readStringAndClose(getReader(), len);
            }
            byte[] buff;
            if (small != null) {
                buff = small;
            } else {
                buff = IOUtils.readBytesAndClose(getInputStream(), len);
            }
            return Utils.convertBytesToString(buff);
        } catch (IOException e) {
            throw DbException.convertIOException(e, toString());
        }
    }

    public byte[] getBytes() {
        if (type == CLOB) {
            // convert hex to string
            return super.getBytes();
        }
        byte[] data = getBytesNoCopy();
        return Utils.cloneByteArray(data);
    }

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

    protected int compareSecure(Value v, CompareMode mode) {
        if (type == Value.CLOB) {
            return Integer.signum(getString().compareTo(v.getString()));
        }
        byte[] v2 = v.getBytesNoCopy();
        return Utils.compareNotNull(getBytes(), v2);
    }

    public Object getObject() {
        if (type == Value.CLOB) {
            return getReader();
        }
        return getInputStream();
    }

    public Reader getReader() {
        return IOUtils.getReader(getInputStream());
    }

    public InputStream getInputStream() {
        if (small != null) {
            return new ByteArrayInputStream(small);
        } else if (fileName != null) {
            FileStore store = handler.openFile(fileName, "r", true);
            boolean alwaysClose = SysProperties.lobCloseBetweenReads;
            return new BufferedInputStream(new FileStoreInputStream(store, handler, false, alwaysClose),
                    Constants.IO_BUFFER_SIZE);
        }
        try {
            return lobStorage.getInputStream(lobId);
        } catch (IOException e) {
            throw DbException.convertIOException(e, toString());
        }
    }

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

    public String getSQL() {
        String s;
        if (type == Value.CLOB) {
            s = getString();
            return StringUtils.quoteStringSQL(s);
        }
        byte[] buff = getBytes();
        s = Utils.convertBytesToString(buff);
        return "X'" + s + "'";
    }

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
    public byte[] getSmall() {
        return small;
    }

    public int getDisplaySize() {
        return MathUtils.convertLongToInt(getPrecision());
    }

    public boolean equals(Object other) {
        return other instanceof ValueLob && compareSecure((Value) other, null) == 0;
    }

    public boolean isFileBased() {
        return small == null;
    }

    public int getMemory() {
        if (small != null) {
            return small.length + 32;
        }
        return 128;
    }

    /**
     * Create an independent copy of this temporary value.
     * The file will not be deleted automatically.
     *
     * @return the value
     */
    public ValueLob2 copyToTemp() {
        return this;
    }

    public long getLobId() {
        return lobId;
    }

    public void setPrecision(long precision) {
        this.precision = precision;
    }

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
    public static ValueLob2 createTempClob(Reader in, long length, DataHandler handler) {
        try {
            boolean compress = handler.getLobCompressionAlgorithm(Value.CLOB) != null;
            long remaining = Long.MAX_VALUE;
            if (length >= 0 && length < remaining) {
                remaining = length;
            }
            int len = getBufferSize(handler, compress, remaining);
            char[] buff;
            if (len >= Integer.MAX_VALUE) {
                String data = IOUtils.readStringAndClose(in, -1);
                buff = data.toCharArray();
                len = buff.length;
            } else {
                buff = new char[len];
                len = IOUtils.readFully(in, buff, len);
                len = len < 0 ? 0 : len;
            }
            if (len <= handler.getMaxLengthInplaceLob()) {
                byte[] small = StringUtils.utf8Encode(new String(buff, 0, len));
                return ValueLob2.createSmallLob(Value.CLOB, small);
            }
            ValueLob2 lob = new ValueLob2(Value.CLOB, null);
            lob.createTempFromReader(buff, len, in, remaining, handler);
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
    public static ValueLob2 createTempBlob(InputStream in, long length, DataHandler handler) {
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
                len = IOUtils.readFully(in, buff, 0, len);
            }
            if (len <= handler.getMaxLengthInplaceLob()) {
                byte[] small = Utils.newBytes(len);
                System.arraycopy(buff, 0, small, 0, len);
                return ValueLob2.createSmallLob(Value.BLOB, small);
            }
            ValueLob2 lob = new ValueLob2(Value.BLOB, null);
            lob.createTempFromStream(buff, len, in, remaining, handler);
            return lob;
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    private void createTempFromReader(char[] buff, int len, Reader in, long remaining, DataHandler h) {
        try {
            FileStoreOutputStream out = initTemp(h);
            try {
                while (true) {
                    precision += len;
                    byte[] b = StringUtils.utf8Encode(new String(buff, 0, len));
                    out.write(b, 0, b.length);
                    remaining -= len;
                    if (remaining <= 0) {
                        break;
                    }
                    len = getBufferSize(h, false, remaining);
                    len = IOUtils.readFully(in, buff, len);
                    if (len <= 0) {
                        break;
                    }
                }
            } finally {
                out.close();
            }
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    private void createTempFromStream(byte[] buff, int len, InputStream in, long remaining, DataHandler h) {
        try {
            FileStoreOutputStream out = initTemp(h);
            boolean compress = h.getLobCompressionAlgorithm(Value.BLOB) != null;
            try {
                while (true) {
                    precision += len;
                    out.write(buff, 0, len);
                    remaining -= len;
                    if (remaining <= 0) {
                        break;
                    }
                    len = getBufferSize(h, compress, remaining);
                    len = IOUtils.readFully(in, buff, 0, len);
                    if (len <= 0) {
                        break;
                    }
                }
            } finally {
                out.close();
            }
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    private FileStoreOutputStream initTemp(DataHandler h) {
        this.precision = 0;
        this.handler = h;
        this.small = null;
        try {
            String path = h.getDatabasePath();
            if (path.length() == 0) {
                path = SysProperties.PREFIX_TEMP_FILE;
            }
            fileName = IOUtils.createTempFile(path, Constants.SUFFIX_TEMP_FILE, true, true);
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
        tempFile = h.openFile(fileName, "rw", false);
        tempFile.autoDelete();
        FileStoreOutputStream out = new FileStoreOutputStream(tempFile, null, null);
        return out;
    }

    private static int getBufferSize(DataHandler handler, boolean compress, long remaining) {
        if (remaining < 0 || remaining > Integer.MAX_VALUE) {
            remaining = Integer.MAX_VALUE;
        }
        long inplace = handler.getMaxLengthInplaceLob();
        if (inplace >= Integer.MAX_VALUE) {
            inplace = remaining;
        }
        long m = compress ? Constants.IO_BUFFER_SIZE_COMPRESS : Constants.IO_BUFFER_SIZE;
        if (m < remaining && m <= inplace) {
            m = Math.min(remaining, inplace + 1);
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

}
