/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
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
import org.h2.store.fs.FileSystem;
import org.h2.util.IOUtils;
import org.h2.util.MathUtils;
import org.h2.util.SmallLRUCache;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * Implementation of the BLOB and CLOB data types. Small objects are kept in
 * memory and stored in the record.
 *
 * Large objects are stored in their own files. When large objects are set in a
 * prepared statement, they are first stored as 'temporary' files. Later, when
 * they are used in a record, and when the record is stored, the lob files are
 * linked: the file is renamed using the file format (tableId).(objectId). There
 * is one exception: large variables are stored in the file (-1).(objectId).
 *
 * When lobs are deleted, they are first renamed to a temp file, and if the
 * delete operation is committed the file is deleted.
 *
 * Data compression is supported.
 */
public class ValueLob extends Value {

    /**
     * This counter is used to calculate the next directory to store lobs. It is
     * better than using a random number because less directories are created.
     */
    private static int dirCounter;

    private final int type;
    private long precision;
    private DataHandler handler;
    private int tableId;
    private int objectId;
    private String fileName;
    private boolean linked;
    private byte[] small;
    private int hash;
    private boolean compression;
    private FileStore tempFile;

    private ValueLob(int type, DataHandler handler, String fileName, int tableId, int objectId, boolean linked,
            long precision, boolean compression) {
        this.type = type;
        this.handler = handler;
        this.fileName = fileName;
        this.tableId = tableId;
        this.objectId = objectId;
        this.linked = linked;
        this.precision = precision;
        this.compression = compression;
    }

    private ValueLob(int type, byte[] small) {
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

    private static ValueLob copy(ValueLob lob) {
        ValueLob copy = new ValueLob(lob.type, lob.handler, lob.fileName, lob.tableId, lob.objectId, lob.linked, lob.precision, lob.compression);
        copy.small = lob.small;
        copy.hash = lob.hash;
        return copy;
    }

    /**
     * Create a small lob using the given byte array.
     *
     * @param type the type (Value.BLOB or CLOB)
     * @param small the byte array
     * @return the lob value
     */
    public static ValueLob createSmallLob(int type, byte[] small) {
        return new ValueLob(type, small);
    }

    private static String getFileName(DataHandler handler, int tableId, int objectId) {
        if (SysProperties.CHECK && tableId == 0 && objectId == 0) {
            DbException.throwInternalError("0 LOB");
        }
        String table = tableId < 0 ? ".temp" : ".t" + tableId;
        return getFileNamePrefix(handler.getDatabasePath(), objectId) + table + Constants.SUFFIX_LOB_FILE;
    }

    /**
     * Create a LOB value with the given parameters.
     *
     * @param type the data type
     * @param handler the file handler
     * @param tableId the table object id
     * @param objectId the object id
     * @param precision the precision (length in elements)
     * @param compression if compression is used
     * @return the value object
     */
    public static ValueLob open(int type, DataHandler handler, int tableId, int objectId, long precision, boolean compression) {
        String fileName = getFileName(handler, tableId, objectId);
        return new ValueLob(type, handler, fileName, tableId, objectId, true, precision, compression);
    }

    /**
     * Create a CLOB value from a stream.
     *
     * @param in the reader
     * @param length the number of characters to read, or -1 for no limit
     * @param handler the data handler
     * @return the lob value
     */
    public static ValueLob createClob(Reader in, long length, DataHandler handler) {
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
                return ValueLob.createSmallLob(Value.CLOB, small);
            }
            ValueLob lob = new ValueLob(Value.CLOB, null);
            lob.createFromReader(buff, len, in, remaining, handler);
            return lob;
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
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

    private void createFromReader(char[] buff, int len, Reader in, long remaining, DataHandler h) {
        try {
            FileStoreOutputStream out = initLarge(h);
            boolean compress = h.getLobCompressionAlgorithm(Value.CLOB) != null;
            try {
                while (true) {
                    precision += len;
                    byte[] b = StringUtils.utf8Encode(new String(buff, 0, len));
                    out.write(b, 0, b.length);
                    remaining -= len;
                    if (remaining <= 0) {
                        break;
                    }
                    len = getBufferSize(h, compress, remaining);
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

    private static String getFileNamePrefix(String path, int objectId) {
        String name;
        int f = objectId % SysProperties.LOB_FILES_PER_DIRECTORY;
        if (f > 0) {
            name = File.separator + objectId;
        } else {
            name = "";
        }
        objectId /= SysProperties.LOB_FILES_PER_DIRECTORY;
        while (objectId > 0) {
            f = objectId % SysProperties.LOB_FILES_PER_DIRECTORY;
            name = File.separator + f + Constants.SUFFIX_LOBS_DIRECTORY + name;
            objectId /= SysProperties.LOB_FILES_PER_DIRECTORY;
        }
        name = IOUtils.normalize(path + Constants.SUFFIX_LOBS_DIRECTORY + name);
        return name;
    }

    private int getNewObjectId(DataHandler h) {
        String path = h.getDatabasePath();
        int newId = 0;
        int lobsPerDir = SysProperties.LOB_FILES_PER_DIRECTORY;
        while (true) {
            String dir = getFileNamePrefix(path, newId);
            String[] list = getFileList(h, dir);
            int fileCount = 0;
            boolean[] used = new boolean[lobsPerDir];
            for (String name : list) {
                if (name.endsWith(Constants.SUFFIX_DB_FILE)) {
                    name = IOUtils.getFileName(name);
                    String n = name.substring(0, name.indexOf('.'));
                    int id;
                    try {
                        id = Integer.parseInt(n);
                    } catch (NumberFormatException e) {
                        id = -1;
                    }
                    if (id > 0) {
                        fileCount++;
                        used[id % lobsPerDir] = true;
                    }
                }
            }
            int fileId = -1;
            if (fileCount < lobsPerDir) {
                for (int i = 1; i < lobsPerDir; i++) {
                    if (!used[i]) {
                        fileId = i;
                        break;
                    }
                }
            }
            if (fileId > 0) {
                newId += fileId;
                invalidateFileList(h, dir);
                break;
            }
            if (newId > Integer.MAX_VALUE / lobsPerDir) {
                // this directory path is full: start from zero
                newId = 0;
                dirCounter = MathUtils.randomInt(lobsPerDir - 1) * lobsPerDir;
            } else {
                // calculate the directory
                // start with 1 (otherwise we don't know the number of directories)
                // it doesn't really matter what directory is used, it might as well be random
                // (but that would generate more directories):
                // int dirId = RandomUtils.nextInt(lobsPerDir - 1) + 1;
                int dirId = (dirCounter++ / (lobsPerDir - 1)) + 1;
                newId = newId * lobsPerDir;
                newId += dirId * lobsPerDir;
            }
        }
        return newId;
    }

    /**
     * Reset the directory counter as if the process was stopped. This method is
     * for debugging only (to simulate stopping a process).
     */
    public static void resetDirCounter() {
        dirCounter = 0;
    }

    private void invalidateFileList(DataHandler h, String dir) {
        SmallLRUCache<String, String[]> cache = h.getLobFileListCache();
        if (cache != null) {
            synchronized (cache) {
                cache.remove(dir);
            }
        }
    }

    private String[] getFileList(DataHandler h, String dir) {
        SmallLRUCache<String, String[]> cache = h.getLobFileListCache();
        String[] list;
        if (cache == null) {
            list = IOUtils.listFiles(dir);
        } else {
            synchronized (cache) {
                list = cache.get(dir);
                if (list == null) {
                    list = IOUtils.listFiles(dir);
                    cache.put(dir, list);
                }
            }
        }
        return list;
    }

    /**
     * Create a BLOB value from a stream.
     *
     * @param in the input stream
     * @param length the number of characters to read, or -1 for no limit
     * @param handler the data handler
     * @return the lob value
     */
    public static ValueLob createBlob(InputStream in, long length, DataHandler handler) {
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
                return ValueLob.createSmallLob(Value.BLOB, small);
            }
            ValueLob lob = new ValueLob(Value.BLOB, null);
            lob.createFromStream(buff, len, in, remaining, handler);
            return lob;
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    private FileStoreOutputStream initLarge(DataHandler h) {
        this.handler = h;
        this.tableId = 0;
        this.linked = false;
        this.precision = 0;
        this.small = null;
        this.hash = 0;
        String compressionAlgorithm = h.getLobCompressionAlgorithm(type);
        this.compression = compressionAlgorithm != null;
        synchronized (h) {
            objectId = getNewObjectId(h);
            fileName = getFileNamePrefix(h.getDatabasePath(), objectId) + Constants.SUFFIX_TEMP_FILE;
            tempFile = h.openFile(fileName, "rw", false);
            tempFile.autoDelete();
        }
        FileStoreOutputStream out = new FileStoreOutputStream(tempFile, h, compressionAlgorithm);
        return out;
    }

    private void createFromStream(byte[] buff, int len, InputStream in, long remaining, DataHandler h) {
        try {
            FileStoreOutputStream out = initLarge(h);
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
            ValueLob copy = ValueLob.createClob(getReader(), -1, handler);
            return copy;
        } else if (t == Value.BLOB) {
            ValueLob copy = ValueLob.createBlob(getInputStream(), -1, handler);
            return copy;
        }
        return super.convertTo(t);
    }

    public boolean isLinked() {
        return linked;
    }

    /**
     * Get the current file name where the lob is saved.
     *
     * @return the file name or null
     */
    public String getFileName() {
        return fileName;
    }

    public void close() {
        if (fileName != null) {
            if (tempFile != null) {
                tempFile.stopAutoDelete();
            }
            deleteFile(handler, fileName);
        }
    }

    public void unlink() {
        if (linked && fileName != null) {
            String temp;
            // synchronize on the database, to avoid concurrent temp file
            // creation / deletion / backup
            synchronized (handler) {
                temp = getFileName(handler, -1, objectId);
                deleteFile(handler, temp);
                renameFile(handler, fileName, temp);
                tempFile = FileStore.open(handler, temp, "rw");
                tempFile.autoDelete();
                tempFile.closeSilently();
                fileName = temp;
                linked = false;
            }
        }
    }

    public Value link(DataHandler h, int tabId) {
        if (fileName == null) {
            this.tableId = tabId;
            return this;
        }
        if (linked) {
            ValueLob copy = ValueLob.copy(this);
            copy.objectId = getNewObjectId(h);
            copy.tableId = tabId;
            String live = getFileName(h, copy.tableId, copy.objectId);
            copyFileTo(h, fileName, live);
            copy.fileName = live;
            copy.linked = true;
            return copy;
        }
        if (!linked) {
            this.tableId = tabId;
            String live = getFileName(h, tableId, objectId);
            if (tempFile != null) {
                tempFile.stopAutoDelete();
                tempFile = null;
            }
            renameFile(h, fileName, live);
            fileName = live;
            linked = true;
        }
        return this;
    }

    /**
     * Get the current table id of this lob.
     *
     * @return the table id
     */
    public int getTableId() {
        return tableId;
    }

    /**
     * Get the current object id of this lob.
     *
     * @return the object id
     */
    public int getObjectId() {
        return objectId;
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
            throw DbException.convertIOException(e, fileName);
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
            throw DbException.convertIOException(e, fileName);
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
        if (fileName == null) {
            return new ByteArrayInputStream(small);
        }
        FileStore store = handler.openFile(fileName, "r", true);
        boolean alwaysClose = SysProperties.lobCloseBetweenReads;
        return new BufferedInputStream(new FileStoreInputStream(store, handler, compression, alwaysClose),
                Constants.IO_BUFFER_SIZE);
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
        buff.append(" /* ").append(fileName).append(" */)");
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

    /**
     * Store the lob data to a file if the size of the buffer it larger than the
     * maximum size for an in-place lob.
     *
     * @param h the data handler
     */
    public void convertToFileIfRequired(DataHandler h) {
        if (small != null && small.length > h.getMaxLengthInplaceLob()) {
            boolean compress = h.getLobCompressionAlgorithm(type) != null;
            int len = getBufferSize(h, compress, Long.MAX_VALUE);
            int tabId = tableId;
            if (type == Value.BLOB) {
                createFromStream(Utils.newBytes(len), 0, getInputStream(), Long.MAX_VALUE, h);
            } else {
                createFromReader(new char[len], 0, getReader(), Long.MAX_VALUE, h);
            }
            Value v2 = link(h, tabId);
            if (SysProperties.CHECK && v2 != this) {
                DbException.throwInternalError();
            }
        }
    }

    /**
     * Remove all lobs for a given table id.
     *
     * @param handler the data handler
     * @param tableId the table id
     */
    public static void removeAllForTable(DataHandler handler, int tableId) {
        String dir = getFileNamePrefix(handler.getDatabasePath(), 0);
        removeAllForTable(handler, dir, tableId);
    }

    private static void removeAllForTable(DataHandler handler, String dir, int tableId) {
        for (String name : IOUtils.listFiles(dir)) {
            if (IOUtils.isDirectory(name)) {
                removeAllForTable(handler, name, tableId);
            } else {
                if (name.endsWith(".t" + tableId + Constants.SUFFIX_LOB_FILE)) {
                    deleteFile(handler, name);
                }
            }
        }
    }

    /**
     * Check if this lob value is compressed.
     *
     * @return true if it is
     */
    public boolean useCompression() {
        return compression;
    }

    public boolean isFileBased() {
        return fileName != null;
    }

    private static synchronized void deleteFile(DataHandler handler, String fileName) {
        // synchronize on the database, to avoid concurrent temp file creation /
        // deletion / backup
        synchronized (handler.getLobSyncObject()) {
            IOUtils.delete(fileName);
        }
    }

    private static synchronized void renameFile(DataHandler handler, String oldName, String newName)
            {
        synchronized (handler.getLobSyncObject()) {
            IOUtils.rename(oldName, newName);
        }
    }

    private void copyFileTo(DataHandler h, String sourceFileName, String targetFileName) {
        synchronized (h.getLobSyncObject()) {
            FileSystem.getInstance(sourceFileName).copy(sourceFileName, targetFileName);
        }
    }

    /**
     * Set the file name of this lob value.
     *
     * @param fileName the file name
     * @param linked if the lob is linked
     */
    public void setFileName(String fileName, boolean linked) {
        this.fileName = fileName;
        this.linked = linked;
    }

    public int getMemory() {
        if (small != null) {
            return small.length + 64;
        }
        return 128;
    }

    /**
     * Create an independent copy of this temporary value.
     * The file will not be deleted automatically.
     *
     * @return the value
     */
    public ValueLob copyToTemp() {
        ValueLob lob;
        if (type == CLOB) {
            lob = ValueLob.createClob(getReader(), precision, handler);
        } else {
            lob = ValueLob.createBlob(getInputStream(), precision, handler);
        }
        return lob;
    }

}
