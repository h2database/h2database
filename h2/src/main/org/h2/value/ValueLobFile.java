/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (https://h2database.com/html/license.html). Initial Developer: H2
 * Group
 */
package org.h2.value;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import org.h2.engine.Constants;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.store.DataHandler;
import org.h2.store.FileStore;
import org.h2.store.FileStoreInputStream;
import org.h2.store.FileStoreOutputStream;
import org.h2.store.RangeReader;
import org.h2.store.fs.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.MathUtils;
import org.h2.util.Utils;

/**
 * A implementation of the BLOB and CLOB data types. Small objects are kept in
 * memory and stored in the record. Large objects are either stored in the
 * database, or in temporary files.
 */
public final class ValueLobFile extends ValueLob {

    private DataHandler handler;
    /**
     * If the LOB is a temporary LOB being managed by a temporary ResultSet, it
     * is stored in a temporary file.
     */
    private final String fileName;
    private final FileStore tempFile;

    /**
     * Create a CLOB in a temporary file.
     */
    ValueLobFile(DataHandler handler, Reader in, long remaining) throws IOException {
        super(Value.CLOB, 0);
        this.handler = handler;
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
    }

    /**
     * Create a BLOB in a temporary file.
     */
    ValueLobFile(DataHandler handler, byte[] buff, int len, InputStream in, long remaining) throws IOException {
        super(Value.BLOB, 0);
        this.handler = handler;
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
    }

    private static String createTempLobFileName(DataHandler handler) throws IOException {
        String path = handler.getDatabasePath();
        if (path.isEmpty()) {
            path = SysProperties.PREFIX_TEMP_FILE;
        }
        return FileUtils.createTempFile(path, Constants.SUFFIX_TEMP_FILE, true);
    }

    /**
     * Remove the underlying resource, if any. For values that are kept fully in
     * memory this method has no effect.
     */
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
    }

    @Override
    public InputStream getInputStream() {
        FileStore store = handler.openFile(fileName, "r", true);
        boolean alwaysClose = SysProperties.lobCloseBetweenReads;
        return new BufferedInputStream(new FileStoreInputStream(store, handler, false, alwaysClose),
                Constants.IO_BUFFER_SIZE);
    }

    @Override
    public InputStream getInputStream(long oneBasedOffset, long length) {
        final FileStore store = handler.openFile(fileName, "r", true);
        final boolean alwaysClose = SysProperties.lobCloseBetweenReads;
        final long byteCount = store.length();
        final InputStream inputStream = new BufferedInputStream(
                new FileStoreInputStream(store, handler, false, alwaysClose), Constants.IO_BUFFER_SIZE);
        return rangeInputStream(inputStream, oneBasedOffset, length, byteCount);
    }

    @Override
    public DataHandler getDataHandler() {
        return handler;
    }

    @Override
    public String toString() {
        return "lob-file: " + fileName;
    }

    /**
     * Create a temporary CLOB value from a stream.
     *
     * @param in the reader
     * @param length the number of characters to read, or -1 for no limit
     * @param handler the data handler
     * @return the lob value
     */
    public static ValueLob createTempClob(Reader in, long length, DataHandler handler) {
        if (length >= 0) {
            // Otherwise BufferedReader may try to read more data than needed
            // and that
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
                return ValueLobInMemory.createSmallLob(Value.CLOB, small, len);
            }
            reader.reset();
            return new ValueLobFile(handler, reader, remaining);
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
    public static ValueLob createTempBlob(InputStream in, long length, DataHandler handler) {
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
                return ValueLobInMemory.createSmallLob(Value.BLOB, small, small.length);
            }
            return new ValueLobFile(handler, buff, len, in, remaining);
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

}
