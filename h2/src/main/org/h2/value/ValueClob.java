/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.h2.engine.CastDataProvider;
import org.h2.engine.Constants;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.store.DataHandler;
import org.h2.store.FileStore;
import org.h2.store.FileStoreOutputStream;
import org.h2.store.LobStorageInterface;
import org.h2.store.RangeReader;
import org.h2.util.IOUtils;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;
import org.h2.value.lob.LobData;
import org.h2.value.lob.LobDataDatabase;
import org.h2.value.lob.LobDataFetchOnDemand;
import org.h2.value.lob.LobDataFile;
import org.h2.value.lob.LobDataInMemory;

/**
 * Implementation of the CHARACTER LARGE OBJECT data type.
 */
public final class ValueClob extends ValueLob {

    /**
     * Creates a small CLOB value that can be stored in the row directly.
     *
     * @param data
     *            the data in UTF-8 encoding
     * @return the CLOB
     */
    public static ValueClob createSmall(byte[] data) {
        return new ValueClob(new LobDataInMemory(data), data.length,
                new String(data, StandardCharsets.UTF_8).length());
    }

    /**
     * Creates a small CLOB value that can be stored in the row directly.
     *
     * @param data
     *            the data in UTF-8 encoding
     * @param charLength
     *            the count of characters, must be exactly the same as count of
     *            characters in the data
     * @return the CLOB
     */
    public static ValueClob createSmall(byte[] data, long charLength) {
        return new ValueClob(new LobDataInMemory(data), data.length, charLength);
    }

    /**
     * Creates a small CLOB value that can be stored in the row directly.
     *
     * @param string
     *            the string with value
     * @return the CLOB
     */
    public static ValueClob createSmall(String string) {
        byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
        return new ValueClob(new LobDataInMemory(bytes), bytes.length, string.length());
    }

    /**
     * Create a temporary CLOB value from a stream.
     *
     * @param in
     *            the reader
     * @param length
     *            the number of characters to read, or -1 for no limit
     * @param handler
     *            the data handler
     * @return the lob value
     */
    public static ValueClob createTempClob(Reader in, long length, DataHandler handler) {
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
            long remaining = Long.MAX_VALUE;
            if (length >= 0 && length < remaining) {
                remaining = length;
            }
            int len = ValueLob.getBufferSize(handler, remaining);
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
                return ValueClob.createSmall(new String(buff, 0, len));
            }
            reader.reset();
            return createTemporary(handler, reader, remaining);
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    /**
     * Create a CLOB in a temporary file.
     */
    private static ValueClob createTemporary(DataHandler handler, Reader in, long remaining) throws IOException {
        String fileName = ValueLob.createTempLobFileName(handler);
        FileStore tempFile = handler.openFile(fileName, "rw", false);
        tempFile.autoDelete();

        long octetLength = 0L, charLength = 0L;
        try (FileStoreOutputStream out = new FileStoreOutputStream(tempFile, null)) {
            char[] buff = new char[Constants.IO_BUFFER_SIZE];
            while (true) {
                int len = ValueLob.getBufferSize(handler, remaining);
                len = IOUtils.readFully(in, buff, len);
                if (len == 0) {
                    break;
                }
                // TODO reduce memory allocation
                byte[] data = new String(buff, 0, len).getBytes(StandardCharsets.UTF_8);
                out.write(data);
                octetLength += data.length;
                charLength += len;
            }
        }
        return new ValueClob(new LobDataFile(handler, fileName, tempFile), octetLength, charLength);
    }

    public ValueClob(LobData lobData, long octetLength, long charLength) {
        super(lobData, octetLength, charLength);
    }

    @Override
    public int getValueType() {
        return CLOB;
    }

    @Override
    public String getString() {
        if (charLength > Constants.MAX_STRING_LENGTH) {
            throw getStringTooLong(charLength);
        }
        if (lobData instanceof LobDataInMemory) {
            return new String(((LobDataInMemory) lobData).getSmall(), StandardCharsets.UTF_8);
        }
        return readString((int) charLength);
    }

    @Override
    byte[] getBytesInternal() {
        long p = octetLength;
        if (p >= 0L) {
            if (p > Constants.MAX_STRING_LENGTH) {
                throw getBinaryTooLong(p);
            }
            return readBytes((int) p);
        }
        if (octetLength > Constants.MAX_STRING_LENGTH) {
            throw getBinaryTooLong(octetLength());
        }
        byte[] b = readBytes(Integer.MAX_VALUE);
        octetLength = p = b.length;
        if (p > Constants.MAX_STRING_LENGTH) {
            throw getBinaryTooLong(p);
        }
        return b;
    }

    @Override
    public InputStream getInputStream() {
        return lobData.getInputStream(-1L);
    }

    @Override
    public InputStream getInputStream(long oneBasedOffset, long length) {
        return rangeInputStream(lobData.getInputStream(-1L), oneBasedOffset, length, -1L);
    }

    @Override
    public Reader getReader(long oneBasedOffset, long length) {
        return rangeReader(getReader(), oneBasedOffset, length, charLength);
    }

    @Override
    public int compareTypeSafe(Value v, CompareMode mode, CastDataProvider provider) {
        if (v == this) {
            return 0;
        }
        ValueClob v2 = (ValueClob) v;
        LobData lobData = this.lobData, lobData2 = v2.lobData;
        if (lobData.getClass() == lobData2.getClass()) {
            if (lobData instanceof LobDataInMemory) {
                return Integer.signum(getString().compareTo(v2.getString()));
            } else if (lobData instanceof LobDataDatabase) {
                if (((LobDataDatabase) lobData).getLobId() == ((LobDataDatabase) lobData2).getLobId()) {
                    return 0;
                }
            } else if (lobData instanceof LobDataFetchOnDemand) {
                if (((LobDataFetchOnDemand) lobData).getLobId() == ((LobDataFetchOnDemand) lobData2).getLobId()) {
                    return 0;
                }
            }
        }
        return compare(this, v2);
    }

    /**
     * Compares two CLOB values directly.
     *
     * @param v1
     *            first CLOB value
     * @param v2
     *            second CLOB value
     * @return result of comparison
     */
    private static int compare(ValueClob v1, ValueClob v2) {
        long minPrec = Math.min(v1.charLength, v2.charLength);
        try (Reader reader1 = v1.getReader(); Reader reader2 = v2.getReader()) {
            char[] buf1 = new char[BLOCK_COMPARISON_SIZE];
            char[] buf2 = new char[BLOCK_COMPARISON_SIZE];
            for (; minPrec >= BLOCK_COMPARISON_SIZE; minPrec -= BLOCK_COMPARISON_SIZE) {
                if (IOUtils.readFully(reader1, buf1, BLOCK_COMPARISON_SIZE) != BLOCK_COMPARISON_SIZE
                        || IOUtils.readFully(reader2, buf2, BLOCK_COMPARISON_SIZE) != BLOCK_COMPARISON_SIZE) {
                    throw DbException.getUnsupportedException("Invalid LOB");
                }
                int cmp = Integer.signum(Arrays.compare(buf1, buf2));
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

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        if ((sqlFlags & REPLACE_LOBS_FOR_TRACE) != 0
                && (!(lobData instanceof LobDataInMemory) || charLength > SysProperties.MAX_TRACE_DATA_LENGTH)) {
            builder.append("SPACE(").append(charLength);
            formatLobDataComment(builder);
        } else {
            if ((sqlFlags & (REPLACE_LOBS_FOR_TRACE | NO_CASTS)) == 0) {
                StringUtils.quoteStringSQL(builder.append("CAST("), getString()).append(" AS CHARACTER LARGE OBJECT(")
                        .append(charLength).append("))");
            } else {
                StringUtils.quoteStringSQL(builder, getString());
            }
        }
        return builder;
    }

    /**
     * Convert the precision to the requested value.
     *
     * @param precision
     *            the new precision
     * @return the truncated or this value
     */
    ValueClob convertPrecision(long precision) {
        if (this.charLength <= precision) {
            return this;
        }
        ValueClob lob;
        DataHandler handler = lobData.getDataHandler();
        if (handler != null) {
            lob = createTempClob(getReader(), precision, handler);
        } else {
            try {
                lob = createSmall(IOUtils.readStringAndClose(getReader(), MathUtils.convertLongToInt(precision)));
            } catch (IOException e) {
                throw DbException.convertIOException(e, null);
            }
        }
        return lob;
    }

    @Override
    public ValueLob copy(DataHandler database, int tableId) {
        if (lobData instanceof LobDataInMemory) {
            byte[] small = ((LobDataInMemory) lobData).getSmall();
            if (small.length > database.getMaxLengthInplaceLob()) {
                LobStorageInterface s = database.getLobStorage();
                ValueClob v = s.createClob(getReader(), charLength);
                ValueLob v2 = v.copy(database, tableId);
                v.remove();
                return v2;
            }
            return this;
        } else if (lobData instanceof LobDataDatabase) {
            return database.getLobStorage().copyLob(this, tableId);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public long charLength() {
        return charLength;
    }

    @Override
    public long octetLength() {
        long p = octetLength;
        if (p < 0L) {
            if (lobData instanceof LobDataInMemory) {
                p = ((LobDataInMemory) lobData).getSmall().length;
            } else {
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
            }
            octetLength = p;
        }
        return p;
    }

}
