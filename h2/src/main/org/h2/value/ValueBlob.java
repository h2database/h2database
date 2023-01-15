/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

import org.h2.engine.CastDataProvider;
import org.h2.engine.Constants;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.store.DataHandler;
import org.h2.store.FileStore;
import org.h2.store.FileStoreOutputStream;
import org.h2.store.LobStorageInterface;
import org.h2.util.Bits;
import org.h2.util.IOUtils;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.value.lob.LobData;
import org.h2.value.lob.LobDataDatabase;
import org.h2.value.lob.LobDataFetchOnDemand;
import org.h2.value.lob.LobDataFile;
import org.h2.value.lob.LobDataInMemory;

/**
 * Implementation of the BINARY LARGE OBJECT data type.
 */
public final class ValueBlob extends ValueLob {

    /**
     * Creates a small BLOB value that can be stored in the row directly.
     *
     * @param data
     *            the data
     * @return the BLOB
     */
    public static ValueBlob createSmall(byte[] data) {
        return new ValueBlob(new LobDataInMemory(data), data.length);
    }

    /**
     * Create a temporary BLOB value from a stream.
     *
     * @param in
     *            the input stream
     * @param length
     *            the number of characters to read, or -1 for no limit
     * @param handler
     *            the data handler
     * @return the lob value
     */
    public static ValueBlob createTempBlob(InputStream in, long length, DataHandler handler) {
        try {
            long remaining = Long.MAX_VALUE;
            if (length >= 0 && length < remaining) {
                remaining = length;
            }
            int len = ValueLob.getBufferSize(handler, remaining);
            byte[] buff;
            if (len >= Integer.MAX_VALUE) {
                buff = IOUtils.readBytesAndClose(in, -1);
                len = buff.length;
            } else {
                buff = Utils.newBytes(len);
                len = IOUtils.readFully(in, buff, len);
            }
            if (len <= handler.getMaxLengthInplaceLob()) {
                return ValueBlob.createSmall(Utils.copyBytes(buff, len));
            }
            return createTemporary(handler, buff, len, in, remaining);
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    /**
     * Create a BLOB in a temporary file.
     */
    private static ValueBlob createTemporary(DataHandler handler, byte[] buff, int len, InputStream in, long remaining)
            throws IOException {
        String fileName = ValueLob.createTempLobFileName(handler);
        FileStore tempFile = handler.openFile(fileName, "rw", false);
        tempFile.autoDelete();
        long tmpPrecision = 0;
        try (FileStoreOutputStream out = new FileStoreOutputStream(tempFile, null)) {
            while (true) {
                tmpPrecision += len;
                out.write(buff, 0, len);
                remaining -= len;
                if (remaining <= 0) {
                    break;
                }
                len = ValueLob.getBufferSize(handler, remaining);
                len = IOUtils.readFully(in, buff, len);
                if (len <= 0) {
                    break;
                }
            }
        }
        return new ValueBlob(new LobDataFile(handler, fileName, tempFile), tmpPrecision);
    }

    public ValueBlob(LobData lobData, long octetLength) {
        super(lobData, octetLength, -1L);
    }

    @Override
    public int getValueType() {
        return BLOB;
    }

    @Override
    public String getString() {
        long p = charLength;
        if (p >= 0L) {
            if (p > Constants.MAX_STRING_LENGTH) {
                throw getStringTooLong(p);
            }
            return readString((int) p);
        }
        // 1 Java character may be encoded with up to 3 bytes
        if (octetLength > Constants.MAX_STRING_LENGTH * 3L) {
            throw getStringTooLong(charLength());
        }
        String s;
        if (lobData instanceof LobDataInMemory) {
            s = new String(((LobDataInMemory) lobData).getSmall(), StandardCharsets.UTF_8);
        } else {
            s = readString(Integer.MAX_VALUE);
        }
        charLength = p = s.length();
        if (p > Constants.MAX_STRING_LENGTH) {
            throw getStringTooLong(p);
        }
        return s;
    }

    @Override
    byte[] getBytesInternal() {
        if (octetLength > Constants.MAX_STRING_LENGTH) {
            throw getBinaryTooLong(octetLength);
        }
        return readBytes((int) octetLength);
    }

    @Override
    public InputStream getInputStream() {
        return lobData.getInputStream(octetLength);
    }

    @Override
    public InputStream getInputStream(long oneBasedOffset, long length) {
        long p = octetLength;
        return rangeInputStream(lobData.getInputStream(p), oneBasedOffset, length, p);
    }

    @Override
    public Reader getReader(long oneBasedOffset, long length) {
        return rangeReader(getReader(), oneBasedOffset, length, -1L);
    }

    @Override
    public int compareTypeSafe(Value v, CompareMode mode, CastDataProvider provider) {
        if (v == this) {
            return 0;
        }
        ValueBlob v2 = (ValueBlob) v;
        LobData lobData = this.lobData, lobData2 = v2.lobData;
        if (lobData.getClass() == lobData2.getClass()) {
            if (lobData instanceof LobDataInMemory) {
                return Bits.compareNotNullUnsigned(((LobDataInMemory) lobData).getSmall(),
                        ((LobDataInMemory) lobData2).getSmall());
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
     * Compares two BLOB values directly.
     *
     * @param v1
     *            first BLOB value
     * @param v2
     *            second BLOB value
     * @return result of comparison
     */
    private static int compare(ValueBlob v1, ValueBlob v2) {
        long minPrec = Math.min(v1.octetLength, v2.octetLength);
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
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        if ((sqlFlags & REPLACE_LOBS_FOR_TRACE) != 0
                && (!(lobData instanceof LobDataInMemory) || octetLength > SysProperties.MAX_TRACE_DATA_LENGTH)) {
            builder.append("CAST(REPEAT(CHAR(0), ").append(octetLength).append(") AS BINARY VARYING");
            formatLobDataComment(builder);
        } else {
            if ((sqlFlags & (REPLACE_LOBS_FOR_TRACE | NO_CASTS)) == 0) {
                builder.append("CAST(X'");
                StringUtils.convertBytesToHex(builder, getBytesNoCopy()).append("' AS BINARY LARGE OBJECT(")
                        .append(octetLength).append("))");
            } else {
                builder.append("X'");
                StringUtils.convertBytesToHex(builder, getBytesNoCopy()).append('\'');
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
    ValueBlob convertPrecision(long precision) {
        if (this.octetLength <= precision) {
            return this;
        }
        ValueBlob lob;
        DataHandler handler = lobData.getDataHandler();
        if (handler != null) {
            lob = createTempBlob(getInputStream(), precision, handler);
        } else {
            try {
                lob = createSmall(IOUtils.readBytesAndClose(getInputStream(), MathUtils.convertLongToInt(precision)));
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
                ValueBlob v = s.createBlob(getInputStream(), octetLength);
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
        long p = charLength;
        if (p < 0L) {
            if (lobData instanceof LobDataInMemory) {
                p = new String(((LobDataInMemory) lobData).getSmall(), StandardCharsets.UTF_8).length();
            } else {
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
            }
            charLength = p;
        }
        return p;
    }

    @Override
    public long octetLength() {
        return octetLength;
    }

}
