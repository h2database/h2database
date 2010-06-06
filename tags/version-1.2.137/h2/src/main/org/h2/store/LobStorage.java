/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.message.DbException;
import org.h2.tools.CompressTool;
import org.h2.util.IOUtils;
import org.h2.util.New;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.value.Value;
import org.h2.value.ValueLob;
import org.h2.value.ValueLobDb;

/**
 * This class stores LOB objects in the database.
 */
public class LobStorage {

    /**
     * The table id for session variables (LOBs not assigned to a table).
     */
    public static final int TABLE_ID_SESSION_VARIABLE = -1;

    /**
     * The table id for temporary objects (not assigned to any object).
     */
    public static final int TABLE_TEMP = -2;


    private static final String LOBS = "INFORMATION_SCHEMA.LOBS";
    private static final String LOB_MAP = "INFORMATION_SCHEMA.LOB_MAP";
    private static final String LOB_DATA = "INFORMATION_SCHEMA.LOB_DATA";

    private static final int BLOCK_LENGTH = 20000;
    private static final boolean HASH = true;
    private static final long UNIQUE = 0xffff;
    private Connection conn;
    private HashMap<String, PreparedStatement> prepared = New.hashMap();
    private long nextBlock;
    private CompressTool compress = CompressTool.getInstance();

    private final DataHandler handler;
    private boolean init;

    public LobStorage(DataHandler handler) {
        this.handler = handler;
    }

    /**
     * Initialize the lob storage.
     */
    public void init() {
        if (init) {
            return;
        }
        conn = handler.getLobConnection();
        init = true;
        if (conn == null) {
            return;
        }
        try {
            Statement stat = conn.createStatement();
            // stat.execute("SET UNDO_LOG 0");
            // stat.execute("SET REDO_LOG_BINARY 0");
            stat.execute("CREATE TABLE IF NOT EXISTS " + LOBS + "(ID BIGINT PRIMARY KEY, BYTE_COUNT BIGINT, TABLE INT) HIDDEN");
            stat.execute("CREATE TABLE IF NOT EXISTS " + LOB_MAP + "(LOB BIGINT, SEQ INT, BLOCK BIGINT, PRIMARY KEY(LOB, SEQ)) HIDDEN");
            stat.execute("CREATE INDEX IF NOT EXISTS INFORMATION_SCHEMA.INDEX_LOB_MAP_DATA_LOB ON " + LOB_MAP + "(BLOCK, LOB)");
            stat.execute("CREATE TABLE IF NOT EXISTS " + LOB_DATA + "(BLOCK BIGINT PRIMARY KEY, COMPRESSED INT, DATA BINARY) HIDDEN");
            ResultSet rs;
            rs = stat.executeQuery("SELECT MAX(BLOCK) FROM " + LOB_DATA);
            rs.next();
            nextBlock = rs.getLong(1) + 1;
            if (HASH) {
                nextBlock = Math.max(UNIQUE + 1, nextBlock);
            }
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    private long getNextLobId() throws SQLException {
        PreparedStatement prep = prepare("SELECT MAX(ID) FROM " + LOBS);
        ResultSet rs = prep.executeQuery();
        rs.next();
        return rs.getLong(1) + 1;
    }

    /**
     * Remove all LOBs for this table.
     *
     * @param tableId the table id
     */
    public void removeAllForTable(int tableId) {
        if (SysProperties.LOB_IN_DATABASE) {
            init();
            try {
                PreparedStatement prep = prepare("SELECT ID FROM " + LOBS + " WHERE TABLE = ?");
                prep.setInt(1, tableId);
                ResultSet rs = prep.executeQuery();
                while (rs.next()) {
                    deleteLob(rs.getLong(1));
                }
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
            if (tableId == TABLE_ID_SESSION_VARIABLE) {
                removeAllForTable(TABLE_TEMP);
            }
            // remove both lobs in the database as well as in the file system
            // (compatibility)
        }
        ValueLob.removeAllForTable(handler, tableId);
    }

    /**
     * Create a LOB object that fits in memory.
     *
     * @param type the value type
     * @param small the byte array
     * @return the LOB
     */
    public static Value createSmallLob(int type, byte[] small) {
        if (SysProperties.LOB_IN_DATABASE) {
            int precision;
            if (type == Value.CLOB) {
                precision = StringUtils.utf8Decode(small).length();
            } else {
                precision = small.length;
            }
            return ValueLobDb.createSmallLob(type, small, precision);
        }
        return ValueLob.createSmallLob(type, small);
    }

    /**
     * An input stream that reads from a LOB.
     */
    public static class LobInputStream extends InputStream {

        private final Connection conn;
        private PreparedStatement prepSelect;
        private byte[] buffer;
        private int pos;
        private long remainingBytes;
        private long lob;
        private int seq;
        private CompressTool compress;

        public LobInputStream(Connection conn, long lob) throws IOException {
            this.conn = conn;
            try {
                this.lob = lob;
                PreparedStatement prep = conn.prepareStatement(
                        "SELECT BYTE_COUNT FROM " + LOBS + " WHERE ID = ?");
                prep.setLong(1, lob);
                ResultSet rs = prep.executeQuery();
                if (!rs.next()) {
                    throw DbException.get(ErrorCode.IO_EXCEPTION_1, "Missing lob: "+ lob).getSQLException();
                }
                remainingBytes = rs.getLong(1);
                rs.close();
            } catch (SQLException e) {
                throw DbException.convertToIOException(e);
            }
        }

        public int read() throws IOException {
            fillBuffer();
            if (remainingBytes <= 0) {
                return -1;
            }
            remainingBytes--;
            return buffer[pos++] & 255;
        }

        public int read(byte[] buff) throws IOException {
            return readFully(buff, 0, buff.length);
        }

        public int read(byte[] buff, int off, int length) throws IOException {
            return readFully(buff, off, length);
        }

        private int readFully(byte[] buff, int off, int length) throws IOException {
            if (length == 0) {
                return 0;
            }
            int read = 0;
            while (length > 0) {
                fillBuffer();
                if (remainingBytes <= 0) {
                    break;
                }
                int len = (int) Math.min(length, remainingBytes);
                len = Math.min(len, buffer.length - pos);
                System.arraycopy(buffer, pos, buff, off, len);
                pos += len;
                read += len;
                remainingBytes -= len;
                off += len;
                length -= len;
            }
            return read == 0 ? -1 : read;
        }

        private void fillBuffer() throws IOException {
            if (buffer != null && pos < buffer.length) {
                return;
            }
            if (remainingBytes <= 0) {
                return;
            }
            try {
                if (prepSelect == null) {
                    prepSelect = conn.prepareStatement(
                        "SELECT COMPRESSED, DATA FROM " + LOB_MAP + " M " +
                        "INNER JOIN " + LOB_DATA + " D ON M.BLOCK = D.BLOCK " +
                        "WHERE M.LOB = ? AND M.SEQ = ?");
                }
                prepSelect.setLong(1, lob);
                prepSelect.setInt(2, seq);
                ResultSet rs = prepSelect.executeQuery();
                if (!rs.next()) {
                    throw DbException.get(ErrorCode.IO_EXCEPTION_1, "Missing lob entry: "+ lob + "/" + seq).getSQLException();
                }
                seq++;
                int compressed = rs.getInt(1);
                buffer = rs.getBytes(2);
                if (compressed != 0) {
                    if (compress == null) {
                        compress = CompressTool.getInstance();
                    }
                    buffer = compress.expand(buffer);
                }
                pos = 0;
            } catch (SQLException e) {
                throw DbException.convertToIOException(e);
            }
        }

    }

    private synchronized PreparedStatement prepare(String sql) throws SQLException {
        PreparedStatement prep = prepared.get(sql);
        if (prep == null) {
            prep = conn.prepareStatement(sql);
            prepared.put(sql, prep);
        }
        return prep;
    }

    private void deleteLob(long lob) throws SQLException {
        PreparedStatement prep;
        prep = prepare(
            "DELETE FROM " + LOB_MAP + " " +
            "WHERE LOB = ?");
        prep.setLong(1, lob);
        prep.execute();
        prep = prepare(
                "DELETE FROM " + LOB_DATA + " D " +
                "WHERE BLOCK IN(SELECT M.BLOCK FROM " + LOB_MAP + " M WHERE LOB = ?) " +
                "AND NOT EXISTS(SELECT 1 FROM " + LOB_MAP + " M " +
                "WHERE M.BLOCK = D.BLOCK AND M.LOB <> ?)");
        prep.setLong(1, lob);
        prep.setLong(2, lob);
        prep.execute();
        prep = prepare(
                "DELETE FROM " + LOBS + " " +
                "WHERE ID = ?");
        prep.setLong(1, lob);
        prep.execute();
    }

    /**
     * Get the input stream for the given lob.
     *
     * @param lobId the lob id
     * @return the stream
     */
    public InputStream getInputStream(long lobId) throws IOException {
        init();
        return new LobInputStream(conn, lobId);
    }

    private ValueLobDb addLob(InputStream in, long maxLength, int type) {
        byte[] buff = new byte[BLOCK_LENGTH];
        if (maxLength < 0) {
            maxLength = Long.MAX_VALUE;
        }
        long length = 0;
        long lobId;
        int maxLengthInPlaceLob = handler.getMaxLengthInplaceLob();
        String compressAlgorithm = handler.getLobCompressionAlgorithm(type);
        try {
            lobId = getNextLobId();
            try {
                for (int seq = 0; maxLength > 0; seq++) {
                    int len = (int) Math.min(BLOCK_LENGTH, maxLength);
                    len = IOUtils.readFully(in, buff, 0, len);
                    if (len <= 0) {
                        break;
                    }
                    length += len;
                    maxLength -= len;
                    byte[] b;
                    if (len != buff.length) {
                        b = new byte[len];
                        System.arraycopy(buff, 0, b, 0, len);
                    } else {
                        b = buff;
                    }
                    if (seq == 0 && b.length < BLOCK_LENGTH && b.length <= maxLengthInPlaceLob) {
                        // CLOB: the precision will be fixed later
                        ValueLobDb v = ValueLobDb.createSmallLob(type, b, b.length);
                        return v;
                    }
                    storeBlock(lobId, seq, b, compressAlgorithm);
                }
                return registerLob(type, lobId, TABLE_TEMP, length);
            } catch (IOException e) {
                deleteLob(lobId);
                throw DbException.convertIOException(e, "adding blob");
            }
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    private ValueLobDb registerLob(int type, long lobId, int tableId, long byteCount) {
        try {
            PreparedStatement prep = prepare(
                    "INSERT INTO " + LOBS + "(ID, BYTE_COUNT, TABLE) VALUES(?, ?, ?)");
            prep.setLong(1, lobId);
            prep.setLong(2, byteCount);
            prep.setInt(3, tableId);
            prep.execute();
            ValueLobDb v = ValueLobDb.create(type, this, null, tableId, lobId, byteCount);
            return v;
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    /**
     * Copy a lob.
     *
     * @param type the type
     * @param oldLobId the old lob id
     * @param tableId the new table id
     * @param length the length
     * @return the new lob
     */
    public ValueLobDb copyLob(int type, long oldLobId, int tableId, long length) {
        try {
            long lobId = getNextLobId();
            PreparedStatement prep = prepare(
                    "INSERT INTO " + LOB_MAP + "(LOB, SEQ, BLOCK) " +
                    "SELECT ?, SEQ, BLOCK FROM " + LOB_MAP + " WHERE LOB = ?");
            prep.setLong(1, lobId);
            prep.setLong(2, oldLobId);
            prep.executeUpdate();
            prep = prepare(
                    "INSERT INTO " + LOBS + "(ID, BYTE_COUNT, TABLE) " +
                    "SELECT ?, BYTE_COUNT, ? FROM " + LOBS + " WHERE ID = ?");
            prep.setLong(1, lobId);
            prep.setLong(2, tableId);
            prep.setLong(3, oldLobId);
            prep.executeUpdate();
            ValueLobDb v = ValueLobDb.create(type, this, null, tableId, lobId, length);
            return v;
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    /**
     * Store a block in the LOB storage.
     *
     * @param lobId the lob id
     * @param seq the sequence number
     * @param b the data
     * @param compressAlgorithm the compression algorithm (may be null)
     */
    synchronized void storeBlock(long lobId, int seq, byte[] b, String compressAlgorithm) throws SQLException {
        long block;
        boolean blockExists = false;
        if (compressAlgorithm != null) {
            b = compress.compress(b, compressAlgorithm);
        }
        if (HASH) {
            block = Arrays.hashCode(b) & UNIQUE;
            PreparedStatement prep = prepare(
                    "SELECT COMPRESSED, DATA FROM " + LOB_DATA +
                    " WHERE BLOCK = ?");
            prep.setLong(1, block);
            ResultSet rs = prep.executeQuery();
            if (rs.next()) {
                boolean compressed = rs.getInt(1) != 0;
                byte[] compare = rs.getBytes(2);
                if (Arrays.equals(b, compare) && compressed == (compressAlgorithm != null)) {
                    blockExists = true;
                } else {
                    block = nextBlock++;
                }
            }
        } else {
            block = nextBlock++;
        }
        if (!blockExists) {
            PreparedStatement prep = prepare(
                    "INSERT INTO " + LOB_DATA + "(BLOCK, COMPRESSED, DATA) VALUES(?, ?, ?)");
            prep.setLong(1, block);
            prep.setInt(2, compressAlgorithm == null ? 0 : 1);
            prep.setBytes(3, b);
            prep.execute();
        }
        PreparedStatement prep = prepare(
                "INSERT INTO " + LOB_MAP + "(LOB, SEQ, BLOCK) VALUES(?, ?, ?)");
        prep.setLong(1, lobId);
        prep.setInt(2, seq);
        prep.setLong(3, block);
        prep.execute();
    }

    /**
     * An input stream that reads the data from a reader.
     */
    static class CountingReaderInputStream extends InputStream {

        private final Reader reader;
        private long length;
        private long remaining;
        private int pos;
        private char[] charBuffer = new char[Constants.IO_BUFFER_SIZE];
        private byte[] buffer;

        CountingReaderInputStream(Reader reader, long maxLength) {
            this.reader = reader;
            this.remaining = maxLength;
            buffer = Utils.EMPTY_BYTES;
        }

        public int read(byte[] buff, int offset, int len) throws IOException {
            if (buffer == null) {
                return -1;
            }
            if (pos >= buffer.length) {
                fillBuffer();
                if (buffer == null) {
                    return -1;
                }
            }
            len = Math.min(len, buffer.length - pos);
            System.arraycopy(buffer, pos, buff, offset, len);
            pos += len;
            return len;
        }

        public int read() throws IOException {
            if (buffer == null) {
                return -1;
            }
            if (pos >= buffer.length) {
                fillBuffer();
                if (buffer == null) {
                    return -1;
                }
            }
            return buffer[pos++];
        }

        private void fillBuffer() throws IOException {
            int len = (int) Math.min(charBuffer.length, remaining);
            if (len > 0) {
                len = reader.read(charBuffer, 0, len);
            } else {
                len = -1;
            }
            if (len < 0) {
                buffer = null;
            } else {
                buffer = StringUtils.utf8Encode(new String(charBuffer, 0, len));
                length += len;
                remaining -= len;
            }
            pos = 0;
        }

        public long getLength() {
            return length;
        }

        public void close() throws IOException {
            reader.close();
        }

    }

    /**
     * Create a BLOB object.
     *
     * @param in the input stream
     * @param maxLength the maximum length (-1 if not known)
     * @return the LOB
     */
    public Value createBlob(InputStream in, long maxLength) {
        if (SysProperties.LOB_IN_DATABASE) {
            init();
            if (conn == null) {
                return ValueLobDb.createTempBlob(in, maxLength, handler);
            }
            return addLob(in, maxLength, Value.BLOB);
        }
        return ValueLob.createBlob(in, maxLength, handler);
    }

    /**
     * Create a CLOB object.
     *
     * @param reader the reader
     * @param maxLength the maximum length (-1 if not known)
     * @return the LOB
     */
    public Value createClob(Reader reader, long maxLength) {
        if (SysProperties.LOB_IN_DATABASE) {
            init();
            if (conn == null) {
                return ValueLobDb.createTempClob(reader, maxLength, handler);
            }
            long max = maxLength == -1 ? Long.MAX_VALUE : maxLength;
            CountingReaderInputStream in = new CountingReaderInputStream(reader, max);
            ValueLobDb lob = addLob(in, Long.MAX_VALUE, Value.CLOB);
            lob.setPrecision(in.getLength());
            return lob;
        }
        return ValueLob.createClob(reader, maxLength, handler);
    }

    /**
     * Set the table reference of this lob.
     *
     * @param lobId the lob
     * @param table the table
     */
    public void setTable(long lobId, int table) {
        try {
            PreparedStatement prep = prepare("UPDATE " + LOBS + " SET TABLE = ? WHERE ID = ?");
            prep.setInt(1, table);
            prep.setLong(2, lobId);
            int updateCount = prep.executeUpdate();
            if (updateCount != 1) {
                // can be zero when recovering
                // throw DbException.throwInternalError("count: " + updateCount);
            }
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

}
