/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.message.DbException;
import org.h2.tools.CompressTool;
import org.h2.util.IOUtils;
import org.h2.util.MathUtils;
import org.h2.util.New;
import org.h2.util.Utils;
import org.h2.value.Value;
import org.h2.value.ValueLob;
import org.h2.value.ValueLobDb;

/**
 * This class stores LOB objects in the database.
 * This is the back-end i.e. the server side of the LOB storage.
 */
public class LobStorageBackend implements LobStorageInterface {

    /**
     * The name of the lob data table. If this table exists, then lob storage is
     * used.
     */
    public static final String LOB_DATA_TABLE = "LOB_DATA";

    private static final String LOB_SCHEMA = "INFORMATION_SCHEMA";
    private static final String LOBS = LOB_SCHEMA + ".LOBS";
    private static final String LOB_MAP = LOB_SCHEMA + ".LOB_MAP";
    private static final String LOB_DATA = LOB_SCHEMA + "." + LOB_DATA_TABLE;

    /**
     * The size of the chunks we use when storing LOBs inside the database file.
     */
    private static final int BLOCK_LENGTH = 20000;

    /**
     * The size of cache for lob block hashes. Each entry needs 2 longs (16
     * bytes), therefore, the size 4096 means 64 KB.
     */
    private static final int HASH_CACHE_SIZE = 4 * 1024;

    private Connection conn;
    private final HashMap<String, PreparedStatement> prepared = New.hashMap();
    private long nextBlock;
    private final CompressTool compress = CompressTool.getInstance();
    private long[] hashBlocks;

    private final Database database;
    private boolean init;

    public LobStorageBackend(Database database) {
        this.database = database;
    }

    /**
     * Initialize the lob storage.
     */
    public void init() {
        if (init) {
            return;
        }
        synchronized (database) {
            // have to check this again or we might miss an update on another thread
            if (init) {
                return;
            }
            conn = database.getLobConnection();
            init = true;
            try {
                Statement stat = conn.createStatement();
                // stat.execute("SET UNDO_LOG 0");
                // stat.execute("SET REDO_LOG_BINARY 0");
                boolean create = true;
                PreparedStatement prep = conn.prepareStatement(
                        "SELECT ZERO() FROM INFORMATION_SCHEMA.COLUMNS WHERE " +
                        "TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?");
                prep.setString(1, "INFORMATION_SCHEMA");
                prep.setString(2, "LOB_MAP");
                prep.setString(3, "POS");
                ResultSet rs;
                rs = prep.executeQuery();
                if (rs.next()) {
                    prep = conn.prepareStatement(
                            "SELECT ZERO() FROM INFORMATION_SCHEMA.TABLES WHERE " +
                            "TABLE_SCHEMA=? AND TABLE_NAME=?");
                    prep.setString(1, "INFORMATION_SCHEMA");
                    prep.setString(2, "LOB_DATA");
                    rs = prep.executeQuery();
                    if (rs.next()) {
                        create = false;
                    }
                }
                if (create) {
                    stat.execute("CREATE CACHED TABLE IF NOT EXISTS " + LOBS +
                            "(ID BIGINT PRIMARY KEY, BYTE_COUNT BIGINT, TABLE INT) HIDDEN");
                    stat.execute("CREATE INDEX IF NOT EXISTS " +
                            "INFORMATION_SCHEMA.INDEX_LOB_TABLE ON " + LOBS + "(TABLE)");
                    stat.execute("CREATE CACHED TABLE IF NOT EXISTS " + LOB_MAP +
                            "(LOB BIGINT, SEQ INT, POS BIGINT, HASH INT, BLOCK BIGINT, PRIMARY KEY(LOB, SEQ)) HIDDEN");
                    stat.execute("ALTER TABLE " + LOB_MAP + " RENAME TO " + LOB_MAP + " HIDDEN");
                    stat.execute("ALTER TABLE " + LOB_MAP + " ADD IF NOT EXISTS POS BIGINT BEFORE HASH");
                    // TODO the column name OFFSET was used in version 1.3.156,
                    // so this can be remove in a later version
                    stat.execute("ALTER TABLE " + LOB_MAP + " DROP COLUMN IF EXISTS \"OFFSET\"");
                    stat.execute("CREATE INDEX IF NOT EXISTS " +
                            "INFORMATION_SCHEMA.INDEX_LOB_MAP_DATA_LOB ON " + LOB_MAP + "(BLOCK, LOB)");
                    stat.execute("CREATE CACHED TABLE IF NOT EXISTS " + LOB_DATA +
                            "(BLOCK BIGINT PRIMARY KEY, COMPRESSED INT, DATA BINARY) HIDDEN");
                }
                rs = stat.executeQuery("SELECT MAX(BLOCK) FROM " + LOB_DATA);
                rs.next();
                nextBlock = rs.getLong(1) + 1;
                stat.close();
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
        }
    }

    private long getNextLobId() throws SQLException {
        String sql = "SELECT MAX(LOB) FROM " + LOB_MAP;
        PreparedStatement prep = prepare(sql);
        ResultSet rs = prep.executeQuery();
        rs.next();
        long x = rs.getLong(1) + 1;
        reuse(sql, prep);
        sql = "SELECT MAX(ID) FROM " + LOBS;
        prep = prepare(sql);
        rs = prep.executeQuery();
        rs.next();
        x = Math.max(x, rs.getLong(1) + 1);
        reuse(sql, prep);
        return x;
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
                String sql = "SELECT ID FROM " + LOBS + " WHERE TABLE = ?";
                PreparedStatement prep = prepare(sql);
                prep.setInt(1, tableId);
                ResultSet rs = prep.executeQuery();
                while (rs.next()) {
                    removeLob(rs.getLong(1));
                }
                reuse(sql, prep);
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
            if (tableId == LobStorageFrontend.TABLE_ID_SESSION_VARIABLE) {
                removeAllForTable(LobStorageFrontend.TABLE_TEMP);
            }
            // remove both lobs in the database as well as in the file system
            // (compatibility)
        }
        ValueLob.removeAllForTable(database, tableId);
    }

    /**
     * Read a block of data from the given LOB.
     *
     * @param lob the lob id
     * @param seq the block sequence number
     * @return the block (expanded if stored compressed)
     */
    byte[] readBlock(long lob, int seq) throws SQLException {
        synchronized (database) {
            String sql = "SELECT COMPRESSED, DATA FROM " + LOB_MAP + " M " +
                    "INNER JOIN " + LOB_DATA + " D ON M.BLOCK = D.BLOCK " +
                    "WHERE M.LOB = ? AND M.SEQ = ?";
            PreparedStatement prep = prepare(sql);
            prep.setLong(1, lob);
            prep.setInt(2, seq);
            ResultSet rs = prep.executeQuery();
            if (!rs.next()) {
                throw DbException.get(ErrorCode.IO_EXCEPTION_1, "Missing lob entry: "+ lob + "/" + seq).getSQLException();
            }
            int compressed = rs.getInt(1);
            byte[] buffer = rs.getBytes(2);
            if (compressed != 0) {
                buffer = compress.expand(buffer);
            }
            reuse(sql, prep);
            return buffer;
        }
    }

    /**
     * Retrieve the sequence id and position that is smaller than the requested
     * position. Those values can be used to quickly skip to a given position
     * without having to read all data.
     *
     * @param lob the lob
     * @param pos the required position
     * @return null if the data is not available, or an array of two elements:
     *         the sequence, and the offset
     */
    long[] skipBuffer(long lob, long pos) throws SQLException {
        synchronized (database) {
            String sql = "SELECT MAX(SEQ), MAX(POS) FROM " + LOB_MAP +
                    " WHERE LOB = ? AND POS < ?";
            PreparedStatement prep = prepare(sql);
            prep.setLong(1, lob);
            prep.setLong(2, pos);
            ResultSet rs = prep.executeQuery();
            rs.next();
            int seq = rs.getInt(1);
            pos = rs.getLong(2);
            boolean wasNull = rs.wasNull();
            rs.close();
            reuse(sql, prep);
            // upgraded: offset not set
            return wasNull ? null : new long[]{seq, pos};
        }
    }

    private PreparedStatement prepare(String sql) throws SQLException {
        if (SysProperties.CHECK2) {
            if (!Thread.holdsLock(database)) {
                throw DbException.throwInternalError();
            }
        }
        PreparedStatement prep = prepared.remove(sql);
        if (prep == null) {
            prep = conn.prepareStatement(sql);
        }
        return prep;
    }

    private void reuse(String sql, PreparedStatement prep) {
        if (SysProperties.CHECK2) {
            if (!Thread.holdsLock(database)) {
                throw DbException.throwInternalError();
            }
        }
        prepared.put(sql, prep);
    }

    @Override
    public void removeLob(long lob) {
        try {
            synchronized (database) {
                String sql = "SELECT BLOCK, HASH FROM " + LOB_MAP + " D WHERE D.LOB = ? " +
                        "AND NOT EXISTS(SELECT 1 FROM " + LOB_MAP + " O " +
                        "WHERE O.BLOCK = D.BLOCK AND O.LOB <> ?)";
                PreparedStatement prep = prepare(sql);
                prep.setLong(1, lob);
                prep.setLong(2, lob);
                ResultSet rs = prep.executeQuery();
                ArrayList<Long> blocks = New.arrayList();
                while (rs.next()) {
                    blocks.add(rs.getLong(1));
                    int hash = rs.getInt(2);
                    setHashCacheBlock(hash, -1);
                }
                reuse(sql, prep);

                sql = "DELETE FROM " + LOB_MAP + " WHERE LOB = ?";
                prep = prepare(sql);
                prep.setLong(1, lob);
                prep.execute();
                reuse(sql, prep);

                sql = "DELETE FROM " + LOB_DATA + " WHERE BLOCK = ?";
                prep = prepare(sql);
                for (long block : blocks) {
                    prep.setLong(1, block);
                    prep.execute();
                }
                reuse(sql, prep);

                sql = "DELETE FROM " + LOBS + " WHERE ID = ?";
                prep = prepare(sql);
                prep.setLong(1, lob);
                prep.execute();
                reuse(sql, prep);
            }
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public InputStream getInputStream(long lobId, byte[] hmac, long byteCount) throws IOException {
        init();
        if (byteCount == -1) {
            synchronized (database) {
                try {
                    String sql = "SELECT BYTE_COUNT FROM " + LOBS + " WHERE ID = ?";
                    PreparedStatement prep = prepare(sql);
                    prep.setLong(1, lobId);
                    ResultSet rs = prep.executeQuery();
                    if (!rs.next()) {
                        throw DbException.get(ErrorCode.IO_EXCEPTION_1, "Missing lob: "+ lobId).getSQLException();
                    }
                    byteCount = rs.getLong(1);
                    reuse(sql, prep);
                } catch (SQLException e) {
                    throw DbException.convertToIOException(e);
                }
            }
        }
        return new LobInputStream(lobId, byteCount);
    }

    private ValueLobDb addLob(InputStream in, long maxLength, int type) {
        try {
            byte[] buff = new byte[BLOCK_LENGTH];
            if (maxLength < 0) {
                maxLength = Long.MAX_VALUE;
            }
            long length = 0;
            long lobId = -1;
            int maxLengthInPlaceLob = database.getMaxLengthInplaceLob();
            String compressAlgorithm = database.getLobCompressionAlgorithm(type);
            try {
                byte[] small = null;
                for (int seq = 0; maxLength > 0; seq++) {
                    int len = (int) Math.min(BLOCK_LENGTH, maxLength);
                    len = IOUtils.readFully(in, buff, 0, len);
                    if (len <= 0) {
                        break;
                    }
                    maxLength -= len;
                    byte[] b;
                    if (len != buff.length) {
                        b = new byte[len];
                        System.arraycopy(buff, 0, b, 0, len);
                    } else {
                        b = buff;
                    }
                    if (seq == 0 && b.length < BLOCK_LENGTH && b.length <= maxLengthInPlaceLob) {
                        small = b;
                        break;
                    }
                    synchronized (database) {
                        if (seq == 0) {
                            lobId = getNextLobId();
                        }
                        storeBlock(lobId, seq, length, b, compressAlgorithm);
                    }
                    length += len;
                }
                if (lobId == -1 && small == null) {
                    // zero length
                    small = new byte[0];
                }
                if (small != null) {
                    // CLOB: the precision will be fixed later
                    ValueLobDb v = ValueLobDb.createSmallLob(type, small, small.length);
                    return v;
                }
                return registerLob(type, lobId, LobStorageFrontend.TABLE_TEMP, length);
            } catch (IOException e) {
                if (lobId != -1) {
                    removeLob(lobId);
                }
                throw DbException.convertIOException(e, null);
            }
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    private ValueLobDb registerLob(int type, long lobId, int tableId, long byteCount) {
        synchronized (database) {
            try {
                String sql = "INSERT INTO " + LOBS + "(ID, BYTE_COUNT, TABLE) VALUES(?, ?, ?)";
                PreparedStatement prep = prepare(sql);
                prep.setLong(1, lobId);
                prep.setLong(2, byteCount);
                prep.setInt(3, tableId);
                prep.execute();
                reuse(sql, prep);
                ValueLobDb v = ValueLobDb.create(type, this, tableId, lobId, null, byteCount);
                return v;
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
        }
    }

    @Override
    public ValueLobDb copyLob(int type, long oldLobId, int tableId, long length) {
        synchronized (database) {
            try {
                init();
                long lobId = getNextLobId();
                String sql = "INSERT INTO " + LOB_MAP + "(LOB, SEQ, POS, HASH, BLOCK) " +
                        "SELECT ?, SEQ, POS, HASH, BLOCK FROM " + LOB_MAP + " WHERE LOB = ?";
                PreparedStatement prep = prepare(sql);
                prep.setLong(1, lobId);
                prep.setLong(2, oldLobId);
                prep.executeUpdate();
                reuse(sql, prep);

                sql = "INSERT INTO " + LOBS + "(ID, BYTE_COUNT, TABLE) " +
                        "SELECT ?, BYTE_COUNT, ? FROM " + LOBS + " WHERE ID = ?";
                prep = prepare(sql);
                prep.setLong(1, lobId);
                prep.setLong(2, tableId);
                prep.setLong(3, oldLobId);
                prep.executeUpdate();
                reuse(sql, prep);

                ValueLobDb v = ValueLobDb.create(type, this, tableId, lobId, null, length);
                return v;
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
        }
    }

    private long getHashCacheBlock(int hash) {
        if (HASH_CACHE_SIZE > 0) {
            initHashCache();
            int index = hash & (HASH_CACHE_SIZE - 1);
            long oldHash = hashBlocks[index];
            if (oldHash == hash) {
                return hashBlocks[index + HASH_CACHE_SIZE];
            }
        }
        return -1;
    }

    private void setHashCacheBlock(int hash, long block) {
        if (HASH_CACHE_SIZE > 0) {
            initHashCache();
            int index = hash & (HASH_CACHE_SIZE - 1);
            hashBlocks[index] = hash;
            hashBlocks[index + HASH_CACHE_SIZE] = block;
        }
    }

    private void initHashCache() {
        if (hashBlocks == null) {
            hashBlocks = new long[HASH_CACHE_SIZE * 2];
        }
    }

    /**
     * Store a block in the LOB storage.
     *
     * @param lobId the lob id
     * @param seq the sequence number
     * @param pos the position within the lob
     * @param b the data
     * @param compressAlgorithm the compression algorithm (may be null)
     */
    void storeBlock(long lobId, int seq, long pos, byte[] b, String compressAlgorithm) throws SQLException {
        long block;
        boolean blockExists = false;
        if (compressAlgorithm != null) {
            b = compress.compress(b, compressAlgorithm);
        }
        int hash = Arrays.hashCode(b);
        synchronized (database) {
            block = getHashCacheBlock(hash);
            if (block != -1) {
                String sql =  "SELECT COMPRESSED, DATA FROM " + LOB_DATA +
                        " WHERE BLOCK = ?";
                PreparedStatement prep = prepare(sql);
                prep.setLong(1, block);
                ResultSet rs = prep.executeQuery();
                if (rs.next()) {
                    boolean compressed = rs.getInt(1) != 0;
                    byte[] compare = rs.getBytes(2);
                    if (compressed == (compressAlgorithm != null) && Arrays.equals(b, compare)) {
                        blockExists = true;
                    }
                }
                reuse(sql, prep);
            }
            if (!blockExists) {
                block = nextBlock++;
                setHashCacheBlock(hash, block);
                String sql = "INSERT INTO " + LOB_DATA + "(BLOCK, COMPRESSED, DATA) VALUES(?, ?, ?)";
                PreparedStatement prep = prepare(sql);
                prep.setLong(1, block);
                prep.setInt(2, compressAlgorithm == null ? 0 : 1);
                prep.setBytes(3, b);
                prep.execute();
                reuse(sql, prep);
            }
            String sql = "INSERT INTO " + LOB_MAP + "(LOB, SEQ, POS, HASH, BLOCK) VALUES(?, ?, ?, ?, ?)";
            PreparedStatement prep = prepare(sql);
            prep.setLong(1, lobId);
            prep.setInt(2, seq);
            prep.setLong(3, pos);
            prep.setLong(4, hash);
            prep.setLong(5, block);
            prep.execute();
            reuse(sql, prep);
        }
    }

    @Override
    public Value createBlob(InputStream in, long maxLength) {
        if (SysProperties.LOB_IN_DATABASE) {
            init();
            return addLob(in, maxLength, Value.BLOB);
        }
        return ValueLob.createBlob(in, maxLength, database);
    }

    @Override
    public Value createClob(Reader reader, long maxLength) {
        if (SysProperties.LOB_IN_DATABASE) {
            init();
            long max = maxLength == -1 ? Long.MAX_VALUE : maxLength;
            CountingReaderInputStream in = new CountingReaderInputStream(reader, max);
            ValueLobDb lob = addLob(in, Long.MAX_VALUE, Value.CLOB);
            lob.setPrecision(in.getLength());
            return lob;
        }
        return ValueLob.createClob(reader, maxLength, database);
    }

    @Override
    public void setTable(long lobId, int table) {
        synchronized (database) {
            try {
                init();
                String sql = "UPDATE " + LOBS + " SET TABLE = ? WHERE ID = ?";
                PreparedStatement prep = prepare(sql);
                prep.setInt(1, table);
                prep.setLong(2, lobId);
                prep.executeUpdate();
                reuse(sql, prep);
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
        }
    }

    /**
     * An input stream that reads from a LOB.
     */
    public class LobInputStream extends InputStream {

        /**
         * The size of the lob.
         */
        private final long length;

        /**
         * The remaining bytes in the lob.
         */
        private long remainingBytes;

        /**
         * The temporary buffer.
         */
        private byte[] buffer;

        /**
         * The position within the buffer.
         */
        private int pos;

        /**
         * The lob id.
         */
        private final long lob;

        /**
         * The lob sequence id.
         */
        private int seq;

        public LobInputStream(long lob, long byteCount) {
            this.lob = lob;
            remainingBytes = byteCount;
            this.length = byteCount;
        }

        @Override
        public int read() throws IOException {
            fillBuffer();
            if (remainingBytes <= 0) {
                return -1;
            }
            remainingBytes--;
            return buffer[pos++] & 255;
        }

        @Override
        public long skip(long n) throws IOException {
            long remaining = n;
            remaining -= skipSmall(remaining);
            if (remaining > BLOCK_LENGTH) {
                long toPos = length - remainingBytes + remaining;
                try {
                    long[] seqPos = skipBuffer(lob, toPos);
                    if (seqPos == null) {
                        remaining -= super.skip(remaining);
                        return n - remaining;
                    }
                    seq = (int) seqPos[0];
                    long p = seqPos[1];
                    remainingBytes = length - p;
                    remaining = toPos - p;
                } catch (SQLException e) {
                    throw DbException.convertToIOException(e);
                }
                pos = 0;
                buffer = null;
            }
            fillBuffer();
            remaining -= skipSmall(remaining);
            remaining -= super.skip(remaining);
            return n - remaining;
        }

        private int skipSmall(long n) {
            if (n > 0 && buffer != null && pos < buffer.length) {
                int x = MathUtils.convertLongToInt(Math.min(n, buffer.length - pos));
                pos += x;
                remainingBytes -= x;
                return x;
            }
            return 0;
        }

        @Override
        public int read(byte[] buff) throws IOException {
            return readFully(buff, 0, buff.length);
        }

        @Override
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
                buffer = readBlock(lob, seq++);
                pos = 0;
            } catch (SQLException e) {
                throw DbException.convertToIOException(e);
            }
        }

    }

    /**
     * An input stream that reads the data from a reader.
     */
    static class CountingReaderInputStream extends InputStream {

        private final Reader reader;
        private long length;
        private long remaining;
        private int pos;
        private final char[] charBuffer = new char[Constants.IO_BUFFER_SIZE];
        private byte[] buffer;

        CountingReaderInputStream(Reader reader, long maxLength) {
            this.reader = reader;
            this.remaining = maxLength;
            buffer = Utils.EMPTY_BYTES;
        }

        @Override
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

        @Override
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
                buffer = new String(charBuffer, 0, len).getBytes(Constants.UTF8);
                length += len;
                remaining -= len;
            }
            pos = 0;
        }

        public long getLength() {
            return length;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

    }

}
