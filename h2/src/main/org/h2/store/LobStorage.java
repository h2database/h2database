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
import org.h2.message.DbException;
import org.h2.util.IOUtils;
import org.h2.util.New;
import org.h2.value.ValueLob;

public class LobStorage {

    /**
     * The 'table id' to use for session variables.
     */
    public static final int TABLE_ID_SESSION_VARIABLE = -1;

    // TODO test recovery

    private static final String LOBS = "INFORMATION_SCHEMA.LOBS";
    private static final String LOB_MAP = "INFORMATION_SCHEMA.LOB_MAP";
    private static final String LOB_DATA = "INFORMATION_SCHEMA.LOB_DATA";

    private static final int BLOCK_LENGTH = 20000;
    private static final boolean HASH = true;
    private static final long UNIQUE = 0xffff;
    private Connection conn;
    private HashMap<String, PreparedStatement> prepared = New.hashMap();
    private long nextLob;
    private long nextBlock;

    public static void removeAllForTable(DataHandler handler, int tableId) {
        if (SysProperties.LOB_IN_DATABASE) {
            // remove both lobs in the database as well as in the file system
        }
        ValueLob.removeAllForTable(handler, tableId);
    }

    public static ValueLob createSmallLob(int type, byte[] small) {
        if (SysProperties.LOB_IN_DATABASE) {
            return null;
        }
        return ValueLob.createSmallLob(type, small);
    }

    public static ValueLob createBlob(InputStream in, long length, DataHandler handler) {
        if (SysProperties.LOB_IN_DATABASE) {
            return null;
        }
        return ValueLob.createBlob(in, length, handler);
    }

    public static ValueLob createClob(Reader in, long length, DataHandler handler) {
        if (SysProperties.LOB_IN_DATABASE) {
            return null;
        }
        return ValueLob.createClob(in, length, handler);
    }

    /**
     * An input stream that reads from a LOB.
     */
    public static class LobInputStream extends InputStream {

        private Connection conn;
        private PreparedStatement prepSelect;
        private byte[] buffer;
        private int pos;
        private long remaining;
        private long lob;
        private int seq;

        public LobInputStream(Connection conn, long lob) throws IOException {
            try {
                this.lob = lob;
                PreparedStatement prep = conn.prepareStatement(
                        "SELECT LENGTH FROM " + LOBS + " WHERE ID = ?");
                prep.setLong(1, lob);
                ResultSet rs = prep.executeQuery();
                if (!rs.next()) {
                    throw DbException.get(ErrorCode.IO_EXCEPTION_1, "lob: "+ lob + " seq: " + seq).getSQLException();

                }
            } catch (SQLException e) {
                throw DbException.convertToIOException(e);
            }
        }

        public int read() throws IOException {
            fillBuffer();
            if (remaining <= 0) {
                return -1;
            }
            remaining--;
            return buffer[pos++] & 255;
        }

        public int read(byte[] buff) throws IOException {
            return readFully(buff, 0, buff.length);
        }

        public int read(byte[] buff, int off, int length) throws IOException {
            return readFully(buff, 0, buff.length);
        }

        private int readFully(byte[] buff, int off, int length) throws IOException {
            if (length == 0) {
                return 0;
            }
            int read = 0;
            while (length > 0) {
                fillBuffer();
                if (remaining <= 0) {
                    break;
                }
                int len = (int) Math.min(length, remaining);
                len = Math.min(len, buffer.length - pos);
                System.arraycopy(buffer, pos, buff, off, len);
                read += len;
                remaining -= len;
                off += len;
                length -= len;
            }
            return read == 0 ? -1 : read;
        }

        private void fillBuffer() throws IOException {
            if (buffer != null && pos < buffer.length) {
                return;
            }
            if (remaining <= 0) {
                return;
            }
            try {
                if (prepSelect == null) {
                    prepSelect = conn.prepareStatement(
                        "SELECT DATA FROM " + LOB_MAP + " M " +
                        "INNER JOIN " + LOB_DATA + " D ON M.BLOCK = D.BLOCK " +
                        "WHERE M.LOB = ? AND M.SEQ = ?");
                }
                prepSelect.setLong(1, lob);
                prepSelect.setInt(2, seq);
                ResultSet rs = prepSelect.executeQuery();
                if (!rs.next()) {
                    throw DbException.get(ErrorCode.IO_EXCEPTION_1, "lob: "+ lob + " seq: " + seq).getSQLException();
                }
                seq++;
                buffer = rs.getBytes(1);
                pos = 0;
            } catch (SQLException e) {
                throw DbException.convertToIOException(e);
            }
        }

    }

    public LobStorage(Connection newConn) {
        try {
            this.conn = newConn;
            Statement stat = conn.createStatement();
            // stat.execute("SET UNDO_LOG 0");
            // stat.execute("SET REDO_LOG_BINARY 0");
            stat.execute("CREATE TABLE IF NOT EXISTS " + LOBS + "(ID BIGINT PRIMARY KEY, LENGTH BIGINT, TABLE INT)");
            stat.execute("CREATE TABLE IF NOT EXISTS " + LOB_MAP + "(LOB BIGINT, SEQ INT, BLOCK BIGINT, PRIMARY KEY(LOB, SEQ))");
            stat.execute("CREATE INDEX INFORMATION_SCHEMA.INDEX_LOB_MAP_DATA_LOB ON " + LOB_MAP + "(BLOCK, LOB)");
            stat.execute("CREATE TABLE IF NOT EXISTS " + LOB_DATA + "(BLOCK BIGINT PRIMARY KEY, DATA BINARY)");
            ResultSet rs;
            rs = stat.executeQuery("SELECT MAX(BLOCK) FROM " + LOB_DATA);
            rs.next();
            nextBlock = rs.getLong(1) + 1;
            if (HASH) {
                nextBlock = Math.max(UNIQUE + 1, nextLob);
            }
            rs = stat.executeQuery("SELECT MAX(ID) FROM " + LOBS);
            rs.next();
            nextLob = rs.getLong(1) + 1;
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    protected synchronized PreparedStatement prepare(String sql) throws SQLException {
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

    public long addLob(InputStream in, long maxLength, int table) throws SQLException {
        byte[] buff = new byte[BLOCK_LENGTH];
        if (maxLength < 0) {
            maxLength = Long.MAX_VALUE;
        }
        long length = 0;
        long lob = nextLob++;
        try {
            for (int seq = 0; maxLength > 0; seq++) {
                int len = IOUtils.readFully(in, buff, 0, BLOCK_LENGTH);
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
                long block;
                boolean blockExists = false;
                if (HASH) {
                    block = Arrays.hashCode(b) & UNIQUE;
                    int todoSynchronize;
                    PreparedStatement prep = prepare(
                            "SELECT DATA FROM " + LOB_DATA +
                            " WHERE BLOCK = ?");
                    prep.setLong(1, block);
                    ResultSet rs = prep.executeQuery();
                    if (rs.next()) {
                        byte[] compare = rs.getBytes(1);
                        if (Arrays.equals(b, compare)) {
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
                            "INSERT INTO " + LOB_DATA + "(BLOCK, DATA) VALUES(?, ?)");
                    prep.setLong(1, block);
                    prep.setBytes(2, b);
                    prep.execute();
                }
                PreparedStatement prep = prepare(
                        "INSERT INTO " + LOB_MAP + "(LOB, SEQ, BLOCK) VALUES(?, ?, ?)");
                prep.setLong(1, lob);
                prep.setInt(2, seq);
                prep.setLong(3, block);
                prep.execute();
            }
            PreparedStatement prep = prepare(
                    "INSERT INTO " + LOBS + "(ID, LENGTH, TABLE) VALUES(?, ?, ?)");
            prep.setLong(1, lob);
            prep.setLong(2, length);
            prep.setInt(3, table);
            prep.execute();
            return lob;
        } catch (IOException e) {
            deleteLob(lob);
            throw DbException.convertIOException(e, "adding blob");
        }
    }

    public InputStream getInputStream(long id) throws IOException {
        return new LobInputStream(conn, id);
    }

}
