/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.utils;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import org.h2.constant.ErrorCode;
import org.h2.message.DbException;
import org.h2.tools.DeleteDbFiles;
import org.h2.util.Utils;
import org.h2.util.IOUtils;

/**
 * Implementation and tests of a LOB storage mechanism that splits LOBs in
 * blocks.
 */
public class TestLob {

    private static final int BLOCK_LENGTH = 20000;
    private Connection conn;
    private PreparedStatement prepInsertBlock, prepSelectBlock, prepDeleteBlock;
    private PreparedStatement prepInsertLob, prepSelectLob, prepDeleteLob;
    private long nextLob;
    private long nextBlock;

    /**
     * The LOB identifier.
     */
    private static class LobId {

        private long id;
        private long length;

        LobId(long id, long length) {
            this.id = id;
            this.length = length;
        }

        LobId(byte[] key) {
            id = Utils.readLong(key, 0);
            length = Utils.readLong(key, 8);
        }

        byte[] getKey() {
            byte[] key = new byte[16];
            Utils.writeLong(key, 0, id);
            Utils.writeLong(key, 0, length);
            return key;
        }

        long getId() {
            return id;
        }

        long getLength() {
            return length;
        }

    }

    /**
     * An input stream that reads from a LOB.
     */
    static class LobInputStream extends InputStream {

        private byte[] buffer = new byte[BLOCK_LENGTH];
        private PreparedStatement prepSelectBlock;
        private int bufferEnd;
        private long remaining;
        private long next;
        private int pos;

        LobInputStream(PreparedStatement prepSelectBlock, long first, long length) {
            this.next = first;
            this.remaining = length;
            this.prepSelectBlock = prepSelectBlock;
        }

        public int read() throws IOException {
            fillBuffer();
            if (pos >= bufferEnd) {
                return -1;
            }
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
            int todo;
//            while (length > 0) {
//                fillBuffer();
//
//                if (r < 0) {
//                    break;
//                }
//                read += r;
//                off += r;
//                len -= r;
//            }
//            return read == 0 ? -1 : read;
//
//            int len = 0;
//            while (length > 0 && remaining > 0) {
//            }
            return read;
        }

        private void fillBuffer() throws IOException {
            if (buffer != null && pos < bufferEnd) {
                return;
            }
            if (remaining <= 0) {
                return;
            }
            try {
                prepSelectBlock.setLong(1, next);
                ResultSet rs = prepSelectBlock.executeQuery();
                if (!rs.next()) {
                    SQLException e = DbException.get(ErrorCode.IO_EXCEPTION_1, "block: "+ next).getSQLException();
                    IOException io = new EOFException("Unexpected end of stream");
                    io.initCause(e);
                    throw e;
                }
            } catch (SQLException e) {
                throw DbException.convertToIOException(e);
            }
            int todo;
//
//
//            int len = readInt();
//            if (decompress == null) {
//                // EOF
//                this.bufferLength = 0;
//            } else if (len < 0) {
//                len = -len;
//                buffer = ensureSize(buffer, len);
//                readFully(buffer, len);
//                this.bufferLength = len;
//            } else {
//                inBuffer = ensureSize(inBuffer, len);
//                int size = readInt();
//                readFully(inBuffer, len);
//                buffer = ensureSize(buffer, size);
//                decompress.expand(inBuffer, 0, len, buffer, 0, size);
//                this.bufferLength = size;
//            }
//            pos = 0;
        }


    }

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        new TestLob().test();
    }

    private void test() throws Exception {
        DeleteDbFiles.execute("data", "test", true);
        org.h2.Driver.load();
        Connection c = DriverManager.getConnection("jdbc:h2:data/test");
        init(c);
        c = DriverManager.getConnection("jdbc:h2:data/test");

        int len = 128 * 1024;
        byte[] buff = new byte[len];
        Random random = new Random(1);
        random.nextBytes(buff);

        LobId lob = addLob(new ByteArrayInputStream(buff), -1, -1);
        InputStream in = getInputStream(lob);
        for (int i = 0; i < len; i++) {
            int x = in.read();
            if (x != (buff[i] & 255)) {
                throw new AssertionError();
            }
        }
        if (in.read() != -1) {
            throw new AssertionError();
        }

        Statement stat = c.createStatement();
        stat.execute("create table test(id int primary key, data blob)");
        PreparedStatement prep = conn.prepareStatement("insert into test(id, data) values(?, ?)");
//        Profiler prof = new Profiler();
//        prof.setInterval(1);
//        prof.startCollecting();
        int x = 0;
        for (int j = 0; j < 6; j++) {
            boolean regular = (j & 1) == 1;
            long time = System.currentTimeMillis();
            for (int i = 0; i < 400; i++) {
                prep.setInt(1, x++);
                if (regular) {
                    prep.setBinaryStream(2, new ByteArrayInputStream(buff), len);
                    prep.execute();
                } else {
                    addLob(new ByteArrayInputStream(buff), -1, -1);
                }
            }
            System.out.println((regular ? "regular: " : "block: ") + (System.currentTimeMillis() - time));
        }
//        prof.stopCollecting();
//        System.out.println(prof.getTop(5));
        c.close();
    }

    private void init(Connection newConn) throws SQLException {
        this.conn = newConn;
        Statement stat = conn.createStatement();
        stat.execute("set undo_log 0");
        // stat.execute("set redo_log_binary 0");
        // TODO support incremental garbage collection
        stat.execute("create table if not exists block(id bigint primary key, next bigint, data binary)");
        stat.execute("create table if not exists lob(id bigint primary key, length bigint, block bigint, table int)");
        ResultSet rs;
        rs = stat.executeQuery("select max(id) from block");
        rs.next();
        nextBlock = rs.getLong(1) + 1;
        rs = stat.executeQuery("select max(id) from lob");
        rs.next();
        nextLob = rs.getLong(1) + 1;
        prepInsertBlock = conn.prepareStatement("insert into block values(?, ?, ?)");
        prepDeleteBlock = conn.prepareStatement("delete from block where id = ?");
        prepInsertLob = conn.prepareStatement("insert into lob values(?, ?, ?, ?)");
        prepSelectBlock = conn.prepareStatement("select id, next, data from block where id = ?");
    }

    private LobId addLob(InputStream in, long maxLength, int table) throws SQLException {
        byte[][] buff = new byte[2][BLOCK_LENGTH];
        if (maxLength < 0) {
            maxLength = Long.MAX_VALUE;
        }
        long length = 0;
        int blockId = 0;
        long firstBlock = nextBlock;
        long lob = nextLob++;
        try {
            for (; maxLength > 0; blockId++) {
                int len = IOUtils.readFully(in, buff[blockId & 1], 0, BLOCK_LENGTH);
                if (len <= 0) {
                    break;
                }
                length += len;
                maxLength -= len;
                if (blockId > 0) {
                    prepInsertBlock.setLong(1, nextBlock++);
                    prepInsertBlock.setLong(2, nextBlock);
                    prepInsertBlock.setBytes(3, buff[(blockId - 1) & 1]);
                    prepInsertBlock.execute();
                }
            }
            prepInsertBlock.setLong(1, nextBlock++);
            prepInsertBlock.setLong(2, firstBlock);
            prepInsertBlock.setBytes(3, buff[(blockId - 1) & 1]);
            prepInsertBlock.execute();
            prepInsertLob.setLong(1, lob);
            prepInsertLob.setLong(2, length);
            prepInsertLob.setLong(3, firstBlock);
            prepInsertLob.setInt(4, table);
            prepInsertLob.execute();
            return new LobId(lob, length);
        } catch (IOException e) {
            deleteBlocks(firstBlock, nextBlock - 1);
            throw DbException.convertIOException(e, "adding blob");
        }
    }

    private InputStream getInputStream(LobId lobId) {
        long id = lobId.getId();
        long length = lobId.getLength();
        return new LobInputStream(prepSelectBlock, id, length);
    }

    private void deleteBlocks(long first, long last) throws SQLException {
        for (long id = first; id <= last; id++) {
            prepDeleteBlock.setLong(1, id);
            prepDeleteBlock.execute();
        }
    }

}
