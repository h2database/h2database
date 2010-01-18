/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.message.Message;
import org.h2.tools.DeleteDbFiles;
import org.h2.util.ByteUtils;
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
            id = ByteUtils.readLong(key, 0);
            length = ByteUtils.readLong(key, 8);
        }

        byte[] getKey() {
            byte[] key = new byte[16];
            ByteUtils.writeLong(key, 0, id);
            ByteUtils.writeLong(key, 0, length);
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

        private byte[] page = new byte[BLOCK_LENGTH];
        private byte[] buff = new byte[1];
        private PreparedStatement prepSelectBlock;
        private long remaining;
        private long offset;
        private long next;

        LobInputStream(PreparedStatement prepSelectBlock, long first, long length) {
            this.next = first;
            this.remaining = length;
            this.prepSelectBlock = prepSelectBlock;
        }

        public int read() throws IOException {
            int len = readFully(buff, 0, 1);
            if (len == 0) {
                return -1;
            }
            return buff[0] & 255;
        }

        public int read(byte[] buff) throws IOException {
            return readFully(buff, 0, buff.length);
        }

        public int read(byte[] buff, int off, int length) throws IOException {
            return readFully(buff, 0, buff.length);
        }

        private int readFully(byte[] buff, int off, int length) throws IOException {
            int len = 0;
            while (length > 0 && remaining > 0) {
            }
            return len;
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

    private void test() throws SQLException {
        DeleteDbFiles.execute("data", "test", true);
        org.h2.Driver.load();
        Connection c = DriverManager.getConnection("jdbc:h2:data/test");
        init(c);
        c = DriverManager.getConnection("jdbc:h2:data/test");
        int len = 128 * 1024;
        byte[] buff = new byte[len];
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
            throw Message.convertIOException(e, "adding blob");
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
