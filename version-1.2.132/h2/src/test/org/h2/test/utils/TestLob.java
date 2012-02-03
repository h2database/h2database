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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;
import org.h2.constant.ErrorCode;
import org.h2.message.DbException;
import org.h2.tools.DeleteDbFiles;
import org.h2.util.IOUtils;
import org.h2.util.New;
import org.h2.util.Profiler;

/**
 * Implementation and tests of a LOB storage mechanism that splits LOBs in
 * blocks.
 */
public class TestLob {

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
    class LobInputStream extends InputStream {

        private byte[] buffer;
        private int pos;
        private long remaining;
        private long lob;
        private int seq;

        LobInputStream(long lob, long length) {
            this.lob = lob;
            this.remaining = length;
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
                PreparedStatement prep = prepare(
                    "SELECT DATA FROM " + LOB_MAP + " M " +
                    "INNER JOIN " + LOB_DATA + " D ON M.BLOCK = D.BLOCK " +
                    "WHERE M.LOB = ? AND M.SEQ = ?");
                prep.setLong(1, lob);
                prep.setInt(2, seq);
                ResultSet rs = prep.executeQuery();
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

//        int len = 10 * 1024 * 1024;
//        block: 1394
//        regular: 725

//        int len = 16 * 1024;
//        block: 1817
//        regular: 4552

//        int len = 32 * 1024;
//        block: 1712 / 636
//                regular: 2255

        int len = 64 * 1024;
//        block: 1540 / 590
//        regular: 1385

//        int len = 1024 * 1024;
//        block: 1682 / 560
//        regular: 455

//        int len = 128 * 1024;
//        block: 1516
//        regular: 1020


        int repeat = 6;
        int count = (int) (52428800L / len);
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
        Profiler prof = new Profiler();
        prof.interval = 1;
        prof.startCollecting();
        int x = 0, y = 0;
        ArrayList<LobId> list = New.arrayList();
        for (int j = 0; j < repeat; j++) {
            boolean regular = (j & 1) == 1;
            long time = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                // random.nextBytes(buff);
                if (regular) {
                    prep.setInt(1, x++);
                    prep.setBinaryStream(2, new ByteArrayInputStream(buff), len);
                    prep.execute();
                } else {
                    LobId id = addLob(new ByteArrayInputStream(buff), -1, -1);
                    list.add(id);
                }
            }
            System.out.println((regular ? "regular: " : "block: ") + (System.currentTimeMillis() - time));
        }

        System.out.println("read------------");
        x = 0;
        y = 0;
        prep = conn.prepareStatement("select data from test where id = ?");
        byte[] buff2 = new byte[1024];
        for (int j = 0; j < repeat; j++) {
            boolean regular = (j & 1) == 1;
            long time = System.currentTimeMillis();
            for (int i = 0; i < count * 10; i++) {
                InputStream in2 = null;
                if (regular) {
//                    prep.setInt(1, x++ % repeat);
//                    ResultSet rs = prep.executeQuery();
//                    rs.next();
//                    in2 = rs.getBinaryStream(1);
                } else {
                    in2 = getInputStream(list.get(y++ % repeat));
                }
                if (in2 != null) {
                    while (true) {
                        int len2 = in2.read(buff2);
                        if (len2 < 0) {
                            break;
                        }
                    }
                }
            }
            System.out.println((regular ? "regular: " : "block: ") + (System.currentTimeMillis() - time));
        }

        prof.stopCollecting();
        System.out.println(prof.getTop(5));
        c.close();
    }

    private void init(Connection newConn) throws SQLException {
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
    }

    /**
     * Create a prepared statement, or re-use an existing one.
     *
     * @param sql the SQL statement
     * @return the prepared statement
     */
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

    private LobId addLob(InputStream in, long maxLength, int table) throws SQLException {
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
            return new LobId(lob, length);
        } catch (IOException e) {
            deleteLob(lob);
            throw DbException.convertIOException(e, "adding blob");
        }
    }

    private InputStream getInputStream(LobId lobId) {
        long id = lobId.getId();
        long length = lobId.getLength();
        return new LobInputStream(id, length);
    }

}
