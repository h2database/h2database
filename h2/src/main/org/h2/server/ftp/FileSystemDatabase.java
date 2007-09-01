/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.ftp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import org.h2.util.IOUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.StringUtils;

public class FileSystemDatabase {
    private Connection conn;
    private HashMap preparedMap = new HashMap();
    private boolean log;

    FileSystemDatabase(Connection conn, boolean log) throws SQLException {
        this.conn = conn;
        this.log = log;
        Statement stat = conn.createStatement();
        conn.setAutoCommit(false);
        stat.execute("SET ALLOW_LITERALS NONE");
        stat.execute("CREATE TABLE IF NOT EXISTS FILES(" + "ID IDENTITY, PARENTID BIGINT, NAME VARCHAR, "
                + "LASTMODIFIED BIGINT, LENGTH BIGINT, " + "UNIQUE(PARENTID, NAME))");
        stat.execute("CREATE TABLE IF NOT EXISTS FILEDATA(" + "ID BIGINT PRIMARY KEY, DATA BLOB)");
        PreparedStatement prep = conn.prepareStatement("SET MAX_LENGTH_INPLACE_LOB ?");
        prep.setLong(1, 4096);
        prep.execute();
        commit();
        if (log) {
            ResultSet rs = stat.executeQuery("SELECT * FROM FILES ORDER BY PARENTID, NAME");
            while (rs.next()) {
                long id = rs.getLong("ID");
                long parentId = rs.getLong("PARENTID");
                String name = rs.getString("NAME");
                long lastModified = rs.getLong("LASTMODIFIED");
                long length = rs.getLong("LENGTH");
                System.out.println(id + " " + name + " parent:" + parentId + " length:" + length + " lastMod:"
                        + lastModified);
            }
        }
    }

    synchronized void delete(String fullName) {
        try {
            long id = getId(fullName, false);
            PreparedStatement prep = prepare("DELETE FROM FILES WHERE ID=?");
            prep.setLong(1, id);
            prep.execute();
            prep = prepare("DELETE FROM FILEDATA WHERE ID=?");
            prep.setLong(1, id);
            prep.execute();
            commit();
        } catch (SQLException e) {
            rollback();
            throw convert(e);
        }
    }

    synchronized boolean exists(String fullName) {
        long id = getId(fullName, false);
        return id >= 0;
    }

    synchronized void read(String fullName, long skip, OutputStream out) throws IOException {
        try {
            long id = getId(fullName, false);
            PreparedStatement prep = prepare("SELECT DATA FROM FILEDATA WHERE ID=?");
            prep.setLong(1, id);
            ResultSet rs = prep.executeQuery();
            if (rs.next()) {
                InputStream in = rs.getBinaryStream(1);
                IOUtils.skipFully(in, skip);
                IOUtils.copyAndClose(in, out);
            }
        } catch (SQLException e) {
            throw convert(e);
        }
    }

    synchronized void write(String fullName, InputStream in) throws IOException {
        try {
            long id = getId(fullName, false);
            if (id >= 0) {
                PreparedStatement prep = prepare("DELETE FROM FILES WHERE ID=?");
                prep.setLong(1, id);
                prep.execute();
                prep = prepare("DELETE FROM FILEDATA WHERE ID=?");
                prep.setLong(1, id);
                prep.execute();
            }
            long parentId = getId(fullName, true);
            PreparedStatement prep = prepare("INSERT INTO FILES(PARENTID, NAME, LASTMODIFIED) VALUES(?, ?, ?)");
            prep.setLong(1, parentId);
            prep.setString(2, getName(fullName));
            prep.setLong(3, System.currentTimeMillis());
            prep.execute();
            ResultSet rs = JdbcUtils.getGeneratedKeys(prep);
            rs.next();
            id = rs.getLong(1);
            prep = prepare("INSERT INTO FILEDATA(ID, DATA) VALUES(?, ?)");
            prep.setLong(1, id);
            prep.setBinaryStream(2, in, -1);
            in.close();
            prep.execute();
            prep = prepare("UPDATE FILES SET LENGTH=(SELECT LENGTH(DATA) FROM FILEDATA WHERE ID=?) WHERE ID=?");
            prep.setLong(1, id);
            prep.setLong(2, id);
            prep.execute();
            commit();
        } catch (SQLException e) {
            rollback();
            throw convert(e);
        }
    }

    synchronized boolean isDirectory(String fullName) {
        try {
            long id = getId(fullName, false);
            PreparedStatement prep = prepare("SELECT LENGTH FROM FILES WHERE ID=?");
            prep.setLong(1, id);
            ResultSet rs = prep.executeQuery();
            rs.next();
            rs.getLong(1);
            return rs.wasNull();
        } catch (SQLException e) {
            throw convert(e);
        }
    }

    synchronized long lastModified(String fullName) {
        try {
            long id = getId(fullName, false);
            PreparedStatement prep = prepare("SELECT LASTMODIFIED FROM FILES WHERE ID=?");
            prep.setLong(1, id);
            ResultSet rs = prep.executeQuery();
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            throw convert(e);
        }
    }

    synchronized long length(String fullName) {
        try {
            long id = getId(fullName, false);
            PreparedStatement prep = prepare("SELECT LENGTH FROM FILES WHERE ID=?");
            prep.setLong(1, id);
            ResultSet rs = prep.executeQuery();
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            throw convert(e);
        }
    }

    synchronized FileObject[] listFiles(String fullName) {
        try {
            String name = fullName;
            if (!name.endsWith("/")) {
                name += "/";
            }
            long id = getId(fullName, false);
            PreparedStatement prep = prepare("SELECT NAME FROM FILES WHERE PARENTID=? ORDER BY NAME");
            prep.setLong(1, id);
            ResultSet rs = prep.executeQuery();
            ArrayList list = new ArrayList();
            while (rs.next()) {
                FileObject f = FileObjectDatabase.get(this, name + rs.getString(1));
                list.add(f);
            }
            FileObject[] result = new FileObject[list.size()];
            list.toArray(result);
            return result;
        } catch (SQLException e) {
            throw convert(e);
        }
    }

    String getName(String fullName) {
        String[] path = StringUtils.arraySplit(fullName, '/', false);
        return path[path.length - 1];
    }

    private long getId(String fullName, boolean parent) {
        try {
            String[] path = StringUtils.arraySplit(fullName, '/', false);
            long id = 0;
            int len = parent ? path.length - 1 : path.length;
            if (fullName.endsWith("/")) {
                len--;
            }
            for (int i = 0; i < len; i++) {
                PreparedStatement prep = prepare("SELECT ID FROM FILES WHERE PARENTID=? AND NAME=?");
                prep.setLong(1, id);
                prep.setString(2, path[i]);
                ResultSet rs = prep.executeQuery();
                if (!rs.next()) {
                    return -1;
                }
                id = rs.getLong(1);
            }
            return id;
        } catch (SQLException e) {
            throw convert(e);
        }
    }

    synchronized void mkdirs(String fullName) {
        try {
            String[] path = StringUtils.arraySplit(fullName, '/', false);
            long parentId = 0;
            int len = path.length;
            if (fullName.endsWith("/")) {
                len--;
            }
            for (int i = 0; i < len; i++) {
                PreparedStatement prep = prepare("SELECT ID FROM FILES WHERE PARENTID=? AND NAME=?");
                prep.setLong(1, parentId);
                prep.setString(2, path[i]);
                ResultSet rs = prep.executeQuery();
                if (!rs.next()) {
                    prep = prepare("INSERT INTO FILES(NAME, PARENTID, LASTMODIFIED) VALUES(?, ?, ?)");
                    prep.setString(1, path[i]);
                    prep.setLong(2, parentId);
                    prep.setLong(3, System.currentTimeMillis());
                    prep.execute();
                    rs = JdbcUtils.getGeneratedKeys(prep);
                    rs.next();
                    parentId = rs.getLong(1);
                } else {
                    parentId = rs.getLong(1);
                }
            }
            commit();
        } catch (SQLException e) {
            rollback();
            throw convert(e);
        }
    }

    synchronized boolean renameTo(String oldFullName, String newFullName) {
        try {
            long parentOld = getId(oldFullName, true);
            long parentNew = getId(newFullName, true);
            if (parentOld != parentNew) {
                return false;
            }
            String newName = getName(newFullName);
            long id = getId(oldFullName, false);
            PreparedStatement prep = prepare("UPDATE FILES SET NAME=? WHERE ID=?");
            prep.setString(1, newName);
            prep.setLong(2, id);
            prep.execute();
            commit();
            return true;
        } catch (SQLException e) {
            rollback();
            throw convert(e);
        }
    }

    private RuntimeException convert(SQLException e) {
        if (log) {
            e.printStackTrace();
        }
        return new RuntimeException(e.toString());
    }

    private PreparedStatement prepare(String sql) throws SQLException {
        PreparedStatement prep = (PreparedStatement) preparedMap.get(sql);
        if (prep == null) {
            prep = conn.prepareStatement(sql);
            preparedMap.put(sql, prep);
        }
        return prep;
    }

    private void commit() {
        try {
            conn.commit();
        } catch (SQLException e) {
            if (log) {
                e.printStackTrace();
            }
        }
    }

    private void rollback() {
        try {
            conn.rollback();
        } catch (SQLException e) {
            if (log) {
                e.printStackTrace();
            }
        }
    }

}
