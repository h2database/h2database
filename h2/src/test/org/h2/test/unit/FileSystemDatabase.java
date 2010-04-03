/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
import java.util.Properties;

import org.h2.Driver;
import org.h2.message.DbException;
import org.h2.store.fs.FileObject;
import org.h2.store.fs.FileObjectInputStream;
import org.h2.store.fs.FileObjectOutputStream;
import org.h2.store.fs.FileSystem;
import org.h2.util.IOUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.New;
import org.h2.util.StringUtils;

/**
 * This file system stores everything in a database.
 */
public class FileSystemDatabase extends FileSystem {

    private Connection conn;
    private String url;
    private HashMap<String, PreparedStatement> preparedMap = New.hashMap();
    private boolean log;

    private FileSystemDatabase(String url, Connection conn, boolean log) throws SQLException {
        this.url = url;
        this.conn = conn;
        this.log = log;
        Statement stat = conn.createStatement();
        conn.setAutoCommit(false);
        stat.execute("SET ALLOW_LITERALS NONE");
        stat.execute("CREATE TABLE IF NOT EXISTS FILES("
                + "ID IDENTITY, PARENTID BIGINT, NAME VARCHAR, "
                + "LASTMODIFIED BIGINT, LENGTH BIGINT, "
                + "UNIQUE(PARENTID, NAME))");
        stat.execute("CREATE TABLE IF NOT EXISTS FILEDATA("
                + "ID BIGINT PRIMARY KEY, DATA BLOB)");
        PreparedStatement prep = conn.prepareStatement("SET MAX_LENGTH_INPLACE_LOB ?");
        prep.setLong(1, 4096);
        prep.execute();
        stat.execute("MERGE INTO FILES VALUES(ZERO(), NULL, SPACE(ZERO()), ZERO(), NULL)");
        commit();
        if (log) {
            ResultSet rs = stat.executeQuery("SELECT * FROM FILES ORDER BY PARENTID, NAME");
            while (rs.next()) {
                long id = rs.getLong("ID");
                long parentId = rs.getLong("PARENTID");
                String name = rs.getString("NAME");
                long lastModified = rs.getLong("LASTMODIFIED");
                long length = rs.getLong("LENGTH");
                log(id + " " + name + " parent:" + parentId + " length:" + length + " lastMod:"
                        + lastModified);
            }
        }
    }

    protected boolean accepts(String fileName) {
        return fileName.startsWith(url);
    }

    public static synchronized FileSystemDatabase register(String url) {
        Connection conn;
        try {
            if (url.startsWith("jdbc:h2:")) {
                // avoid using DriverManager if possible
                conn = Driver.load().connect(url, new Properties());
            } else {
                conn = JdbcUtils.getConnection(null, url, new Properties());
            }
            boolean log = url.toUpperCase().indexOf("TRACE_") >= 0;
            FileSystemDatabase fs = new FileSystemDatabase(url, conn, log);
            FileSystem.register(fs);
            return fs;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Close the underlying database and unregister the file system.
     */
    public void unregister() {
        JdbcUtils.closeSilently(conn);
        FileSystem.unregister(this);
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

    private void log(String s) {
        if (log) {
            System.out.println(s);
        }
    }

    private long getId(String fileName, boolean parent) {
        fileName = translateFileName(fileName);
        log(fileName);
        try {
            String[] path = StringUtils.arraySplit(fileName, '/', false);
            long id = 0;
            int len = parent ? path.length - 1 : path.length;
            if (fileName.endsWith("/")) {
                len--;
            }
            for (int i = 1; i < len; i++) {
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

    private String translateFileName(String fileName) {
        if (fileName.startsWith(url)) {
            fileName = fileName.substring(url.length());
        }
        return fileName;
    }

    private PreparedStatement prepare(String sql) throws SQLException {
        PreparedStatement prep = preparedMap.get(sql);
        if (prep == null) {
            prep = conn.prepareStatement(sql);
            preparedMap.put(sql, prep);
        }
        return prep;
    }

    private RuntimeException convert(SQLException e) {
        if (log) {
            e.printStackTrace();
        }
        return new RuntimeException(e.toString(), e);
    }

    public boolean canWrite(String fileName) {
        return true;
    }

    public void copy(String original, String copy) {
        try {
            OutputStream out = openFileOutputStream(copy, false);
            InputStream in = openFileInputStream(original);
            IOUtils.copyAndClose(in, out);
        } catch (IOException e) {
            rollback();
            throw DbException.convertIOException(e, "Can not copy " + original + " to " + copy);
        }
    }

    public void createDirs(String fileName) {
        fileName = translateFileName(fileName);
        try {
            String[] path = StringUtils.arraySplit(fileName, '/', false);
            long parentId = 0;
            int len = path.length;
            if (fileName.endsWith("/")) {
                len--;
            }
            len--;
            for (int i = 1; i < len; i++) {
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
                    rs = prep.getGeneratedKeys();
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

    public boolean createNewFile(String fileName) {
        try {
            if (exists(fileName)) {
                return false;
            }
            openFileObject(fileName, "rw").close();
            return true;
        } catch (IOException e) {
            throw DbException.convertIOException(e, fileName);
        }
    }

    public synchronized void delete(String fileName) {
        try {
            long id = getId(fileName, false);
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

    public void deleteRecursive(String fileName, boolean tryOnly) {
        throw DbException.getUnsupportedException("db");
    }

    public boolean exists(String fileName) {
        long id = getId(fileName, false);
        return id >= 0;
    }

    public boolean fileStartsWith(String fileName, String prefix) {
        fileName = translateFileName(fileName);
        return fileName.startsWith(prefix);
    }

    public String getAbsolutePath(String fileName) {
        return fileName;
    }

    public String getFileName(String fileName) {
        fileName = translateFileName(fileName);
        String[] path = StringUtils.arraySplit(fileName, '/', false);
        return path[path.length - 1];
    }

    public synchronized  long getLastModified(String fileName) {
        try {
            long id = getId(fileName, false);
            PreparedStatement prep = prepare("SELECT LASTMODIFIED FROM FILES WHERE ID=?");
            prep.setLong(1, id);
            ResultSet rs = prep.executeQuery();
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            throw convert(e);
        }
    }

    public String getParent(String fileName) {
        int idx = Math.max(fileName.indexOf(':'), fileName.lastIndexOf('/'));
        return fileName.substring(0, idx);
    }

    public boolean isAbsolute(String fileName) {
        return true;
    }

    public synchronized boolean isDirectory(String fileName) {
        try {
            long id = getId(fileName, false);
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

    public boolean isReadOnly(String fileName) {
        return false;
    }

    public synchronized long length(String fileName) {
        try {
            long id = getId(fileName, false);
            PreparedStatement prep = prepare("SELECT LENGTH FROM FILES WHERE ID=?");
            prep.setLong(1, id);
            ResultSet rs = prep.executeQuery();
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            throw convert(e);
        }
    }

    public synchronized String[] listFiles(String path) {
        try {
            String name = path;
            if (!name.endsWith("/")) {
                name += "/";
            }
            long id = getId(path, false);
            PreparedStatement prep = prepare("SELECT NAME FROM FILES WHERE PARENTID=? ORDER BY NAME");
            prep.setLong(1, id);
            ResultSet rs = prep.executeQuery();
            ArrayList<String> list = New.arrayList();
            while (rs.next()) {
                list.add(name + rs.getString(1));
            }
            String[] result = new String[list.size()];
            list.toArray(result);
            return result;
        } catch (SQLException e) {
            throw convert(e);
        }
    }

    public String normalize(String fileName) {
        return fileName;
    }

    public InputStream openFileInputStream(String fileName) throws IOException {
        return new FileObjectInputStream(openFileObject(fileName, "r"));
    }

    public FileObject openFileObject(String fileName, String mode) throws IOException {
        try {
            long id = getId(fileName, false);
            PreparedStatement prep = prepare("SELECT DATA FROM FILEDATA WHERE ID=?");
            prep.setLong(1, id);
            ResultSet rs = prep.executeQuery();
            if (rs.next()) {
                InputStream in = rs.getBinaryStream(1);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                IOUtils.copyAndClose(in, out);
                byte[] data = out.toByteArray();
                return new FileObjectDatabase(this, fileName, data, false);
            }
            return new FileObjectDatabase(this, fileName, new byte[0], true);
        } catch (SQLException e) {
            throw convert(e);
        }
    }

    public OutputStream openFileOutputStream(String fileName, boolean append) {
        try {
            return new FileObjectOutputStream(openFileObject(fileName, "rw"), append);
        } catch (IOException e) {
            throw DbException.convertIOException(e, fileName);
        }
    }

    public synchronized void rename(String oldName, String newName) {
        try {
            long parentOld = getId(oldName, true);
            long parentNew = getId(newName, true);
            if (parentOld != parentNew) {
                throw DbException.getUnsupportedException("different parents");
            }
            newName = getFileName(newName);
            long id = getId(oldName, false);
            PreparedStatement prep = prepare("UPDATE FILES SET NAME=? WHERE ID=?");
            prep.setString(1, newName);
            prep.setLong(2, id);
            prep.execute();
            commit();
        } catch (SQLException e) {
            rollback();
            throw convert(e);
        }
    }

    public boolean tryDelete(String fileName) {
        delete(fileName);
        return true;
    }

    /**
     * Update a file in the file system.
     *
     * @param fileName the file name
     * @param b the data
     * @param len the number of bytes
     */
    synchronized void write(String fileName, byte[] b, int len) {
        try {
            long id = getId(fileName, false);
            if (id >= 0) {
                PreparedStatement prep = prepare("DELETE FROM FILES WHERE ID=?");
                prep.setLong(1, id);
                prep.execute();
                prep = prepare("DELETE FROM FILEDATA WHERE ID=?");
                prep.setLong(1, id);
                prep.execute();
            }
            long parentId = getId(fileName, true);
            PreparedStatement prep = prepare("INSERT INTO FILES(PARENTID, NAME, LASTMODIFIED) VALUES(?, ?, ?)");
            prep.setLong(1, parentId);
            prep.setString(2, getFileName(fileName));
            prep.setLong(3, System.currentTimeMillis());
            prep.execute();
            ResultSet rs = prep.getGeneratedKeys();
            rs.next();
            id = rs.getLong(1);
            prep = prepare("INSERT INTO FILEDATA(ID, DATA) VALUES(?, ?)");
            prep.setLong(1, id);
            ByteArrayInputStream in = new ByteArrayInputStream(b, 0, len);
            prep.setBinaryStream(2, in, -1);
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

}
