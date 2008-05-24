/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.SQLException;
import java.util.Properties;

import org.h2.constant.SysProperties;
import org.h2.message.Message;
import org.h2.message.TraceSystem;
import org.h2.store.fs.FileSystem;

/**
 * This utility class supports basic operations on files
 */
public class FileUtils {

    public static void setLength(RandomAccessFile file, long newLength) throws IOException {
        try {
            trace("setLength", null, file);
            file.setLength(newLength);
        } catch (IOException e) {
            long length = file.length();
            if (newLength < length) {
                throw e;
            }
            long pos = file.getFilePointer();
            file.seek(length);
            long remaining = newLength - length;
            int maxSize = 1024 * 1024;
            int block = (int) Math.min(remaining, maxSize);
            byte[] buffer = new byte[block];
            while (remaining > 0) {
                int write = (int) Math.min(remaining, maxSize);
                file.write(buffer, 0, write);
                remaining -= write;
            }
            file.seek(pos);
        }
    }

    public static synchronized Properties loadProperties(String fileName) throws IOException {
        Properties prop = new SortedProperties();
        if (exists(fileName)) {
            InputStream in = openFileInputStream(fileName);
            try {
                prop.load(in);
            } finally {
                in.close();
            }
        }
        return prop;
    }

    public static boolean getBooleanProperty(Properties prop, String key, boolean def) {
        String value = prop.getProperty(key, ""+def);
        try {
            return Boolean.valueOf(value).booleanValue();
        } catch (Exception e) {
            TraceSystem.traceThrowable(e);
            return def;
        }
    }

    public static int getIntProperty(Properties prop, String key, int def) {
        String value = prop.getProperty(key, ""+def);
        try {
            return MathUtils.decodeInt(value);
        } catch (Exception e) {
            TraceSystem.traceThrowable(e);
            return def;
        }
    }

    public static String getFileInUserHome(String fileName) {
        String userDir = SysProperties.USER_HOME;
        if (userDir == null) {
            return fileName;
        }
        File file = new File(userDir, fileName);
        return file.getAbsolutePath();
    }

    public static void trace(String method, String fileName, Object o) {
        if (SysProperties.TRACE_IO) {
            System.out.println("FileUtils." + method + " " + fileName + " " + o);
        }
    }

    public static String getFileName(String name) throws SQLException {
        return FileSystem.getInstance(name).getFileName(name);
    }

    public static String normalize(String fileName) throws SQLException {
        return FileSystem.getInstance(fileName).normalize(fileName);
    }

    public static void tryDelete(String fileName) {
        FileSystem.getInstance(fileName).tryDelete(fileName);
    }

    public static boolean isReadOnly(String fileName) {
        return FileSystem.getInstance(fileName).isReadOnly(fileName);
    }

    public static boolean exists(String fileName) {
        return FileSystem.getInstance(fileName).exists(fileName);
    }

    public static long length(String fileName) {
        return FileSystem.getInstance(fileName).length(fileName);
    }

    /**
     * Create a new temporary file.
     * 
     * @param prefix the prefix of the file name (including directory name if
     *            required)
     * @param suffix the suffix
     * @param deleteOnExit if the file should be deleted when the virtual
     *            machine exists
     * @param inTempDir if the file should be stored in the temporary directory
     * @return the name of the created file
     */
    public static String createTempFile(String prefix, String suffix, boolean deleteOnExit, boolean inTempDir)
            throws IOException {
        return FileSystem.getInstance(prefix).createTempFile(prefix, suffix, deleteOnExit, inTempDir);
    }

    public static String getParent(String fileName) {
        return FileSystem.getInstance(fileName).getParent(fileName);
    }

    public static String[] listFiles(String path) throws SQLException {
        return FileSystem.getInstance(path).listFiles(path);
    }

    public static boolean isDirectory(String fileName) {
        return FileSystem.getInstance(fileName).isDirectory(fileName);
    }

    public static boolean isAbsolute(String fileName) {
        return FileSystem.getInstance(fileName).isAbsolute(fileName);
    }

    public static String getAbsolutePath(String fileName) {
        return FileSystem.getInstance(fileName).getAbsolutePath(fileName);
    }

    public static Writer openFileWriter(String fileName, boolean append) throws SQLException {
        OutputStream out = FileSystem.getInstance(fileName).openFileOutputStream(fileName, append);
        try {
            return new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw Message.convert(e);
        }
    }

    public static boolean fileStartsWith(String fileName, String prefix) {
        return FileSystem.getInstance(fileName).fileStartsWith(fileName, prefix);
    }

    public static InputStream openFileInputStream(String fileName) throws IOException {
        return FileSystem.getInstance(fileName).openFileInputStream(fileName);
    }

    public static OutputStream openFileOutputStream(String fileName, boolean append) throws SQLException {
        return FileSystem.getInstance(fileName).openFileOutputStream(fileName, append);
    }

    public static void rename(String oldName, String newName) throws SQLException {
        FileSystem.getInstance(oldName).rename(oldName, newName);
    }

    public static void createDirs(String fileName) throws SQLException {
        FileSystem.getInstance(fileName).createDirs(fileName);
    }

    public static void delete(String fileName) throws SQLException {
        FileSystem.getInstance(fileName).delete(fileName);
    }

    public static long getLastModified(String fileName) {
        return FileSystem.getInstance(fileName).getLastModified(fileName);
    }

}
