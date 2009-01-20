/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.sql.SQLException;

import org.h2.constant.SysProperties;
import org.h2.store.fs.FileSystem;

/**
 * This utility class supports basic operations on files
 */
public class FileUtils {

    private FileUtils() {
        // utility class
    }

    /**
     * Change the length of the file.
     *
     * @param file the random access file
     * @param newLength the new length
     */
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

    /**
     * Get the absolute file path of a file in the user home directory.
     *
     * @param fileName the file name
     * @return the absolute path
     */
    public static String getFileInUserHome(String fileName) {
        String userDir = SysProperties.USER_HOME;
        if (userDir == null) {
            return fileName;
        }
        File file = new File(userDir, fileName);
        return file.getAbsolutePath();
    }

    /**
     * Trace input or output operations if enabled.
     *
     * @param method the method from where this method was called
     * @param fileName the file name
     * @param o the object to append to the message
     */
    static void trace(String method, String fileName, Object o) {
        if (SysProperties.TRACE_IO) {
            System.out.println("FileUtils." + method + " " + fileName + " " + o);
        }
    }

    /**
     * Get the file name (without directory part).
     *
     * @param name the directory and file name
     * @return just the file name
     */
    public static String getFileName(String name) throws SQLException {
        return FileSystem.getInstance(name).getFileName(name);
    }

    /**
     * Normalize a file name.
     *
     * @param fileName the file name
     * @return the normalized file name
     */
    public static String normalize(String fileName) throws SQLException {
        return FileSystem.getInstance(fileName).normalize(fileName);
    }

    /**
     * Try to delete a file.
     *
     * @param fileName the file name
     */
    public static void tryDelete(String fileName) {
        FileSystem.getInstance(fileName).tryDelete(fileName);
    }

    /**
     * Check if a file is read-only.
     *
     * @param fileName the file name
     * @return if it is read only
     */
    public static boolean isReadOnly(String fileName) {
        return FileSystem.getInstance(fileName).isReadOnly(fileName);
    }

    /**
     * Checks if a file exists.
     *
     * @param fileName the file name
     * @return true if it exists
     */
    public static boolean exists(String fileName) {
        return FileSystem.getInstance(fileName).exists(fileName);
    }

    /**
     * Get the length of a file.
     *
     * @param fileName the file name
     * @return the length in bytes
     */
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

    /**
     * Get the parent directory of a file or directory.
     *
     * @param fileName the file or directory name
     * @return the parent directory name
     */
    public static String getParent(String fileName) {
        return FileSystem.getInstance(fileName).getParent(fileName);
    }

    /**
     * List the files in the given directory.
     *
     * @param path the directory
     * @return the list of fully qualified file names
     */
    public static String[] listFiles(String path) throws SQLException {
        return FileSystem.getInstance(path).listFiles(path);
    }

    /**
     * Check if it is a file or a directory.
     *
     * @param fileName the file or directory name
     * @return true if it is a directory
     */
    public static boolean isDirectory(String fileName) {
        return FileSystem.getInstance(fileName).isDirectory(fileName);
    }

    /**
     * Check if the file name includes a path.
     *
     * @param fileName the file name
     * @return if the file name is absolute
     */
    public static boolean isAbsolute(String fileName) {
        return FileSystem.getInstance(fileName).isAbsolute(fileName);
    }

    /**
     * Get the absolute file name.
     *
     * @param fileName the file name
     * @return the absolute file name
     */
    public static String getAbsolutePath(String fileName) {
        return FileSystem.getInstance(fileName).getAbsolutePath(fileName);
    }

    /**
     * Check if a file starts with a given prefix.
     *
     * @param fileName the complete file name
     * @param prefix the prefix
     * @return true if it starts with the prefix
     */
    public static boolean fileStartsWith(String fileName, String prefix) {
        return FileSystem.getInstance(fileName).fileStartsWith(fileName, prefix);
    }

    /**
     * Create an input stream to read from the file.
     *
     * @param fileName the file name
     * @return the input stream
     */
    public static InputStream openFileInputStream(String fileName) throws IOException {
        return FileSystem.getInstance(fileName).openFileInputStream(fileName);
    }

    /**
     * Create an output stream to write into the file.
     *
     * @param fileName the file name
     * @param append if true, the file will grow, if false, the file will be
     *            truncated first
     * @return the output stream
     */
    public static OutputStream openFileOutputStream(String fileName, boolean append) throws SQLException {
        return FileSystem.getInstance(fileName).openFileOutputStream(fileName, append);
    }

    /**
     * Rename a file if this is allowed.
     *
     * @param oldName the old fully qualified file name
     * @param newName the new fully qualified file name
     * @throws SQLException
     */
    public static void rename(String oldName, String newName) throws SQLException {
        FileSystem.getInstance(oldName).rename(oldName, newName);
    }

    /**
     * Create all required directories that are required for this file.
     *
     * @param fileName the file name (not directory name)
     */
    public static void createDirs(String fileName) throws SQLException {
        FileSystem.getInstance(fileName).createDirs(fileName);
    }

    /**
     * Delete a file.
     *
     * @param fileName the file name
     */
    public static void delete(String fileName) throws SQLException {
        FileSystem.getInstance(fileName).delete(fileName);
    }

    /**
     * Get the last modified date of a file
     *
     * @param fileName the file name
     * @return the last modified date
     */
    public static long getLastModified(String fileName) {
        return FileSystem.getInstance(fileName).getLastModified(fileName);
    }

}
