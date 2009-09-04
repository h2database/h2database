/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import org.h2.message.Message;
import org.h2.store.fs.FileObject;
import org.h2.store.fs.FileSystem;

/**
 * A debugging file system that logs all operations.
 */
public class FileSystemDebug extends FileSystem {

    /**
     * The prefix used for a debugging file system.
     */
    static final String PREFIX = "debug:";

    private static final FileSystemDebug INSTANCE = new FileSystemDebug();

    /**
     * Register the file system.
     */
    public static void register() {
        FileSystem.register(INSTANCE);
    }

    public boolean canWrite(String fileName) {
        fileName = translateFileName(fileName);
        debug("canWrite", fileName);
        return FileSystem.getInstance(fileName).canWrite(fileName);
    }

    public void copy(String original, String copy) throws SQLException {
        original = translateFileName(original);
        copy = translateFileName(copy);
        debug("copy", original, copy);
        FileSystem.getInstance(original).copy(original, copy);
    }

    public void createDirs(String fileName) throws SQLException {
        fileName = translateFileName(fileName);
        debug("createDirs", fileName);
        FileSystem.getInstance(fileName).createDirs(fileName);
    }

    public boolean createNewFile(String fileName) throws SQLException {
        fileName = translateFileName(fileName);
        debug("createNewFile", fileName);
        return FileSystem.getInstance(fileName).createNewFile(fileName);
    }

    public String createTempFile(String prefix, String suffix, boolean deleteOnExit, boolean inTempDir)
            throws IOException {
        prefix = translateFileName(prefix);
        debug("createTempFile", prefix, suffix, deleteOnExit, inTempDir);
        return PREFIX + FileSystem.getInstance(prefix).createTempFile(prefix, suffix, deleteOnExit, inTempDir);
    }

    public void delete(String fileName) throws SQLException {
        fileName = translateFileName(fileName);
        debug("fileName", fileName);
        FileSystem.getInstance(fileName).delete(fileName);
    }

    public void deleteRecursive(String directory, boolean tryOnly) throws SQLException {
        directory = translateFileName(directory);
        debug("deleteRecursive", directory);
        FileSystem.getInstance(directory).deleteRecursive(directory, tryOnly);
    }

    public boolean exists(String fileName) {
        fileName = translateFileName(fileName);
        debug("exists", fileName);
        return FileSystem.getInstance(fileName).exists(fileName);
    }

    public boolean fileStartsWith(String fileName, String prefix) {
        fileName = translateFileName(fileName);
        prefix = translateFileName(prefix);
        debug("fileStartsWith", fileName, prefix);
        return FileSystem.getInstance(fileName).fileStartsWith(fileName, prefix);
    }

    public String getAbsolutePath(String fileName) {
        fileName = translateFileName(fileName);
        debug("getAbsolutePath", fileName);
        return PREFIX + FileSystem.getInstance(fileName).getAbsolutePath(fileName);
    }

    public String getFileName(String name) throws SQLException {
        name = translateFileName(name);
        debug("getFileName", name);
        return FileSystem.getInstance(name).getFileName(name);
    }

    public long getLastModified(String fileName) {
        fileName = translateFileName(fileName);
        debug("getLastModified", fileName);
        return FileSystem.getInstance(fileName).getLastModified(fileName);
    }

    public String getParent(String fileName) {
        fileName = translateFileName(fileName);
        debug("getParent", fileName);
        return PREFIX + FileSystem.getInstance(fileName).getParent(fileName);
    }

    public boolean isAbsolute(String fileName) {
        fileName = translateFileName(fileName);
        debug("isAbsolute", fileName);
        return FileSystem.getInstance(fileName).isAbsolute(fileName);
    }

    public boolean isDirectory(String fileName) {
        fileName = translateFileName(fileName);
        debug("isDirectory", fileName);
        return FileSystem.getInstance(fileName).isDirectory(fileName);
    }

    public boolean isReadOnly(String fileName) {
        fileName = translateFileName(fileName);
        debug("isReadOnly", fileName);
        return FileSystem.getInstance(fileName).isReadOnly(fileName);
    }

    public long length(String fileName) {
        fileName = translateFileName(fileName);
        debug("length", fileName);
        return FileSystem.getInstance(fileName).length(fileName);
    }

    public String[] listFiles(String directory) throws SQLException {
        directory = translateFileName(directory);
        debug("listFiles", directory);
        String[] list = FileSystem.getInstance(directory).listFiles(directory);
        for (int i = 0; i < list.length; i++) {
            list[i] = PREFIX + list[i];
        }
        return list;
    }

    public String normalize(String fileName) throws SQLException {
        fileName = translateFileName(fileName);
        debug("normalize", fileName);
        return PREFIX + FileSystem.getInstance(fileName).normalize(fileName);
    }

    public InputStream openFileInputStream(String fileName) throws IOException {
        fileName = translateFileName(fileName);
        debug("openFileInputStream", fileName);
        return FileSystem.getInstance(fileName).openFileInputStream(fileName);
    }

    public FileObject openFileObject(String fileName, String mode) throws IOException {
        fileName = translateFileName(fileName);
        debug("openFileObject", fileName, mode);
        return new FileObjectDebug(this, FileSystem.getInstance(fileName).openFileObject(fileName, mode));
    }

    public OutputStream openFileOutputStream(String fileName, boolean append) throws SQLException {
        fileName = translateFileName(fileName);
        debug("openFileOutputStream", fileName, append);
        return FileSystem.getInstance(fileName).openFileOutputStream(fileName, append);
    }

    public void rename(String oldName, String newName) throws SQLException {
        oldName = translateFileName(oldName);
        newName = translateFileName(newName);
        debug("rename", oldName, newName);
        FileSystem.getInstance(oldName).copy(oldName, newName);
    }

    public boolean tryDelete(String fileName) {
        fileName = translateFileName(fileName);
        debug("tryDelete", fileName);
        return FileSystem.getInstance(fileName).tryDelete(fileName);
    }

    protected boolean accepts(String fileName) {
        return fileName.startsWith(PREFIX);
    }

    private String translateFileName(String fileName) {
        if (!fileName.startsWith(PREFIX)) {
            Message.throwInternalError(fileName + " doesn't start with " + PREFIX);
        }
        return fileName.substring(PREFIX.length());
    }

    /**
     * Print a debug message.
     *
     * @param method the method name
     * @param fileName the file name
     * @param params parameters if any
     */
    void debug(String method, String fileName, Object... params) {
        StringBuilder buff = new StringBuilder("    ");
        buff.append(fileName).append(' ').append(method);
        for (Object s : params) {
            buff.append(' ').append(s);
        }
        System.out.println(buff);
    }

}
