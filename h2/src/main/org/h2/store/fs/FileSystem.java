/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;


public abstract class FileSystem {
    
    public static final String MEMORY_PREFIX = "memFS:";
    public static final String MEMORY_PREFIX_LZF = "memLZF:";
    public static final String DB_PREFIX = "jdbc:";
    public static final String ZIP_PREFIX = "zip:";
    
    public static FileSystem getInstance(String fileName) {
        if (isInMemory(fileName)) {
            return FileSystemMemory.getInstance();
        } else if (fileName.startsWith(DB_PREFIX)) {
            return FileSystemDatabase.getInstance(fileName);
        } else if (fileName.startsWith(ZIP_PREFIX)) {
            return FileSystemZip.getInstance();
        }
        return FileSystemDisk.getInstance();
    }
    
    private static boolean isInMemory(String fileName) {
        return fileName != null && (fileName.startsWith(MEMORY_PREFIX) || fileName.startsWith(MEMORY_PREFIX_LZF));
    }    
  
    public abstract long length(String fileName);
    
    public abstract void rename(String oldName, String newName) throws SQLException;
    
    public abstract boolean createNewFile(String fileName) throws SQLException;

    public abstract boolean exists(String fileName);

    public abstract void delete(String fileName) throws SQLException;
    
    public abstract boolean tryDelete(String fileName);
    
    public abstract String createTempFile(String prefix, String suffix, boolean deleteOnExit, boolean inTempDir) throws IOException;

    public abstract String[] listFiles(String path) throws SQLException;
    
    public abstract void deleteRecursive(String fileName) throws SQLException;

    public abstract boolean isReadOnly(String fileName);

    public abstract String normalize(String fileName) throws SQLException;

    public abstract String getParent(String fileName);

    public abstract boolean isDirectory(String fileName);

    public abstract boolean isAbsolute(String fileName);
    
    public abstract String getAbsolutePath(String fileName);

    public abstract long getLastModified(String fileName);

    public abstract boolean canWrite(String fileName);

    public abstract void copy(String original, String copy) throws SQLException;
    
    public void mkdirs(String directoryName) throws SQLException {
        createDirs(directoryName + "/x");
    }
    
    public abstract void createDirs(String fileName) throws SQLException;
    
    public abstract String getFileName(String name) throws SQLException;

    public abstract boolean fileStartsWith(String fileName, String prefix);
    
    public abstract OutputStream openFileOutputStream(String fileName, boolean append) throws SQLException;
    
    public abstract FileObject openFileObject(String fileName, String mode) throws IOException;
    
    public abstract InputStream openFileInputStream(String fileName) throws IOException;
}
