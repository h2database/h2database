/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import org.h2.util.New;

/**
 * The file system is a storage abstraction.
 */
public abstract class FileSystem {

    /**
     * The prefix used for an in-memory file system.
     */
    public static final String PREFIX_MEMORY = "memFS:";

    /**
     * The prefix used for a compressed in-memory file system.
     */
    public static final String PREFIX_MEMORY_LZF = "memLZF:";

    /**
     * The prefix used for a read-only zip-file based file system.
     */
    public static final String PREFIX_ZIP = "zip:";

    /**
     * The prefix used to split large files (required for a FAT32 because it
     * only support files up to 2 GB).
     */
    public static final String PREFIX_SPLIT = "split:";

    /**
     * The prefix used for the NIO FileChannel file system.
     */
    public static final String PREFIX_NIO = "nio:";

    /**
     * The prefix used for the NIO (memory mapped) file system.
     */
    public static final String PREFIX_NIO_MAPPED = "nioMapped:";

    private static final ArrayList<FileSystem> SERVICES = New.arrayList();

    /**
     * Get the file system object.
     *
     * @param fileName the file name or prefix
     * @return the file system
     */
    public static FileSystem getInstance(String fileName) {
        if (isInMemory(fileName)) {
            return FileSystemMemory.getInstance();
        } else if (fileName.startsWith(PREFIX_ZIP)) {
            return FileSystemZip.getInstance();
        } else if (fileName.startsWith(PREFIX_SPLIT)) {
            return FileSystemSplit.getInstance();
        } else if (fileName.startsWith(PREFIX_NIO)) {
            return FileSystemDiskNio.getInstance();
        } else if (fileName.startsWith(PREFIX_NIO_MAPPED)) {
            return FileSystemDiskNioMapped.getInstance();
        }
        for (FileSystem fs : SERVICES) {
            if (fs.accepts(fileName)) {
                return fs;
            }
        }
        return FileSystemDisk.getInstance();
    }

    /**
     * Register a file system.
     *
     * @param service the file system
     */
    public static synchronized void register(FileSystem service) {
        SERVICES.add(service);
    }

    /**
     * Unregister a file system.
     *
     * @param service the file system
     */
    public static synchronized void unregister(FileSystem service) {
        SERVICES.remove(service);
    }

    /**
     * Check if the file system is responsible for this file name.
     *
     * @param fileName the file name
     * @return true if it is
     */
    protected boolean accepts(String fileName) {
        return false;
    }

    private static boolean isInMemory(String fileName) {
        return fileName != null && (fileName.startsWith(PREFIX_MEMORY) || fileName.startsWith(PREFIX_MEMORY_LZF));
    }

    /**
     * Get the length of a file.
     *
     * @param fileName the file name
     * @return the length in bytes
     */
    public abstract long length(String fileName);

    /**
     * Rename a file if this is allowed.
     *
     * @param oldName the old fully qualified file name
     * @param newName the new fully qualified file name
     * @throws SQLException
     */
    public abstract void rename(String oldName, String newName) throws SQLException;

    /**
     * Create a new file.
     *
     * @param fileName the file name
     * @return true if creating was successful
     */
    public abstract boolean createNewFile(String fileName) throws SQLException;

    /**
     * Checks if a file exists.
     *
     * @param fileName the file name
     * @return true if it exists
     */
    public abstract boolean exists(String fileName);

    /**
     * Delete a file.
     *
     * @param fileName the file name
     */
    public abstract void delete(String fileName) throws SQLException;

    /**
     * Try to delete a file.
     *
     * @param fileName the file name
     * @return true if it could be deleted
     */
    public abstract boolean tryDelete(String fileName);

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
    public abstract String createTempFile(String prefix, String suffix, boolean deleteOnExit, boolean inTempDir) throws IOException;

    /**
     * List the files in the given directory.
     *
     * @param directory the directory
     * @return the list of fully qualified file names
     */
    public abstract String[] listFiles(String directory) throws SQLException;

    /**
     * Delete a directory or file and all subdirectories and files.
     *
     * @param directory the directory
     * @param tryOnly whether errors should  be ignored
     */
    public abstract void deleteRecursive(String directory, boolean tryOnly) throws SQLException;

    /**
     * Check if a file is read-only.
     *
     * @param fileName the file name
     * @return if it is read only
     */
    public abstract boolean isReadOnly(String fileName);

    /**
     * Normalize a file name.
     *
     * @param fileName the file name
     * @return the normalized file name
     */
    public abstract String normalize(String fileName) throws SQLException;

    /**
     * Get the parent directory of a file or directory.
     *
     * @param fileName the file or directory name
     * @return the parent directory name
     */
    public abstract String getParent(String fileName);

    /**
     * Check if it is a file or a directory.
     *
     * @param fileName the file or directory name
     * @return true if it is a directory
     */
    public abstract boolean isDirectory(String fileName);

    /**
     * Check if the file name includes a path.
     *
     * @param fileName the file name
     * @return if the file name is absolute
     */
    public abstract boolean isAbsolute(String fileName);

    /**
     * Get the absolute file name.
     *
     * @param fileName the file name
     * @return the absolute file name
     */
    public abstract String getAbsolutePath(String fileName);

    /**
     * Get the last modified date of a file
     *
     * @param fileName the file name
     * @return the last modified date
     */
    public abstract long getLastModified(String fileName);

    /**
     * Check if the file is writable.
     *
     * @param fileName the file name
     * @return if the file is writable
     */
    public abstract boolean canWrite(String fileName);

    /**
     * Copy a file from one directory to another, or to another file.
     *
     * @param original the original file name
     * @param copy the file name of the copy
     */
    public abstract void copy(String original, String copy) throws SQLException;

    /**
     * Create all required directories.
     *
     * @param directoryName the directory name
     */
    public void mkdirs(String directoryName) throws SQLException {
        createDirs(directoryName + "/x");
    }

    /**
     * Create all required directories that are required for this file.
     *
     * @param fileName the file name (not directory name)
     */
    public abstract void createDirs(String fileName) throws SQLException;

    /**
     * Get the file name (without directory part).
     *
     * @param name the directory and file name
     * @return just the file name
     */
    public abstract String getFileName(String name) throws SQLException;

    /**
     * Check if a file starts with a given prefix.
     *
     * @param fileName the complete file name
     * @param prefix the prefix
     * @return true if it starts with the prefix
     */
    public abstract boolean fileStartsWith(String fileName, String prefix);

    /**
     * Create an output stream to write into the file.
     *
     * @param fileName the file name
     * @param append if true, the file will grow, if false, the file will be
     *            truncated first
     * @return the output stream
     */
    public abstract OutputStream openFileOutputStream(String fileName, boolean append) throws SQLException;

    /**
     * Open a random access file object.
     *
     * @param fileName the file name
     * @param mode the access mode. Supported are r, rw, rws, rwd
     * @return the file object
     */
    public abstract FileObject openFileObject(String fileName, String mode) throws IOException;

    /**
     * Create an input stream to read from the file.
     *
     * @param fileName the file name
     * @return the input stream
     */
    public abstract InputStream openFileInputStream(String fileName) throws IOException;

}
