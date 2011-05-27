/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.h2.message.DbException;
import org.h2.store.fs.FileObject;
import org.h2.store.fs.FileObjectInputStream;
import org.h2.store.fs.FileObjectOutputStream;
import org.h2.store.fs.FileSystem;
import org.h2.store.fs.FileSystemWrapper;
import org.h2.util.IOUtils;

/**
 * A file system that encrypts the contents of the files.
 */
public class FileSystemCrypt extends FileSystemWrapper {

    /**
     * The prefix to use for this file system.
     */
    public static final String PREFIX = "crypt:";

    private static final FileSystemCrypt INSTANCE = new FileSystemCrypt();

    protected String getPrefix() {
        return PREFIX;
    }

    /**
     * Register the file system.
     *
     * @return the instance
     */
    public static FileSystemCrypt register() {
        FileSystem.register(INSTANCE);
        return INSTANCE;
    }

    public long length(String fileName) {
        long len = super.length(fileName);
        return Math.max(0, len - FileObjectCrypt.HEADER_LENGTH - FileObjectCrypt.BLOCK_SIZE);
    }

    public FileObject openFileObject(String fileName, String mode) throws IOException {
        String[] parsed = parse(fileName);
        FileObject file = IOUtils.openFileObject(parsed[2], mode);
        return new FileObjectCrypt(fileName, parsed[0], parsed[1], file);
    }

    public OutputStream openFileOutputStream(String fileName, boolean append) {
        try {
            FileObject file = openFileObject(fileName, "rw");
            return new FileObjectOutputStream(file, append);
        } catch (IOException e) {
            throw DbException.convertIOException(e, fileName);
        }
    }

    public InputStream openFileInputStream(String fileName) {
        try {
            FileObject file = openFileObject(fileName, "r");
            return new FileObjectInputStream(file);
        } catch (IOException e) {
            throw DbException.convertIOException(e, fileName);
        }
    }

    public String getParent(String fileName) {
        String[] parsed = parse(fileName);
        return combine(parsed[0], parsed[1], IOUtils.getParent(parsed[2]));
    }

    public String[] listFiles(String directory) {
        String[] parsed = parse(directory);
        String[] array = IOUtils.listFiles(parsed[2]);
        for (int i = 0; i < array.length; i++) {
            array[i] = combine(parsed[0], parsed[1], array[i]);
        }
        return array;
    }

    public String getCanonicalPath(String fileName) {
        String[] parsed = parse(fileName);
        return combine(parsed[0], parsed[1], IOUtils.getCanonicalPath(parsed[2]));
    }

    public String unwrap(String fileName) {
        return parse(fileName)[2];
    }

    public String createTempFile(String prefix, String suffix, boolean deleteOnExit, boolean inTempDir)
        throws IOException {
        String[] parsed = parse(prefix);
        return combine(parsed[0], parsed[1], IOUtils.createTempFile(parsed[2], suffix, deleteOnExit, inTempDir));
    }

    /**
     * Combine the parameters into a file name.
     *
     * @param algorithm the encryption algorithm
     * @param password the password
     * @param fileName the base file name
     * @return the combined file name
     */
    private String combine(String algorithm, String password, String fileName) {
        return PREFIX + algorithm + ":" + password + ":" + fileName;
    }

    /**
     * Split the file name into algorithm, password, and base file name.
     *
     * @param fileName the file name
     * @return an array with algorithm, password, and base file name
     */
    private String[] parse(String fileName) {
        if (!fileName.startsWith(PREFIX)) {
            DbException.throwInternalError(fileName + " doesn't start with " + PREFIX);
        }
        fileName = fileName.substring(PREFIX.length());
        int idx = fileName.indexOf(':');
        String algorithm, password;
        if (idx < 0) {
            DbException.throwInternalError(fileName + " doesn't contain encryption algorithm and password");
        }
        algorithm = fileName.substring(0, idx);
        fileName = fileName.substring(idx + 1);
        idx = fileName.indexOf(':');
        if (idx < 0) {
            DbException.throwInternalError(fileName + " doesn't contain encryption password");
        }
        password = fileName.substring(0, idx);
        fileName = fileName.substring(idx + 1);
        return new String[] { algorithm, password, fileName };
    }

}
