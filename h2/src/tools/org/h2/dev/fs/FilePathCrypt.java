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
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FilePathWrapper;
import org.h2.store.fs.FileUtils;

/**
 * A file system that encrypts the contents of the files.
 */
public class FilePathCrypt extends FilePathWrapper {

    /**
     * Register this file system.
     */
    public static void register() {
        FilePath.register(new FilePathCrypt());
    }

    protected String getPrefix() {
        String[] parsed = parse(name);
        return getScheme() + ":" + parsed[0] + ":" + parsed[1] + ":";
    }

    public FilePath unwrap(String fileName) {
        return FilePath.get(parse(fileName)[2]);
    }

    public long size() {
        long len = getBase().size();
        return Math.max(0, len - FileObjectCrypt.HEADER_LENGTH - FileObjectCrypt.BLOCK_SIZE);
    }

    public FileObject openFileObject(String mode) throws IOException {
        String[] parsed = parse(name);
        FileObject file = FileUtils.openFileObject(parsed[2], mode);
        return new FileObjectCrypt(name, parsed[0], parsed[1], file);
    }

    public OutputStream newOutputStream(boolean append) {
        try {
            FileObject file = openFileObject("rw");
            return new FileObjectOutputStream(file, append);
        } catch (IOException e) {
            throw DbException.convertIOException(e, name);
        }
    }

    public InputStream newInputStream() {
        try {
            FileObject file = openFileObject("r");
            return new FileObjectInputStream(file);
        } catch (IOException e) {
            throw DbException.convertIOException(e, name);
        }
    }

    /**
     * Split the file name into algorithm, password, and base file name.
     *
     * @param fileName the file name
     * @return an array with algorithm, password, and base file name
     */
    private String[] parse(String fileName) {
        if (!fileName.startsWith(getScheme())) {
            DbException.throwInternalError(fileName + " doesn't start with " + getScheme());
        }
        fileName = fileName.substring(getScheme().length() + 1);
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

    public String getScheme() {
        return "crypt";
    }

}
