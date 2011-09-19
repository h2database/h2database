/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.util.List;
import org.h2.message.DbException;

/**
 * The base class for wrapping / delegating file systems such as
 * the split file system.
 */
public abstract class FilePathWrapper extends FilePath {

    private FilePath base;

    public FilePathWrapper getPath(String path) {
        return create(path, unwrap(path));
    }

    /**
     * Create a wrapped path instance for the given base path.
     *
     * @param base the base path
     * @return the wrapped path
     */
    public FilePathWrapper wrap(FilePath base) {
        return base == null ? null : create(getPrefix() + base.name, base);
    }

    public FilePath unwrap() {
        return unwrap(name);
    }

    private FilePathWrapper create(String path, FilePath base) {
        try {
            FilePathWrapper p = getClass().newInstance();
            p.name = path;
            p.base = base;
            return p;
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    protected String getPrefix() {
        return getScheme() + ":";
    }

    /**
     * Get the base path for the given wrapped path.
     *
     * @param path the path including the scheme prefix
     * @return the base file path
     */
    protected FilePath unwrap(String path) {
        return FilePath.get(path.substring(getScheme().length() + 1));
    }

    protected FilePath getBase() {
        return base;
    }

    public boolean canWrite() {
        return base.canWrite();
    }

    public void createDirectory() {
        base.createDirectory();
    }

    public boolean createFile() {
        return base.createFile();
    }

    public void delete() {
        base.delete();
    }

    public boolean exists() {
        return base.exists();
    }

    public FilePath getParent() {
        return wrap(base.getParent());
    }

    public boolean isAbsolute() {
        return base.isAbsolute();
    }

    public boolean isDirectory() {
        return base.isDirectory();
    }

    public long lastModified() {
        return base.lastModified();
    }

    public FilePath toRealPath() {
        return wrap(base.toRealPath());
    }

    public List<FilePath> newDirectoryStream() {
        List<FilePath> list = base.newDirectoryStream();
        for (int i = 0, len = list.size(); i < len; i++) {
            list.set(i, wrap(list.get(i)));
        }
        return list;
    }

    public void moveTo(FilePath newName) {
        base.moveTo(((FilePathWrapper) newName).base);
    }

    public InputStream newInputStream() throws IOException {
        return base.newInputStream();
    }

    public OutputStream newOutputStream(boolean append) {
        return base.newOutputStream(append);
    }

    public FileChannel open(String mode) throws IOException {
        return base.open(mode);
    }

    public boolean setReadOnly() {
        return base.setReadOnly();
    }

    public long size() {
        return base.size();
    }

    public FilePath createTempFile(String suffix, boolean deleteOnExit, boolean inTempDir)
            throws IOException {
        return wrap(base.createTempFile(suffix, deleteOnExit, inTempDir));
    }

}
