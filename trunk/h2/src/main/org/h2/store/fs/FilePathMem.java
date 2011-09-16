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
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import org.h2.message.DbException;
import org.h2.util.New;

/**
 * This file system keeps files fully in memory. There is an option to compress
 * file blocks to safe memory.
 */
public class FilePathMem extends FilePath {

    private static final TreeMap<String, FileObjectMemoryData> MEMORY_FILES = new TreeMap<String, FileObjectMemoryData>();

    public FilePathMem getPath(String path) {
        FilePathMem p = new FilePathMem();
        p.name = getCanonicalPath(path);
        return p;
    }

    public long size() {
        return getMemoryFile().length();
    }

    public void moveTo(FilePath newName) {
        synchronized (MEMORY_FILES) {
            FileObjectMemoryData f = getMemoryFile();
            f.setName(newName.name);
            MEMORY_FILES.remove(name);
            MEMORY_FILES.put(newName.name, f);
        }
    }

    public boolean createFile() {
        synchronized (MEMORY_FILES) {
            if (exists()) {
                return false;
            }
            getMemoryFile();
        }
        return true;
    }

    public boolean exists() {
        if (isRoot()) {
            return true;
        }
        synchronized (MEMORY_FILES) {
            return MEMORY_FILES.get(name) != null;
        }
    }

    public void delete() {
        if (isRoot()) {
            return;
        }
        synchronized (MEMORY_FILES) {
            MEMORY_FILES.remove(name);
        }
    }

    public List<FilePath> listFiles() {
        ArrayList<FilePath> list = New.arrayList();
        synchronized (MEMORY_FILES) {
            for (String n : MEMORY_FILES.tailMap(name).keySet()) {
                if (n.startsWith(name)) {
                    list.add(getPath(n));
                } else {
                    break;
                }
            }
            return list;
        }
    }

    public boolean setReadOnly() {
        return getMemoryFile().setReadOnly();
    }

    public boolean canWrite() {
        return getMemoryFile().canWrite();
    }

    public FilePathMem getParent() {
        int idx = name.lastIndexOf('/');
        return idx < 0 ? null : getPath(name.substring(0, idx));
    }

    public boolean isDirectory() {
        // TODO in memory file system currently
        // does not really support directories
        return false;
    }

    public boolean isAbsolute() {
        // TODO relative files are not supported
        return true;
    }

    public FilePathMem getCanonicalPath() {
        return this;
    }

    public long lastModified() {
        return getMemoryFile().getLastModified();
    }

    public void createDirectory() {
        // TODO directories are not really supported
    }

    public boolean fileStartsWith(String prefix) {
        prefix = getCanonicalPath(prefix);
        return name.startsWith(prefix);
    }

    public OutputStream newOutputStream(boolean append) {
        try {
            FileObjectMemoryData obj = getMemoryFile();
            FileObjectMemory m = new FileObjectMemory(obj, false);
            return new FileObjectOutputStream(m, append);
        } catch (IOException e) {
            throw DbException.convertIOException(e, name);
        }
    }

    public InputStream newInputStream() {
        FileObjectMemoryData obj = getMemoryFile();
        FileObjectMemory m = new FileObjectMemory(obj, true);
        return new FileObjectInputStream(m);
    }

    public FileObject openFileObject(String mode) {
        FileObjectMemoryData obj = getMemoryFile();
        return new FileObjectMemory(obj, "r".equals(mode));
    }

    private FileObjectMemoryData getMemoryFile() {
        synchronized (MEMORY_FILES) {
            FileObjectMemoryData m = MEMORY_FILES.get(name);
            if (m == null) {
                m = new FileObjectMemoryData(name, compressed());
                MEMORY_FILES.put(name, m);
            }
            return m;
        }
    }

    private boolean isRoot() {
        return name.equals(getScheme());
    }

    protected boolean accepts(String fileName) {
        return fileName.startsWith(getScheme());
    }

    public String unwrap(String fileName) {
        return fileName;
    }

    private static String getCanonicalPath(String fileName) {
        fileName = fileName.replace('\\', '/');
        int idx = fileName.indexOf(':') + 1;
        if (fileName.length() > idx && fileName.charAt(idx) != '/') {
            fileName = fileName.substring(0, idx) + "/" + fileName.substring(idx);
        }
        return fileName;
    }

    public String getScheme() {
        return "memFS";
    }

    boolean compressed() {
        return false;
    }

}

/**
 * A memory file system that compresses blocks to conserve memory.
 */
class FilePathMemLZF extends FilePathMem {

    boolean compressed() {
        return true;
    }

    public String getScheme() {
        return "memLZF";
    }

}

