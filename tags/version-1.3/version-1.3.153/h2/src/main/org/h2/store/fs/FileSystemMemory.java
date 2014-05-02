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
import java.util.Iterator;
import java.util.TreeMap;
import org.h2.message.DbException;
import org.h2.util.IOUtils;
import org.h2.util.New;

/**
 * This file system keeps files fully in memory.
 * There is an option to compress file blocks to safe memory.
 */
public class FileSystemMemory extends FileSystem {

    /**
     * The prefix used for an in-memory file system.
     */
    public static final String PREFIX = "memFS:";

    /**
     * The prefix used for a compressed in-memory file system.
     */
    public static final String PREFIX_LZF = "memLZF:";

    private static final FileSystemMemory INSTANCE = new FileSystemMemory();
    private static final TreeMap<String, FileObjectMemoryData> MEMORY_FILES = new TreeMap<String, FileObjectMemoryData>();

    private FileSystemMemory() {
        // don't allow construction
    }

    public static FileSystemMemory getInstance() {
        return INSTANCE;
    }

    public long length(String fileName) {
        return getMemoryFile(fileName).length();
    }

    public void rename(String oldName, String newName) {
        oldName = getCanonicalPath(oldName);
        newName = getCanonicalPath(newName);
        synchronized (MEMORY_FILES) {
            FileObjectMemoryData f = getMemoryFile(oldName);
            f.setName(newName);
            MEMORY_FILES.remove(oldName);
            MEMORY_FILES.put(newName, f);
        }
    }

    public boolean createNewFile(String fileName) {
        synchronized (MEMORY_FILES) {
            if (exists(fileName)) {
                return false;
            }
            getMemoryFile(fileName);
        }
        return true;
    }

    public boolean exists(String fileName) {
        fileName = getCanonicalPath(fileName);
        synchronized (MEMORY_FILES) {
            return MEMORY_FILES.get(fileName) != null;
        }
    }

    public void delete(String fileName) {
        fileName = getCanonicalPath(fileName);
        synchronized (MEMORY_FILES) {
            MEMORY_FILES.remove(fileName);
        }
    }

    public boolean tryDelete(String fileName) {
        delete(fileName);
        return true;
    }

    public String[] listFiles(String path) {
        ArrayList<String> list = New.arrayList();
        synchronized (MEMORY_FILES) {
            for (String name : MEMORY_FILES.tailMap(path).keySet()) {
                if (name.startsWith(path)) {
                    list.add(name);
                } else {
                    break;
                }
            }
            String[] array = new String[list.size()];
            list.toArray(array);
            return array;
        }
    }

    public void deleteRecursive(String fileName, boolean tryOnly) {
        fileName = getCanonicalPath(fileName);
        synchronized (MEMORY_FILES) {
            Iterator<String> it = MEMORY_FILES.tailMap(fileName).keySet().iterator();
            while (it.hasNext()) {
                String name = it.next();
                if (name.startsWith(fileName)) {
                    it.remove();
                } else {
                    break;
                }
            }
        }
    }

    public boolean isReadOnly(String fileName) {
        return !getMemoryFile(fileName).canWrite();
    }

    public boolean setReadOnly(String fileName) {
        return getMemoryFile(fileName).setReadOnly();
    }

    public String getCanonicalPath(String fileName) {
        fileName = fileName.replace('\\', '/');
        int idx = fileName.indexOf(':') + 1;
        if (fileName.length() > idx && fileName.charAt(idx) != '/') {
            fileName = fileName.substring(0, idx) + "/" + fileName.substring(idx);
        }
        return fileName;
    }

    public String getParent(String fileName) {
        fileName = getCanonicalPath(fileName);
        int idx = fileName.lastIndexOf('/');
        if (idx < 0) {
            idx = fileName.indexOf(':') + 1;
        }
        return fileName.substring(0, idx);
    }

    public boolean isDirectory(String fileName) {
        // TODO in memory file system currently
        // does not really support directories
        return false;
    }

    public boolean isAbsolute(String fileName) {
        // TODO relative files are not supported
        return true;
    }

    public long getLastModified(String fileName) {
        return getMemoryFile(fileName).getLastModified();
    }

    public boolean canWrite(String fileName) {
        return true;
    }

    public void copy(String source, String target) {
        try {
            OutputStream out = openFileOutputStream(target, false);
            InputStream in = openFileInputStream(source);
            IOUtils.copyAndClose(in, out);
        } catch (IOException e) {
            throw DbException.convertIOException(e, "Can not copy " + source + " to " + target);
        }
    }

    public void createDirs(String fileName) {
        // TODO directories are not really supported
    }

    public String getFileName(String name) {
        int idx = Math.max(name.indexOf(':'), name.lastIndexOf('/'));
        return idx < 0 ? name : name.substring(idx + 1);
    }

    public boolean fileStartsWith(String fileName, String prefix) {
        fileName = getCanonicalPath(fileName);
        prefix = getCanonicalPath(prefix);
        return fileName.startsWith(prefix);
    }

    public OutputStream openFileOutputStream(String fileName, boolean append) {
        try {
            FileObjectMemoryData obj = getMemoryFile(fileName);
            FileObjectMemory m = new FileObjectMemory(obj, false);
            return new FileObjectOutputStream(m, append);
        } catch (IOException e) {
            throw DbException.convertIOException(e, fileName);
        }
    }

    public InputStream openFileInputStream(String fileName) {
        FileObjectMemoryData obj = getMemoryFile(fileName);
        FileObjectMemory m = new FileObjectMemory(obj, true);
        return new FileObjectInputStream(m);
    }

    public FileObject openFileObject(String fileName, String mode) {
        FileObjectMemoryData obj = getMemoryFile(fileName);
        return new FileObjectMemory(obj, "r".equals(mode));
    }

    private FileObjectMemoryData getMemoryFile(String fileName) {
        fileName = getCanonicalPath(fileName);
        synchronized (MEMORY_FILES) {
            FileObjectMemoryData m = MEMORY_FILES.get(fileName);
            if (m == null) {
                boolean compress = fileName.startsWith(PREFIX_LZF);
                m = new FileObjectMemoryData(fileName, compress);
                MEMORY_FILES.put(fileName, m);
            }
            return m;
        }
    }

    protected boolean accepts(String fileName) {
        return fileName.startsWith(PREFIX) || fileName.startsWith(PREFIX_LZF);
    }

    public String unwrap(String fileName) {
        return fileName;
    }

}
