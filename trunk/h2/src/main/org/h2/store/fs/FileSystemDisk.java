/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URL;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.message.DbException;
import org.h2.util.IOUtils;
import org.h2.util.StringUtils;

/**
 * This file system stores files on disk.
 * This is the most common file system.
 */
public class FileSystemDisk extends FileSystem {

    private static final FileSystemDisk INSTANCE = new FileSystemDisk();
    // TODO detection of 'case in sensitive filesystem'
    // could maybe implemented using some other means
    private static final boolean IS_FILE_SYSTEM_CASE_INSENSITIVE = File.separatorChar == '\\';

    private static final String CLASSPATH_PREFIX = "classpath:";

    protected FileSystemDisk() {
        // nothing to do
    }

    public static FileSystemDisk getInstance() {
        return INSTANCE;
    }

    public long length(String fileName) {
        fileName = translateFileName(fileName);
        return new File(fileName).length();
    }

    /**
     * Translate the file name to the native format.
     * This will expand the home directory (~).
     *
     * @param fileName the file name
     * @return the native file name
     */
    protected static String translateFileName(String fileName) {
        return expandUserHomeDirectory(fileName);
    }

    /**
     * Expand '~' to the user home directory. It is only be expanded if the ~
     * stands alone, or is followed by / or \.
     *
     * @param fileName the file name
     * @return the native file name
     */
    public static String expandUserHomeDirectory(String fileName) {
        if (fileName == null) {
            return null;
        }
        if (fileName.startsWith("~") && (fileName.length() == 1 || fileName.startsWith("~/") || fileName.startsWith("~\\"))) {
            String userDir = SysProperties.USER_HOME;
            fileName = userDir + fileName.substring(1);
        }
        return fileName;
    }

    public void rename(String oldName, String newName) {
        oldName = translateFileName(oldName);
        newName = translateFileName(newName);
        File oldFile = new File(oldName);
        File newFile = new File(newName);
        if (oldFile.getAbsolutePath().equals(newFile.getAbsolutePath())) {
            DbException.throwInternalError("rename file old=new");
        }
        if (!oldFile.exists()) {
            throw DbException.get(ErrorCode.FILE_RENAME_FAILED_2,
                    oldName + " (not found)",
                    newName);
        }
        if (newFile.exists()) {
            throw DbException.get(ErrorCode.FILE_RENAME_FAILED_2,
                    new String[] { oldName, newName + " (exists)" });
        }
        for (int i = 0; i < SysProperties.MAX_FILE_RETRY; i++) {
            IOUtils.trace("rename", oldName + " >" + newName, null);
            boolean ok = oldFile.renameTo(newFile);
            if (ok) {
                return;
            }
            wait(i);
        }
        throw DbException.get(ErrorCode.FILE_RENAME_FAILED_2, new String[]{oldName, newName});
    }

    private static void wait(int i) {
        if (i > 8) {
            System.gc();
        }
        try {
            // sleep at most 256 ms
            long sleep = Math.min(256, i * i);
            Thread.sleep(sleep);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    public boolean createNewFile(String fileName) {
        fileName = translateFileName(fileName);
        File file = new File(fileName);
        for (int i = 0; i < SysProperties.MAX_FILE_RETRY; i++) {
            try {
                return file.createNewFile();
            } catch (IOException e) {
                // 'access denied' is really a concurrent access problem
                wait(i);
            }
        }
        return false;
    }

    public boolean exists(String fileName) {
        fileName = translateFileName(fileName);
        return new File(fileName).exists();
    }

    public void delete(String fileName) {
        fileName = translateFileName(fileName);
        File file = new File(fileName);
        if (file.exists()) {
            for (int i = 0; i < SysProperties.MAX_FILE_RETRY; i++) {
                IOUtils.trace("delete", fileName, null);
                boolean ok = file.delete();
                if (ok) {
                    return;
                }
                wait(i);
            }
            throw DbException.get(ErrorCode.FILE_DELETE_FAILED_1, fileName);
        }
    }

    public boolean tryDelete(String fileName) {
        fileName = translateFileName(fileName);
        IOUtils.trace("tryDelete", fileName, null);
        return new File(fileName).delete();
    }

    public String createTempFile(String name, String suffix, boolean deleteOnExit, boolean inTempDir)
            throws IOException {
        name = translateFileName(name);
        name += ".";
        String prefix = new File(name).getName();
        File dir;
        if (inTempDir) {
            dir = new File(System.getProperty("java.io.tmpdir"));
        } else {
            dir = new File(name).getAbsoluteFile().getParentFile();
            IOUtils.mkdirs(dir);
        }
        File f;
        while (true) {
            f = new File(dir, prefix + getNextTempFileNamePart(false) + suffix);
            if (f.exists()) {
                // in theory, the random number could collide
                getNextTempFileNamePart(true);
            } else {
                break;
            }
        }
        if (deleteOnExit) {
            try {
                f.deleteOnExit();
            } catch (Throwable e) {
                // sometimes this throws a NullPointerException
                // at java.io.DeleteOnExitHook.add(DeleteOnExitHook.java:33)
                // we can ignore it
            }
        }
        return f.getCanonicalPath();
    }

    public String[] listFiles(String path) {
        path = translateFileName(path);
        File f = new File(path);
        try {
            String[] list = f.list();
            if (list == null) {
                return new String[0];
            }
            String base = f.getCanonicalPath();
            if (!base.endsWith(SysProperties.FILE_SEPARATOR)) {
                base += SysProperties.FILE_SEPARATOR;
            }
            for (int i = 0, len = list.length; i < len; i++) {
                list[i] = base + list[i];
            }
            return list;
        } catch (IOException e) {
            throw DbException.convertIOException(e, path);
        }
    }

    public void deleteRecursive(String fileName, boolean tryOnly) {
        fileName = translateFileName(fileName);
        if (IOUtils.isDirectory(fileName)) {
            String[] list = listFiles(fileName);
            if (list != null) {
                for (String l : list) {
                    deleteRecursive(l, tryOnly);
                }
            }
        }
        if (tryOnly) {
            tryDelete(fileName);
        } else {
            delete(fileName);
        }
    }

    public boolean isReadOnly(String fileName) {
        fileName = translateFileName(fileName);
        File f = new File(fileName);
        return f.exists() && !canWriteInternal(f);
    }

    public boolean setReadOnly(String fileName) {
        fileName = translateFileName(fileName);
        File f = new File(fileName);
        return f.setReadOnly();
    }

    public String getCanonicalPath(String fileName) {
        fileName = translateFileName(fileName);
        File f = new File(fileName);
        try {
            return f.getCanonicalPath();
        } catch (IOException e) {
            throw DbException.convertIOException(e, fileName);
        }
    }

    public String getParent(String fileName) {
        fileName = translateFileName(fileName);
        return new File(fileName).getParent();
    }

    public boolean isDirectory(String fileName) {
        fileName = translateFileName(fileName);
        return new File(fileName).isDirectory();
    }

    public boolean isAbsolute(String fileName) {
        fileName = translateFileName(fileName);
        File file = new File(fileName);
        return file.isAbsolute();
    }

    public long getLastModified(String fileName) {
        fileName = translateFileName(fileName);
        return new File(fileName).lastModified();
    }

    public boolean canWrite(String fileName) {
        fileName = translateFileName(fileName);
        return canWriteInternal(new File(fileName));
    }

    private static boolean canWriteInternal(File file) {
        try {
            if (!file.canWrite()) {
                return false;
            }
        } catch (Exception e) {
            // workaround for GAE which throws a
            // java.security.AccessControlException
            return false;
        }
        // File.canWrite() does not respect windows user permissions,
        // so we must try to open it using the mode "rw".
        // See also http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4420020
        RandomAccessFile r = null;
        try {
            r = new RandomAccessFile(file, "rw");
            return true;
        } catch (FileNotFoundException e) {
            return false;
        } finally {
            if (r != null) {
                try {
                    r.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    public void createDirs(String fileName) {
        fileName = translateFileName(fileName);
        File f = new File(fileName);
        if (!f.exists()) {
            String parent = f.getParent();
            if (parent == null) {
                return;
            }
            File dir = new File(parent);
            for (int i = 0; i < SysProperties.MAX_FILE_RETRY; i++) {
                if ((dir.exists() && dir.isDirectory()) || dir.mkdirs()) {
                    return;
                }
                wait(i);
            }
            throw DbException.get(ErrorCode.FILE_CREATION_FAILED_1, parent);
        }
    }

    public String getFileName(String name) {
        name = translateFileName(name);
        return new File(name).getName();
    }

    public boolean fileStartsWith(String fileName, String prefix) {
        fileName = translateFileName(fileName);
        prefix = translateFileName(prefix);
        if (IS_FILE_SYSTEM_CASE_INSENSITIVE) {
            fileName = StringUtils.toUpperEnglish(fileName);
            prefix = StringUtils.toUpperEnglish(prefix);
        }
        return fileName.startsWith(prefix);
    }

    public OutputStream openFileOutputStream(String fileName, boolean append) {
        fileName = translateFileName(fileName);
        try {
            File file = new File(fileName);
            createDirs(file.getAbsolutePath());
            FileOutputStream out = new FileOutputStream(fileName, append);
            IOUtils.trace("openFileOutputStream", fileName, out);
            return out;
        } catch (IOException e) {
            freeMemoryAndFinalize();
            try {
                return new FileOutputStream(fileName);
            } catch (IOException e2) {
                throw DbException.convertIOException(e, fileName);
            }
        }
    }

    public InputStream openFileInputStream(String fileName) throws IOException {
        if (fileName.indexOf(':') > 1) {
            // if the : is in position 1, a windows file access is assumed: C:.. or D:
            if (fileName.startsWith(CLASSPATH_PREFIX)) {
                fileName = fileName.substring(CLASSPATH_PREFIX.length());
                InputStream in = getClass().getClassLoader().getResourceAsStream(fileName);
                if (in == null) {
                    throw new FileNotFoundException("resource " + fileName);
                }
                return in;
            }
            // otherwise an URL is assumed
            URL url = new URL(fileName);
            InputStream in = url.openStream();
            return in;
        }
        fileName = translateFileName(fileName);
        FileInputStream in = new FileInputStream(fileName);
        IOUtils.trace("openFileInputStream", fileName, in);
        return in;
    }

    /**
     * Call the garbage collection and run finalization. This close all files that
     * were not closed, and are no longer referenced.
     */
    static void freeMemoryAndFinalize() {
        IOUtils.trace("freeMemoryAndFinalize", null, null);
        Runtime rt = Runtime.getRuntime();
        long mem = rt.freeMemory();
        for (int i = 0; i < 16; i++) {
            rt.gc();
            long now = rt.freeMemory();
            rt.runFinalization();
            if (now == mem) {
                break;
            }
            mem = now;
        }
    }

    public FileObject openFileObject(String fileName, String mode) throws IOException {
        fileName = translateFileName(fileName);
        FileObjectDisk f;
        try {
            f = new FileObjectDisk(fileName, mode);
            IOUtils.trace("openFileObject", fileName, f);
        } catch (IOException e) {
            freeMemoryAndFinalize();
            try {
                f = new FileObjectDisk(fileName, mode);
            } catch (IOException e2) {
                throw e;
            }
        }
        return f;
    }

    protected boolean accepts(String fileName) {
        return true;
    }

    public String unwrap(String fileName) {
        return fileName;
    }

}
