/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.CopyOption;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import org.h2.api.ErrorCode;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.util.IOUtils;

/**
 * This file system stores files on disk.
 * This is the most common file system.
 */
public class FilePathDisk extends FilePath {

    private static final String CLASSPATH_PREFIX = "classpath:";

    @Override
    public FilePathDisk getPath(String path) {
        FilePathDisk p = new FilePathDisk();
        p.name = translateFileName(path);
        return p;
    }

    @Override
    public long size() {
        if (name.startsWith(CLASSPATH_PREFIX)) {
            try {
                String fileName = name.substring(CLASSPATH_PREFIX.length());
                // Force absolute resolution in Class.getResource
                if (!fileName.startsWith("/")) {
                    fileName = "/" + fileName;
                }
                URL resource = this.getClass().getResource(fileName);
                if (resource != null) {
                    return Files.size(Paths.get(resource.toURI()));
                } else {
                    return 0;
                }
            } catch (Exception e) {
                return 0;
            }
        }
        return new File(name).length();
    }

    /**
     * Translate the file name to the native format. This will replace '\' with
     * '/' and expand the home directory ('~').
     *
     * @param fileName the file name
     * @return the native file name
     */
    protected static String translateFileName(String fileName) {
        fileName = fileName.replace('\\', '/');
        if (fileName.startsWith("file:")) {
            fileName = fileName.substring(5);
        } else if (fileName.startsWith("nio:")) {
            fileName = fileName.substring(4);
        }
        return expandUserHomeDirectory(fileName);
    }

    /**
     * Expand '~' to the user home directory. It is only be expanded if the '~'
     * stands alone, or is followed by '/' or '\'.
     *
     * @param fileName the file name
     * @return the native file name
     */
    public static String expandUserHomeDirectory(String fileName) {
        if (fileName.startsWith("~") && (fileName.length() == 1 ||
                fileName.startsWith("~/"))) {
            String userDir = SysProperties.USER_HOME;
            fileName = userDir + fileName.substring(1);
        }
        return fileName;
    }

    @Override
    public void moveTo(FilePath newName, boolean atomicReplace) {
        Path oldFile = Paths.get(name);
        Path newFile = Paths.get(newName.name);
        if (!Files.exists(oldFile)) {
            throw DbException.get(ErrorCode.FILE_RENAME_FAILED_2, name + " (not found)", newName.name);
        }
        if (atomicReplace) {
            try {
                Files.move(oldFile, newFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                return;
            } catch (AtomicMoveNotSupportedException ex) {
                // Ignore
            } catch (IOException ex) {
                throw DbException.get(ErrorCode.FILE_RENAME_FAILED_2, ex, name, newName.name);
            }
        }
        CopyOption[] copyOptions = atomicReplace ? new CopyOption[] { StandardCopyOption.REPLACE_EXISTING }
                : new CopyOption[0];
        IOException cause;
        try {
            Files.move(oldFile, newFile, copyOptions);
        } catch (FileAlreadyExistsException ex) {
            throw DbException.get(ErrorCode.FILE_RENAME_FAILED_2, name, newName + " (exists)");
        } catch (IOException ex) {
            cause = ex;
            for (int i = 0; i < SysProperties.MAX_FILE_RETRY; i++) {
                IOUtils.trace("rename", name + " >" + newName, null);
                try {
                    Files.move(oldFile, newFile, copyOptions);
                    return;
                } catch (FileAlreadyExistsException ex2) {
                    throw DbException.get(ErrorCode.FILE_RENAME_FAILED_2, name, newName + " (exists)");
                } catch (IOException ex2) {
                    cause = ex;
                }
                wait(i);
            }
            throw DbException.get(ErrorCode.FILE_RENAME_FAILED_2, cause, name, newName.name);
        }
    }

    private static void wait(int i) {
        if (i == 8) {
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

    @Override
    public boolean createFile() {
        File file = new File(name);
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

    @Override
    public boolean exists() {
        return new File(name).exists();
    }

    @Override
    public void delete() {
        File file = new File(name);
        for (int i = 0; i < SysProperties.MAX_FILE_RETRY; i++) {
            IOUtils.trace("delete", name, null);
            boolean ok = file.delete();
            if (ok || !file.exists()) {
                return;
            }
            wait(i);
        }
        throw DbException.get(ErrorCode.FILE_DELETE_FAILED_1, name);
    }

    @Override
    public List<FilePath> newDirectoryStream() {
        ArrayList<FilePath> list = new ArrayList<>();
        File f = new File(name);
        try {
            String[] files = f.list();
            if (files != null) {
                String base = f.getCanonicalPath();
                if (!base.endsWith(SysProperties.FILE_SEPARATOR)) {
                    base += SysProperties.FILE_SEPARATOR;
                }
                list.ensureCapacity(files.length);
                for (String file : files) {
                    list.add(getPath(base + file));
                }
            }
            return list;
        } catch (IOException e) {
            throw DbException.convertIOException(e, name);
        }
    }

    @Override
    public boolean canWrite() {
        return canWriteInternal(new File(name));
    }

    @Override
    public boolean setReadOnly() {
        File f = new File(name);
        return f.setReadOnly();
    }

    @Override
    public FilePathDisk toRealPath() {
        try {
            String fileName = new File(name).getCanonicalPath();
            return getPath(fileName);
        } catch (IOException e) {
            throw DbException.convertIOException(e, name);
        }
    }

    @Override
    public FilePath getParent() {
        String p = new File(name).getParent();
        return p == null ? null : getPath(p);
    }

    @Override
    public boolean isDirectory() {
        return new File(name).isDirectory();
    }

    @Override
    public boolean isAbsolute() {
        return new File(name).isAbsolute();
    }

    @Override
    public long lastModified() {
        return new File(name).lastModified();
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
        // See also https://bugs.java.com/bugdatabase/view_bug.do?bug_id=4420020
        try (FileChannel f = FileChannel.open(file.toPath(), FileUtils.RW, FileUtils.NO_ATTRIBUTES)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public void createDirectory() {
        File dir = new File(name);
        for (int i = 0; i < SysProperties.MAX_FILE_RETRY; i++) {
            if (dir.exists()) {
                if (dir.isDirectory()) {
                    return;
                }
                throw DbException.get(ErrorCode.FILE_CREATION_FAILED_1,
                        name + " (a file with this name already exists)");
            } else if (dir.mkdir()) {
                return;
            }
            wait(i);
        }
        throw DbException.get(ErrorCode.FILE_CREATION_FAILED_1, name);
    }

    @Override
    public OutputStream newOutputStream(boolean append) throws IOException {
        Path file = Paths.get(name);
        OpenOption[] options = append //
                ? new OpenOption[] { StandardOpenOption.CREATE, StandardOpenOption.APPEND }
                : new OpenOption[0];
        try {
            Path parent = file.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            OutputStream out = Files.newOutputStream(file, options);
            IOUtils.trace("openFileOutputStream", name, out);
            return out;
        } catch (IOException e) {
            freeMemoryAndFinalize();
            return Files.newOutputStream(file, options);
        }
    }

    @Override
    public InputStream newInputStream() throws IOException {
        if (name.matches("[a-zA-Z]{2,19}:.*")) {
            // if the ':' is in position 1, a windows file access is assumed:
            // C:.. or D:, and if the ':' is not at the beginning, assume its a
            // file name with a colon
            if (name.startsWith(CLASSPATH_PREFIX)) {
                String fileName = name.substring(CLASSPATH_PREFIX.length());
                // Force absolute resolution in Class.getResourceAsStream
                if (!fileName.startsWith("/")) {
                    fileName = "/" + fileName;
                }
                InputStream in = getClass().getResourceAsStream(fileName);
                if (in == null) {
                    // ClassLoader.getResourceAsStream doesn't need leading "/"
                    in = Thread.currentThread().getContextClassLoader().
                            getResourceAsStream(fileName.substring(1));
                }
                if (in == null) {
                    throw new FileNotFoundException("resource " + fileName);
                }
                return in;
            }
            // otherwise a URL is assumed
            URL url = new URL(name);
            return url.openStream();
        }
        InputStream in = Files.newInputStream(Paths.get(name));
        IOUtils.trace("openFileInputStream", name, in);
        return in;
    }

    /**
     * Call the garbage collection and run finalization. This close all files
     * that were not closed, and are no longer referenced.
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

    @Override
    public FileChannel open(String mode) throws IOException {
        FileChannel f = FileChannel.open(Paths.get(name), FileUtils.modeToOptions(mode), FileUtils.NO_ATTRIBUTES);
        IOUtils.trace("open", name, f);
        return f;
    }

    @Override
    public String getScheme() {
        return "file";
    }

    @Override
    public FilePath createTempFile(String suffix, boolean inTempDir) throws IOException {
        String fileName = name + ".";
        String prefix = new File(fileName).getName();
        File dir;
        if (inTempDir) {
            dir = new File(System.getProperty("java.io.tmpdir", "."));
        } else {
            dir = new File(fileName).getAbsoluteFile().getParentFile();
        }
        FileUtils.createDirectories(dir.getAbsolutePath());
        while (true) {
            File f = new File(dir, prefix + getNextTempFileNamePart(false) + suffix);
            if (f.exists() || !f.createNewFile()) {
                // in theory, the random number could collide
                getNextTempFileNamePart(true);
                continue;
            }
            return get(f.getCanonicalPath());
        }
    }

}
