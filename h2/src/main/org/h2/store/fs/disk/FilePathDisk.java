/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs.disk;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.DosFileAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.h2.api.ErrorCode;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FileUtils;
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
            String path = this.name.substring("classpath:".length());
            if (!path.startsWith("/")) {
                path = "/" + path;
            }
            URL url = this.getClass().getResource(path);
            if (url == null) {
                return 0L;
            }
            try {
                URI uri = url.toURI();
                if ("file".equals(url.getProtocol())) {
                    return Files.size(Paths.get(uri));
                }
                try {
                    // If filesystem is opened, let it be closed by the code that opened it.
                    // This way subsequent access to the FS does not fail
                    FileSystems.getFileSystem(uri);
                    return Files.size(Paths.get(uri));
                } catch (FileSystemNotFoundException e) {
                    Map<String, String> env = new HashMap<>();
                    env.put("create", "true");
                    // If filesystem was not opened, open it and close it after access to avoid resource leak.
                    try (FileSystem fs = FileSystems.newFileSystem(uri, env)) {
                        return Files.size(Paths.get(uri));
                    }
                }
            } catch (Exception ex) {
                return 0L;
            }
        }
        try {
            return Files.size(Paths.get(name));
        } catch (IOException e) {
            return 0L;
        }
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
        Path file = Paths.get(name);
        for (int i = 0; i < SysProperties.MAX_FILE_RETRY; i++) {
            try {
                Files.createFile(file);
                return true;
            } catch (FileAlreadyExistsException e) {
                return false;
            } catch (IOException e) {
                // 'access denied' is really a concurrent access problem
                wait(i);
            }
        }
        return false;
    }

    @Override
    public boolean exists() {
        return Files.exists(Paths.get(name));
    }

    @Override
    public void delete() {
        Path file = Paths.get(name);
        IOException cause = null;
        for (int i = 0; i < SysProperties.MAX_FILE_RETRY; i++) {
            IOUtils.trace("delete", name, null);
            try {
                Files.deleteIfExists(file);
                return;
            } catch (DirectoryNotEmptyException e) {
                throw DbException.get(ErrorCode.FILE_DELETE_FAILED_1, e, name);
            } catch (AccessDeniedException e) {
                // On Windows file systems, delete a readonly file can cause AccessDeniedException,
                // we should change readonly attribute to false and then delete file
                try {
                    FileStore fileStore = Files.getFileStore(file);
                    if (!fileStore.supportsFileAttributeView(PosixFileAttributeView.class)
                        && fileStore.supportsFileAttributeView(DosFileAttributeView.class)) {
                        Files.setAttribute(file, "dos:readonly", false);
                        Files.delete(file);
                    }
                } catch (IOException ioe) {
                    cause = ioe;
                }
            } catch (IOException e) {
                cause = e;
            }
            wait(i);
        }
        throw DbException.get(ErrorCode.FILE_DELETE_FAILED_1, cause, name);
    }

    @Override
    public List<FilePath> newDirectoryStream() {
        try (Stream<Path> files = Files.list(toRealPath(Paths.get(name)))) {
            return files.collect(ArrayList::new, (t, u) -> t.add(getPath(u.toString())), ArrayList::addAll);
        } catch (NoSuchFileException e) {
            return Collections.emptyList();
        } catch (IOException e) {
            throw DbException.convertIOException(e, name);
        }
    }

    @Override
    public boolean canWrite() {
        try {
            return Files.isWritable(Paths.get(name));
        } catch (Exception e) {
            // Catch security exceptions
            return false;
        }
    }

    @Override
    public boolean setReadOnly() {
        Path f = Paths.get(name);
        try {
            FileStore fileStore = Files.getFileStore(f);
            /*
             * Need to check PosixFileAttributeView first because
             * DosFileAttributeView is also supported by recent Java versions on
             * non-Windows file systems, but it doesn't affect real access
             * permissions.
             */
            if (fileStore.supportsFileAttributeView(PosixFileAttributeView.class)) {
                HashSet<PosixFilePermission> permissions = new HashSet<>();
                for (PosixFilePermission p : Files.getPosixFilePermissions(f)) {
                    switch (p) {
                    case OWNER_WRITE:
                    case GROUP_WRITE:
                    case OTHERS_WRITE:
                        break;
                    default:
                        permissions.add(p);
                    }
                }
                Files.setPosixFilePermissions(f, permissions);
            } else if (fileStore.supportsFileAttributeView(DosFileAttributeView.class)) {
                Files.setAttribute(f, "dos:readonly", true);
            } else {
                return false;
            }
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public FilePathDisk toRealPath() {
        return getPath(toRealPath(Paths.get(name)).toString());
    }

    private static Path toRealPath(Path path) {
        try {
            path = path.toRealPath();
        } catch (IOException e) {
            /*
             * File does not exist or isn't accessible, try to get the real path
             * of parent directory.
             *
             * toRealPath() can also throw AccessDeniedException on accessible
             * remote directory on Windows if other directories on remote drive
             * aren't accessible, but toAbsolutePath() should work.
             */
            path = parentToRealPath(path.toAbsolutePath().normalize());
        }
        return path;
    }

    private static Path parentToRealPath(Path path) {
        Path parent = path.getParent();
        if (parent == null) {
            return path;
        }
        try {
            parent = parent.toRealPath();
        } catch (IOException e) {
            parent = parentToRealPath(parent);
        }
        return parent.resolve(path.getFileName());
    }

    @Override
    public FilePath getParent() {
        Path p = Paths.get(name).getParent();
        return p == null ? null : getPath(p.toString());
    }

    @Override
    public boolean isDirectory() {
        return Files.isDirectory(Paths.get(name));
    }

    @Override
    public boolean isRegularFile() {
        return Files.isRegularFile(Paths.get(name));
    }

    @Override
    public boolean isAbsolute() {
        return Paths.get(name).isAbsolute();
    }

    @Override
    public long lastModified() {
        try {
            return Files.getLastModifiedTime(Paths.get(name)).toMillis();
        } catch (IOException e) {
            return 0L;
        }
    }

    @Override
    public void createDirectory() {
        Path dir = Paths.get(name);
        try {
            Files.createDirectory(dir);
        } catch (FileAlreadyExistsException e) {
            throw DbException.get(ErrorCode.FILE_CREATION_FAILED_1, name + " (a file with this name already exists)");
        } catch (IOException e) {
            IOException cause = e;
            for (int i = 0; i < SysProperties.MAX_FILE_RETRY; i++) {
                if (Files.isDirectory(dir)) {
                    return;
                }
                try {
                    Files.createDirectory(dir);
                } catch (FileAlreadyExistsException ex) {
                    throw DbException.get(ErrorCode.FILE_CREATION_FAILED_1,
                            name + " (a file with this name already exists)");
                } catch (IOException ex) {
                    cause = ex;
                }
                wait(i);
            }
            throw DbException.get(ErrorCode.FILE_CREATION_FAILED_1, cause, name);
        }
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
        Path file = Paths.get(name + '.').toAbsolutePath();
        String prefix = file.getFileName().toString();
        if (inTempDir) {
            final Path tempDir = Paths.get(System.getProperty("java.io.tmpdir", "."));
            if (!Files.isDirectory(tempDir)) {
                Files.createDirectories(tempDir);
            }
            file = Files.createTempFile(prefix, suffix);
        } else {
            Path dir = file.getParent();
            Files.createDirectories(dir);
            file = Files.createTempFile(dir, prefix, suffix);
        }
        return get(file.toString());
    }

}
