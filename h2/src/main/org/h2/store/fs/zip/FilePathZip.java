/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs.zip;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.h2.message.DbException;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.disk.FilePathDisk;

/**
 * This is a read-only file system that allows
 * to access databases stored in a .zip or .jar file.
 */
public class FilePathZip extends FilePath {

    @Override
    public FilePathZip getPath(String path) {
        FilePathZip p = new FilePathZip();
        p.name = path;
        return p;
    }

    @Override
    public void createDirectory() {
        // ignore
    }

    @Override
    public boolean createFile() {
        throw DbException.getUnsupportedException("write");
    }

    @Override
    public void delete() {
        throw DbException.getUnsupportedException("write");
    }

    @Override
    public boolean exists() {
        try {
            String entryName = getEntryName();
            if (entryName.isEmpty()) {
                return true;
            }
            try (ZipFile file = openZipFile()) {
                return file.getEntry(entryName) != null;
            }
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public long lastModified() {
        return 0;
    }

    @Override
    public FilePath getParent() {
        int idx = name.lastIndexOf('/');
        return idx < 0 ? null : getPath(name.substring(0, idx));
    }

    @Override
    public boolean isAbsolute() {
        String fileName = translateFileName(name);
        return FilePath.get(fileName).isAbsolute();
    }

    @Override
    public FilePath unwrap() {
        return FilePath.get(name.substring(getScheme().length() + 1));
    }

    @Override
    public boolean isDirectory() {
        return isRegularOrDirectory(true);
    }

    @Override
    public boolean isRegularFile() {
        return isRegularOrDirectory(false);
    }

    private boolean isRegularOrDirectory(boolean directory) {
        try {
            String entryName = getEntryName();
            if (entryName.isEmpty()) {
                return directory;
            }
            try (ZipFile file = openZipFile()) {
                Enumeration<? extends ZipEntry> en = file.entries();
                while (en.hasMoreElements()) {
                    ZipEntry entry = en.nextElement();
                    String n = entry.getName();
                    if (n.equals(entryName)) {
                        return entry.isDirectory() == directory;
                    } else  if (n.startsWith(entryName)) {
                        if (n.length() == entryName.length() + 1) {
                            if (n.equals(entryName + "/")) {
                                return directory;
                            }
                        }
                    }
                }
            }
            return false;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public boolean canWrite() {
        return false;
    }

    @Override
    public boolean setReadOnly() {
        return true;
    }

    @Override
    public long size() {
        try {
            try (ZipFile file = openZipFile()) {
                ZipEntry entry = file.getEntry(getEntryName());
                return entry == null ? 0 : entry.getSize();
            }
        } catch (IOException e) {
            return 0;
        }
    }

    @Override
    public ArrayList<FilePath> newDirectoryStream() {
        String path = name;
        ArrayList<FilePath> list = new ArrayList<>();
        try {
            if (path.indexOf('!') < 0) {
                path += "!";
            }
            if (!path.endsWith("/")) {
                path += "/";
            }
            try (ZipFile file = openZipFile()) {
                String dirName = getEntryName();
                String prefix = path.substring(0, path.length() - dirName.length());
                Enumeration<? extends ZipEntry> en = file.entries();
                while (en.hasMoreElements()) {
                    ZipEntry entry = en.nextElement();
                    String name = entry.getName();
                    if (!name.startsWith(dirName)) {
                        continue;
                    }
                    if (name.length() <= dirName.length()) {
                        continue;
                    }
                    int idx = name.indexOf('/', dirName.length());
                    if (idx < 0 || idx >= name.length() - 1) {
                        list.add(getPath(prefix + name));
                    }
                }
            }
            return list;
        } catch (IOException e) {
            throw DbException.convertIOException(e, "listFiles " + path);
        }
    }

    @Override
    public FileChannel open(String mode) throws IOException {
        ZipFile file = openZipFile();
        ZipEntry entry = file.getEntry(getEntryName());
        if (entry == null) {
            file.close();
            throw new FileNotFoundException(name);
        }
        return new FileZip(file, entry);
    }

    @Override
    public OutputStream newOutputStream(boolean append) throws IOException {
        throw new IOException("write");
    }

    @Override
    public void moveTo(FilePath newName, boolean atomicReplace) {
        throw DbException.getUnsupportedException("write");
    }

    private static String translateFileName(String fileName) {
        if (fileName.startsWith("zip:")) {
            fileName = fileName.substring("zip:".length());
        }
        int idx = fileName.indexOf('!');
        if (idx >= 0) {
            fileName = fileName.substring(0, idx);
        }
        return FilePathDisk.expandUserHomeDirectory(fileName);
    }

    @Override
    public FilePath toRealPath() {
        return this;
    }

    private String getEntryName() {
        int idx = name.indexOf('!');
        String fileName;
        if (idx <= 0) {
            fileName = "";
        } else {
            fileName = name.substring(idx + 1);
        }
        fileName = fileName.replace('\\', '/');
        if (fileName.startsWith("/")) {
            fileName = fileName.substring(1);
        }
        return fileName;
    }

    private ZipFile openZipFile() throws IOException {
        String fileName = translateFileName(name);
        return new ZipFile(fileName);
    }

    @Override
    public FilePath createTempFile(String suffix, boolean inTempDir) throws IOException {
        if (!inTempDir) {
            throw new IOException("File system is read-only");
        }
        return new FilePathDisk().getPath(name).createTempFile(suffix, true);
    }

    @Override
    public String getScheme() {
        return "zip";
    }

}
