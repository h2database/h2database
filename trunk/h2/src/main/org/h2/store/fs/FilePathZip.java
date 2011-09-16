/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.h2.message.DbException;
import org.h2.util.New;

/**
 * This is a read-only file system that allows
 * to access databases stored in a .zip or .jar file.
 */
public class FilePathZip extends FilePath {

    public FilePathZip getPath(String path) {
        FilePathZip p = new FilePathZip();
        p.name = path;
        return p;
    }

    public void createDirectory() {
        // ignore
    }

    public boolean createFile() {
        throw DbException.getUnsupportedException("write");
    }

    public String createTempFile(String prefix, String suffix, boolean deleteOnExit, boolean inTempDir) throws IOException {
        if (!inTempDir) {
            throw new IOException("File system is read-only");
        }
        return FileSystemDisk.getInstance().createTempFile(prefix, suffix, deleteOnExit, true);
    }

    public void delete() {
        throw DbException.getUnsupportedException("write");
    }

    public boolean exists() {
        try {
            String entryName = getEntryName();
            if (entryName.length() == 0) {
                return true;
            }
            ZipFile file = openZipFile();
            return file.getEntry(entryName) != null;
        } catch (IOException e) {
            return false;
        }
    }

    public boolean fileStartsWith(String fileName, String prefix) {
        return fileName.startsWith(prefix);
    }

//    public String _getName(String name) {
//        name = getEntryName(name);
//        if (name.endsWith("/")) {
//            name = name.substring(0, name.length() - 1);
//        }
//        int idx = name.lastIndexOf('/');
//        if (idx >= 0) {
//            name = name.substring(idx + 1);
//        }
//        return name;
//    }

    public long lastModified() {
        return 0;
    }

    public FilePath getParent() {
        int idx = name.lastIndexOf('/');
        return idx < 0 ? null : getPath(name.substring(0, idx));
    }

    public boolean isAbsolute() {
        return true;
    }

    public boolean isDirectory() {
        try {
            String entryName = getEntryName();
            if (entryName.length() == 0) {
                return true;
            }
            ZipFile file = openZipFile();
            Enumeration<? extends ZipEntry> en = file.entries();
            while (en.hasMoreElements()) {
                ZipEntry entry = en.nextElement();
                String n = entry.getName();
                if (n.equals(entryName)) {
                    return entry.isDirectory();
                } else  if (n.startsWith(entryName)) {
                    if (n.length() == entryName.length() + 1) {
                        if (n.equals(entryName + "/")) {
                            return true;
                        }
                    }
                }
            }
            return false;
        } catch (IOException e) {
            return false;
        }
    }

    public boolean canWrite() {
        return false;
    }

    public boolean setReadOnly() {
        return true;
    }

    public long size() {
        try {
            ZipFile file = openZipFile();
            ZipEntry entry = file.getEntry(getEntryName());
            return entry == null ? 0 : entry.getSize();
        } catch (IOException e) {
            return 0;
        }
    }

    public ArrayList<FilePath> listFiles() {
        String path = name;
        ArrayList<FilePath> list = New.arrayList();
        try {
            if (path.indexOf('!') < 0) {
                path += "!";
            }
            if (!path.endsWith("/")) {
                path += "/";
            }
            ZipFile file = openZipFile();
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
            return list;
        } catch (IOException e) {
            throw DbException.convertIOException(e, "listFiles " + path);
        }
    }

    public InputStream newInputStream() throws IOException {
        return new FileObjectInputStream(openFileObject("r"));
    }

    public FileObject openFileObject(String mode) throws IOException {
        ZipFile file = openZipFile();
        ZipEntry entry = file.getEntry(getEntryName());
        if (entry == null) {
            throw new FileNotFoundException(name);
        }
        return new FileObjectZip(file, entry);
    }

    public OutputStream newOutputStream(boolean append) {
        throw DbException.getUnsupportedException("write");
    }

    public void moveTo(FilePath newName) {
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
        return FileSystemDisk.expandUserHomeDirectory(fileName);
    }

    public FilePath getCanonicalPath() {
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

    public FilePath createTempFile(String suffix, boolean deleteOnExit, boolean inTempDir) throws IOException {
        if (!inTempDir) {
            throw new IOException("File system is read-only");
        }
        return new FilePathDisk().getPath(name).createTempFile(suffix, deleteOnExit, true);
    }

//    protected boolean accepts(String fileName) {
//        return fileName.startsWith(PREFIX);
//    }

    public boolean fileStartsWith(String prefix) {
        return name.startsWith(prefix);
    }

    public String getScheme() {
        return "zip";
    }

}
