/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.h2.message.Message;
import org.h2.util.New;

/**
 * This is a read-only file system that allows
 * to access databases stored in a .zip or .jar file.
 */
public class FileSystemZip extends FileSystem {

    private static final FileSystemZip INSTANCE = new FileSystemZip();

    private FileSystemZip() {
        // don't allow construction
    }

    public static FileSystemZip getInstance() {
        return INSTANCE;
    }

    public boolean canWrite(String fileName) {
        return false;
    }

    public void copy(String original, String copy) throws SQLException {
        throw Message.getUnsupportedException("write");
    }

    public void createDirs(String fileName) {
        // ignore
    }

    public boolean createNewFile(String fileName) throws SQLException {
        throw Message.getUnsupportedException("write");
    }

    public String createTempFile(String prefix, String suffix, boolean deleteOnExit, boolean inTempDir) throws IOException {
        if (!inTempDir) {
            throw new IOException("File system is read-only");
        }
        return FileSystemDisk.getInstance().createTempFile(prefix, suffix, deleteOnExit, true);
    }

    public void delete(String fileName) throws SQLException {
        throw Message.getUnsupportedException("write");
    }

    public void deleteRecursive(String fileName, boolean tryOnly) throws SQLException {
        throw Message.getUnsupportedException("write");
    }

    public boolean exists(String fileName) {
        try {
            String entryName = getEntryName(fileName);
            if (entryName.length() == 0) {
                return true;
            }
            ZipFile file = openZipFile(fileName);
            return file.getEntry(entryName) != null;
        } catch (IOException e) {
            return false;
        }
    }

    public boolean fileStartsWith(String fileName, String prefix) {
        return fileName.startsWith(prefix);
    }

    public String getAbsolutePath(String fileName) {
        return fileName;
    }

    public String getFileName(String name) {
        name = getEntryName(name);
        if (name.endsWith("/")) {
            name = name.substring(0, name.length() - 1);
        }
        int idx = name.lastIndexOf('/');
        if (idx >= 0) {
            name = name.substring(idx + 1);
        }
        return name;
    }

    public long getLastModified(String fileName) {
        return 0;
    }

    public String getParent(String fileName) {
        int idx = fileName.lastIndexOf('/');
        if (idx > 0) {
            fileName = fileName.substring(0, idx);
        }
        return fileName;
    }

    public boolean isAbsolute(String fileName) {
        return true;
    }

    public boolean isDirectory(String fileName) {
        try {
            String entryName = getEntryName(fileName);
            if (entryName.length() == 0) {
                return true;
            }
            ZipFile file = openZipFile(fileName);
            Enumeration< ? extends ZipEntry> en = file.entries();
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

    public boolean isReadOnly(String fileName) {
        return true;
    }

    public long length(String fileName) {
        try {
            ZipFile file = openZipFile(fileName);
            ZipEntry entry = file.getEntry(getEntryName(fileName));
            return entry == null ? 0 : entry.getSize();
        } catch (IOException e) {
            return 0;
        }
    }

    public String[] listFiles(String path) throws SQLException {
        try {
            if (path.indexOf('!') < 0) {
                path += "!";
            }
            if (!path.endsWith("/")) {
                path += "/";
            }
            ZipFile file = openZipFile(path);
            String dirName = getEntryName(path);
            String prefix = path.substring(0, path.length() - dirName.length());
            Enumeration< ? extends ZipEntry> en = file.entries();
            ArrayList<String> list = New.arrayList();
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
                    list.add(prefix + name);
                }
            }
            String[] result = new String[list.size()];
            list.toArray(result);
            return result;
        } catch (IOException e) {
            throw Message.convertIOException(e, "listFiles " + path);
        }
    }

    public String normalize(String fileName) {
        return fileName;
    }

    public InputStream openFileInputStream(String fileName) throws IOException {
        FileObject file = openFileObject(fileName, "r");
        return new FileObjectInputStream(file);
    }

    public FileObject openFileObject(String fileName, String mode) throws IOException {
        ZipFile file = openZipFile(translateFileName(fileName));
        ZipEntry entry = file.getEntry(getEntryName(fileName));
        if (entry == null) {
            throw new FileNotFoundException(fileName);
        }
        return new FileObjectZip(file, entry);
    }

    public OutputStream openFileOutputStream(String fileName, boolean append) throws SQLException {
        throw Message.getUnsupportedException("write");
    }

    public void rename(String oldName, String newName) throws SQLException {
        throw Message.getUnsupportedException("write");
    }

    public boolean tryDelete(String fileName) {
        return false;
    }

    private String translateFileName(String fileName) {
        if (fileName.startsWith(FileSystem.PREFIX_ZIP)) {
            fileName = fileName.substring(FileSystem.PREFIX_ZIP.length());
        }
        int idx = fileName.indexOf('!');
        if (idx >= 0) {
            fileName = fileName.substring(0, idx);
        }
        return fileName;
    }

    private String getEntryName(String fileName) {
        int idx = fileName.indexOf('!');
        if (idx <= 0) {
            fileName = "";
        } else {
            fileName = fileName.substring(idx + 1);
        }
        fileName = fileName.replace('\\', '/');
        if (fileName.startsWith("/")) {
            fileName = fileName.substring(1);
        }
        return fileName;
    }

    private ZipFile openZipFile(String fileName) throws IOException {
        fileName = translateFileName(fileName);
        return new ZipFile(fileName);
    }
}
