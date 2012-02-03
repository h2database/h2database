/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.sql.SQLException;
import java.util.ArrayList;

import org.h2.constant.SysProperties;
import org.h2.message.Message;
import org.h2.util.New;

/**
 * A file system that may split files into multiple smaller files.
 */
public class FileSystemSplit extends FileSystem {

    private static final String PART_SUFFIX = ".part";
    private static final FileSystemSplit INSTANCE = new FileSystemSplit();
    private long defaultMaxSize = 1L << SysProperties.SPLIT_FILE_SIZE_SHIFT;

    public static FileSystemSplit getInstance() {
        return INSTANCE;
    }

    public boolean canWrite(String fileName) {
        fileName = translateFileName(fileName);
        return getFileSystem(fileName).canWrite(fileName);
    }

    public void copy(String original, String copy) throws SQLException {
        original = translateFileName(original);
        copy = translateFileName(copy);
        getFileSystem(original).copy(original, copy);
        for (int i = 1;; i++) {
            String o = getFileName(original, i);
            if (getFileSystem(o).exists(o)) {
                String c = getFileName(copy, i);
                getFileSystem(o).copy(o, c);
            } else {
                break;
            }
        }
    }

    public void createDirs(String fileName) throws SQLException {
        fileName = translateFileName(fileName);
        getFileSystem(fileName).createDirs(fileName);
    }

    public boolean createNewFile(String fileName) throws SQLException {
        fileName = translateFileName(fileName);
        return getFileSystem(fileName).createNewFile(fileName);
    }

    public String createTempFile(String prefix, String suffix, boolean deleteOnExit, boolean inTempDir)
            throws IOException {
        prefix = translateFileName(prefix);
        return FileSystem.PREFIX_SPLIT + getFileSystem(prefix).createTempFile(prefix, suffix, deleteOnExit, inTempDir);
    }

    public void delete(String fileName) throws SQLException {
        fileName = translateFileName(fileName);
        for (int i = 0;; i++) {
            String f = getFileName(fileName, i);
            if (getFileSystem(fileName).exists(f)) {
                getFileSystem(fileName).delete(f);
            } else {
                break;
            }
        }
    }

    public void deleteRecursive(String directory, boolean tryOnly) throws SQLException {
        directory = translateFileName(directory);
        getFileSystem(directory).deleteRecursive(directory, tryOnly);
    }

    public boolean exists(String fileName) {
        fileName = translateFileName(fileName);
        return getFileSystem(fileName).exists(fileName);
    }

    public boolean fileStartsWith(String fileName, String prefix) {
        fileName = translateFileName(fileName);
        prefix = translateFileName(prefix);
        return getFileSystem(fileName).fileStartsWith(fileName, prefix);
    }

    public String getAbsolutePath(String fileName) {
        fileName = translateFileName(fileName);
        return FileSystem.PREFIX_SPLIT + getFileSystem(fileName).getAbsolutePath(fileName);
    }

    public String getFileName(String name) throws SQLException {
        name = translateFileName(name);
        return getFileSystem(name).getFileName(name);
    }

    public long getLastModified(String fileName) {
        fileName = translateFileName(fileName);
        long lastModified = 0;
        for (int i = 0;; i++) {
            String f = getFileName(fileName, i);
            if (getFileSystem(fileName).exists(f)) {
                long l = getFileSystem(fileName).getLastModified(fileName);
                lastModified = Math.max(lastModified, l);
            } else {
                break;
            }
        }
        return lastModified;
    }

    public String getParent(String fileName) {
        fileName = translateFileName(fileName);
        return FileSystem.PREFIX_SPLIT + getFileSystem(fileName).getParent(fileName);
    }

    public boolean isAbsolute(String fileName) {
        fileName = translateFileName(fileName);
        return getFileSystem(fileName).isAbsolute(fileName);
    }

    public boolean isDirectory(String fileName) {
        fileName = translateFileName(fileName);
        return getFileSystem(fileName).isDirectory(fileName);
    }

    public boolean isReadOnly(String fileName) {
        fileName = translateFileName(fileName);
        return getFileSystem(fileName).isReadOnly(fileName);
    }

    public long length(String fileName) {
        fileName = translateFileName(fileName);
        long length = 0;
        for (int i = 0;; i++) {
            String f = getFileName(fileName, i);
            if (getFileSystem(fileName).exists(f)) {
                length += getFileSystem(fileName).length(f);
            } else {
                break;
            }
        }
        return length;
    }

    public String[] listFiles(String directory) throws SQLException {
        directory = translateFileName(directory);
        String[] array = getFileSystem(directory).listFiles(directory);
        ArrayList<String> list = New.arrayList();
        for (int i = 0; i < array.length; i++) {
            String f = array[i];
            if (f.endsWith(PART_SUFFIX)) {
                continue;
            }
            array[i] = f = FileSystem.PREFIX_SPLIT + f;
            list.add(f);
        }
        if (list.size() != array.length) {
            array = new String[list.size()];
            list.toArray(array);
        }
        return array;
    }

    public String normalize(String fileName) throws SQLException {
        fileName = translateFileName(fileName);
        return FileSystem.PREFIX_SPLIT + getFileSystem(fileName).normalize(fileName);
    }

    public InputStream openFileInputStream(String fileName) throws IOException {
        fileName = translateFileName(fileName);
        InputStream input = getFileSystem(fileName).openFileInputStream(fileName);
        for (int i = 1;; i++) {
            String f = getFileName(fileName, i);
            if (getFileSystem(f).exists(f)) {
                InputStream i2 = getFileSystem(f).openFileInputStream(f);
                input = new SequenceInputStream(input, i2);
            } else {
                break;
            }
        }
        return input;
    }

    public FileObject openFileObject(String fileName, String mode) throws IOException {
        fileName = translateFileName(fileName);
        ArrayList<FileObject> list = New.arrayList();
        FileObject o = getFileSystem(fileName).openFileObject(fileName, mode);
        list.add(o);
        for (int i = 1;; i++) {
            String f = getFileName(fileName, i);
            if (getFileSystem(fileName).exists(f)) {
                o = getFileSystem(f).openFileObject(f, mode);
                list.add(o);
            } else {
                break;
            }
        }
        FileObject[] array = new FileObject[list.size()];
        list.toArray(array);
        long maxLength = array[0].length();
        long length = maxLength;
        if (array.length == 1) {
            if (maxLength < defaultMaxSize) {
                maxLength = defaultMaxSize;
            }
        } else {
            for (int i = 1; i < array.length - 1; i++) {
                o = array[i];
                long l = o.length();
                length += l;
                if (l != maxLength) {
                    throw new IOException("Expected file length: " + maxLength + " got: " + l + " for " + o.getName());
                }
            }
            o = array[array.length - 1];
            long l = o.length();
            length += l;
            if (l > maxLength) {
                throw new IOException("Expected file length: " + maxLength + " got: " + l + " for " + o.getName());
            }
        }
        FileObjectSplit fo = new FileObjectSplit(fileName, mode, array, length, maxLength);
        return fo;
    }

    public OutputStream openFileOutputStream(String fileName, boolean append) throws SQLException {
        fileName = translateFileName(fileName);
        // TODO the output stream is not split
        return getFileSystem(fileName).openFileOutputStream(fileName, append);
    }

    public void rename(String oldName, String newName) throws SQLException {
        oldName = translateFileName(oldName);
        newName = translateFileName(newName);
        for (int i = 0;; i++) {
            String o = getFileName(oldName, i);
            if (getFileSystem(o).exists(o)) {
                String n = getFileName(newName, i);
                getFileSystem(n).rename(o, n);
            } else {
                break;
            }
        }
    }

    public boolean tryDelete(String fileName) {
        fileName = translateFileName(fileName);
        for (int i = 0;; i++) {
            String f = getFileName(fileName, i);
            if (getFileSystem(fileName).exists(f)) {
                boolean ok = getFileSystem(fileName).tryDelete(f);
                if (!ok) {
                    return false;
                }
            } else {
                break;
            }
        }
        return true;
    }

    private String translateFileName(String fileName) {
        if (!fileName.startsWith(FileSystem.PREFIX_SPLIT)) {
            Message.throwInternalError(fileName + " doesn't start with " + FileSystem.PREFIX_SPLIT);
        }
        fileName = fileName.substring(FileSystem.PREFIX_SPLIT.length());
        if (fileName.length() > 0 && Character.isDigit(fileName.charAt(0))) {
            int idx = fileName.indexOf(':');
            String size = fileName.substring(0, idx);
            try {
                defaultMaxSize = 1L << Integer.decode(size).intValue();
                fileName = fileName.substring(idx + 1);
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        return fileName;
    }

    /**
     * Get the file name of a part file.
     *
     * @param fileName the file name
     * @param id the part id
     * @return the file name including the part id
     */
    static String getFileName(String fileName, int id) {
        if (id > 0) {
            fileName += "." + id + PART_SUFFIX;
        }
        return fileName;
    }

    private FileSystem getFileSystem(String fileName) {
        return FileSystem.getInstance(fileName);
    }

}
