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
import java.io.SequenceInputStream;
import java.util.ArrayList;

import org.h2.constant.SysProperties;
import org.h2.message.DbException;
import org.h2.util.New;

/**
 * A file system that may split files into multiple smaller files.
 * (required for a FAT32 because it only support files up to 2 GB).
 */
public class FileSystemSplit extends FileSystemWrapper {

    /**
     * The prefix to use for this file system.
     */
    public static final String PREFIX = "split:";

    private static final String PART_SUFFIX = ".part";

    private long defaultMaxSize = 1L << SysProperties.SPLIT_FILE_SIZE_SHIFT;

    static {
        FileSystem.register(new FileSystemSplit());
    }

    public boolean setReadOnly(String fileName) {
        fileName = unwrap(fileName);
        boolean result = false;
        for (int i = 0;; i++) {
            String f = getFileName(fileName, i);
            if (getInstance(f).exists(f)) {
                result = getInstance(f).setReadOnly(f);
            } else {
                break;
            }
        }
        return result;
    }

    public void delete(String fileName) {
        fileName = unwrap(fileName);
        for (int i = 0;; i++) {
            String f = getFileName(fileName, i);
            if (getInstance(fileName).exists(f)) {
                getInstance(fileName).delete(f);
            } else {
                break;
            }
        }
    }

    public long lastModified(String fileName) {
        fileName = unwrap(fileName);
        long lastModified = 0;
        for (int i = 0;; i++) {
            String f = getFileName(fileName, i);
            if (getInstance(fileName).exists(f)) {
                long l = getInstance(fileName).lastModified(fileName);
                lastModified = Math.max(lastModified, l);
            } else {
                break;
            }
        }
        return lastModified;
    }

    public long size(String fileName) {
        fileName = unwrap(fileName);
        long length = 0;
        for (int i = 0;; i++) {
            String f = getFileName(fileName, i);
            if (getInstance(fileName).exists(f)) {
                length += getInstance(fileName).size(f);
            } else {
                break;
            }
        }
        return length;
    }

    public String[] listFiles(String directory) {
        String[] array = super.listFiles(directory);
        ArrayList<String> list = New.arrayList();
        for (int i = 0; i < array.length; i++) {
            String f = array[i];
            if (f.endsWith(PART_SUFFIX)) {
                continue;
            }
            list.add(f);
        }
        if (list.size() != array.length) {
            array = new String[list.size()];
            list.toArray(array);
        }
        return array;
    }

    public InputStream newInputStream(String fileName) throws IOException {
        fileName = unwrap(fileName);
        InputStream input = getInstance(fileName).newInputStream(fileName);
        for (int i = 1;; i++) {
            String f = getFileName(fileName, i);
            if (getInstance(f).exists(f)) {
                InputStream i2 = getInstance(f).newInputStream(f);
                input = new SequenceInputStream(input, i2);
            } else {
                break;
            }
        }
        return input;
    }

    public FileObject openFileObject(String fileName, String mode) throws IOException {
        fileName = unwrap(fileName);
        ArrayList<FileObject> list = New.arrayList();
        FileObject o = getInstance(fileName).openFileObject(fileName, mode);
        list.add(o);
        for (int i = 1;; i++) {
            String f = getFileName(fileName, i);
            if (getInstance(fileName).exists(f)) {
                o = getInstance(f).openFileObject(f, mode);
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
            if (maxLength == 0) {
                closeAndThrow(array, array[0], maxLength);
            }
            for (int i = 1; i < array.length - 1; i++) {
                o = array[i];
                long l = o.length();
                length += l;
                if (l != maxLength) {
                    closeAndThrow(array, o, maxLength);
                }
            }
            o = array[array.length - 1];
            long l = o.length();
            length += l;
            if (l > maxLength) {
                closeAndThrow(array, o, maxLength);
            }
        }
        FileObjectSplit fo = new FileObjectSplit(fileName, mode, array, length, maxLength);
        return fo;
    }

    private static void closeAndThrow(FileObject[] array, FileObject o, long maxLength) throws IOException {
        String message = "Expected file length: " + maxLength + " got: " + o.length() + " for " + o.getName();
        for (FileObject f : array) {
            f.close();
        }
        throw new IOException(message);
    }

    public OutputStream newOutputStream(String fileName, boolean append) {
        try {
            return new FileObjectOutputStream(openFileObject(fileName, "rw"), append);
        } catch (IOException e) {
            throw DbException.convertIOException(e, fileName);
        }
    }

    public void moveTo(String oldName, String newName) {
        oldName = unwrap(oldName);
        newName = unwrap(newName);
        for (int i = 0;; i++) {
            String o = getFileName(oldName, i);
            if (getInstance(o).exists(o)) {
                String n = getFileName(newName, i);
                getInstance(n).moveTo(o, n);
            } else {
                break;
            }
        }
    }

    public boolean tryDelete(String fileName) {
        fileName = unwrap(fileName);
        for (int i = 0;; i++) {
            String f = getFileName(fileName, i);
            if (getInstance(fileName).exists(f)) {
                boolean ok = getInstance(fileName).tryDelete(f);
                if (!ok) {
                    return false;
                }
            } else {
                break;
            }
        }
        return true;
    }

    public String unwrap(String fileName) {
        if (!fileName.startsWith(PREFIX)) {
            DbException.throwInternalError(fileName + " doesn't start with " + PREFIX);
        }
        fileName = fileName.substring(PREFIX.length());
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

    protected String getPrefix() {
        return PREFIX;
    }

}
