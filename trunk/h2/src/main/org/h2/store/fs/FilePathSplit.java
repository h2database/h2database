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
import java.util.List;
import org.h2.constant.SysProperties;
import org.h2.message.DbException;
import org.h2.util.New;

/**
 * A file system that may split files into multiple smaller files.
 * (required for a FAT32 because it only support files up to 2 GB).
 */
public class FilePathSplit extends FilePathWrapper {

    private static final String PART_SUFFIX = ".part";

    protected String getPrefix() {
        return getScheme() + ":" + parse(name)[0] + ":";
    }

    public FilePath unwrap(String fileName) {
        return FilePath.get(parse(fileName)[1]);
    }

    public boolean setReadOnly() {
        boolean result = false;
        for (int i = 0;; i++) {
            FilePath f = getBase(i);
            if (f.exists()) {
                result = f.setReadOnly();
            } else {
                break;
            }
        }
        return result;
    }

    public void delete() {
        for (int i = 0;; i++) {
            FilePath f = getBase(i);
            if (f.exists()) {
                f.delete();
            } else {
                break;
            }
        }
    }

    public long lastModified() {
        long lastModified = 0;
        for (int i = 0;; i++) {
            FilePath f = getBase(i);
            if (f.exists()) {
                long l = f.lastModified();
                lastModified = Math.max(lastModified, l);
            } else {
                break;
            }
        }
        return lastModified;
    }

    public long size() {
        long length = 0;
        for (int i = 0;; i++) {
            FilePath f = getBase(i);
            if (f.exists()) {
                length += f.size();
            } else {
                break;
            }
        }
        return length;
    }

    public ArrayList<FilePath> listFiles() {
        List<FilePath> list = getBase().listFiles();
        ArrayList<FilePath> newList = New.arrayList();
        for (int i = 0, size = list.size(); i < size; i++) {
            FilePath f = list.get(i);
            if (!f.getName().endsWith(PART_SUFFIX)) {
                newList.add(wrap(f));
            }
        }
        return newList;
    }

    public InputStream newInputStream() throws IOException {
        InputStream input = getBase().newInputStream();
        for (int i = 1;; i++) {
            FilePath f = getBase(i);
            if (f.exists()) {
                InputStream i2 = f.newInputStream();
                input = new SequenceInputStream(input, i2);
            } else {
                break;
            }
        }
        return input;
    }

    public FileObject openFileObject(String mode) throws IOException {
        ArrayList<FileObject> list = New.arrayList();
        FileObject o = getBase().openFileObject(mode);
        list.add(o);
        for (int i = 1;; i++) {
            FilePath f = getBase(i);
            if (f.exists()) {
                o = f.openFileObject(mode);
                list.add(o);
            } else {
                break;
            }
        }
        FileObject[] array = new FileObject[list.size()];
        list.toArray(array);
        long maxLength = array[0].size();
        long length = maxLength;
        if (array.length == 1) {
            long defaultMaxLength = getDefaultMaxLength();
            if (maxLength < defaultMaxLength) {
                maxLength = defaultMaxLength;
            }
        } else {
            if (maxLength == 0) {
                closeAndThrow(0, array, array[0], maxLength);
            }
            for (int i = 1; i < array.length - 1; i++) {
                o = array[i];
                long l = o.size();
                length += l;
                if (l != maxLength) {
                    closeAndThrow(i, array, o, maxLength);
                }
            }
            o = array[array.length - 1];
            long l = o.size();
            length += l;
            if (l > maxLength) {
                closeAndThrow(array.length - 1, array, o, maxLength);
            }
        }
        FileObjectSplit fo = new FileObjectSplit(name, mode, array, length, maxLength);
        return fo;
    }

    private long getDefaultMaxLength() {
        return 1L << Integer.decode(parse(name)[0]).intValue();
    }

    private void closeAndThrow(int id, FileObject[] array, FileObject o, long maxLength) throws IOException {
        String message = "Expected file length: " + maxLength + " got: " + o.size() + " for " + getName(id);
        for (FileObject f : array) {
            f.close();
        }
        throw new IOException(message);
    }

    public OutputStream newOutputStream(boolean append) {
        try {
            return new FileObjectOutputStream(openFileObject("rw"), append);
        } catch (IOException e) {
            throw DbException.convertIOException(e, name);
        }
    }

    public void moveTo(FilePath path) {
        FilePathSplit newName = (FilePathSplit) path;
        for (int i = 0;; i++) {
            FilePath o = getBase(i);
            if (o.exists()) {
                o.moveTo(newName.getBase(i));
            } else {
                break;
            }
        }
    }

    /**
     * Split the file name into size and base file name.
     *
     * @param fileName the file name
     * @return an array with size and file name
     */
    private String[] parse(String fileName) {
        if (!fileName.startsWith(getScheme())) {
            DbException.throwInternalError(fileName + " doesn't start with " + getScheme());
        }
        fileName = fileName.substring(getScheme().length() + 1);
        String size;
        if (fileName.length() > 0 && Character.isDigit(fileName.charAt(0))) {
            int idx = fileName.indexOf(':');
            size = fileName.substring(0, idx);
            try {
                fileName = fileName.substring(idx + 1);
            } catch (NumberFormatException e) {
                // ignore
            }
        } else {
            size = Long.toString(SysProperties.SPLIT_FILE_SIZE_SHIFT);
        }
        return new String[] { size, fileName };
    }

    /**
     * Get the file name of a part file.
     *
     * @param id the part id
     * @return the file name including the part id
     */
    private FilePath getBase(int id) {
        return FilePath.get(getName(id));
    }

    private String getName(int id) {
        return id > 0 ? getBase().name + "." + id + PART_SUFFIX : getBase().name;
    }

    public String getScheme() {
        return "split";
    }

}