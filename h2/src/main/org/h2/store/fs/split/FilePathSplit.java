/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs.split;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FilePathWrapper;

/**
 * A file system that may split files into multiple smaller files.
 * (required for a FAT32 because it only support files up to 2 GB).
 */
public class FilePathSplit extends FilePathWrapper {

    private static final String PART_SUFFIX = ".part";

    @Override
    protected String getPrefix() {
        return getScheme() + ":" + parse(name)[0] + ":";
    }

    @Override
    public FilePath unwrap(String fileName) {
        return FilePath.get(parse(fileName)[1]);
    }

    @Override
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

    @Override
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

    @Override
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

    @Override
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

    @Override
    public ArrayList<FilePath> newDirectoryStream() {
        List<FilePath> list = getBase().newDirectoryStream();
        ArrayList<FilePath> newList = new ArrayList<>();
        for (FilePath f : list) {
            if (!f.getName().endsWith(PART_SUFFIX)) {
                newList.add(wrap(f));
            }
        }
        return newList;
    }

    @Override
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

    @Override
    public FileChannel open(String mode) throws IOException {
        ArrayList<FileChannel> list = new ArrayList<>();
        list.add(getBase().open(mode));
        for (int i = 1;; i++) {
            FilePath f = getBase(i);
            if (f.exists()) {
                list.add(f.open(mode));
            } else {
                break;
            }
        }
        FileChannel[] array = list.toArray(new FileChannel[0]);
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
                FileChannel c = array[i];
                long l = c.size();
                length += l;
                if (l != maxLength) {
                    closeAndThrow(i, array, c, maxLength);
                }
            }
            FileChannel c = array[array.length - 1];
            long l = c.size();
            length += l;
            if (l > maxLength) {
                closeAndThrow(array.length - 1, array, c, maxLength);
            }
        }
        return new FileSplit(this, mode, array, length, maxLength);
    }

    private long getDefaultMaxLength() {
        return 1L << Integer.decode(parse(name)[0]);
    }

    private void closeAndThrow(int id, FileChannel[] array, FileChannel o,
            long maxLength) throws IOException {
        String message = "Expected file length: " + maxLength + " got: " +
                o.size() + " for " + getName(id);
        for (FileChannel f : array) {
            f.close();
        }
        throw new IOException(message);
    }

    @Override
    public OutputStream newOutputStream(boolean append) throws IOException {
        return newFileChannelOutputStream(open("rw"), append);
    }

    @Override
    public void moveTo(FilePath path, boolean atomicReplace) {
        FilePathSplit newName = (FilePathSplit) path;
        for (int i = 0;; i++) {
            FilePath o = getBase(i);
            if (o.exists()) {
                o.moveTo(newName.getBase(i), atomicReplace);
            } else if (newName.getBase(i).exists()) {
                newName.getBase(i).delete();
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
            throw DbException.getInternalError(fileName + " doesn't start with " + getScheme());
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
    FilePath getBase(int id) {
        return FilePath.get(getName(id));
    }

    private String getName(int id) {
        return id > 0 ? getBase().name + "." + id + PART_SUFFIX : getBase().name;
    }

    @Override
    public String getScheme() {
        return "split";
    }

}
