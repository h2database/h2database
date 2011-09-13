/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.EOFException;
import java.io.IOException;
import org.h2.message.DbException;

/**
 * A file that may be split into multiple smaller files.
 */
public class FileObjectSplit implements FileObject {

    private final String name;
    private final String mode;
    private final long maxLength;
    private FileObject[] list;
    private long filePointer;
    private long length;

    FileObjectSplit(String name, String mode, FileObject[] list, long length, long maxLength) {
        this.name = name;
        this.mode = mode;
        this.list = list;
        this.length = length;
        this.maxLength = maxLength;
    }

    public void close() throws IOException {
        for (FileObject f : list) {
            f.close();
        }
    }

    public long position() {
        return filePointer;
    }

    public long size() {
        return length;
    }

    private int read(byte[] b, int off, int len) throws IOException {
        long offset = filePointer % maxLength;
        int l = (int) Math.min(len, maxLength - offset);
        FileObject fo = getFileObject();
        fo.position(offset);
        fo.readFully(b, off, l);
        filePointer += l;
        return l;
    }

    public void readFully(byte[] b, int off, int len) throws IOException {
        if (filePointer + len > length) {
            throw new EOFException();
        }
        while (true) {
            int l = read(b, off, len);
            len -= l;
            if (len <= 0) {
                return;
            }
            off += l;
        }
    }

    public void position(long pos) {
        filePointer = pos;
    }

    private FileObject getFileObject() throws IOException {
        int id = (int) (filePointer / maxLength);
        while (id >= list.length) {
            int i = list.length;
            FileObject[] newList = new FileObject[i + 1];
            System.arraycopy(list, 0, newList, 0, i);
            String fileName = FileSystemSplit.getFileName(name, i);
            newList[i] = FileSystem.getInstance(fileName).openFileObject(fileName, mode);
            list = newList;
        }
        return list[id];
    }

    public void truncate(long newLength) throws IOException {
        if (newLength >= length) {
            return;
        }
        filePointer = Math.min(filePointer, newLength);
        int newFileCount = 1 + (int) (newLength / maxLength);
        if (newFileCount < list.length) {
            // delete some of the files
            FileObject[] newList = new FileObject[newFileCount];
            // delete backwards, so that truncating is somewhat transactional
            for (int i = list.length - 1; i >= newFileCount; i--) {
                // verify the file is writable
                list[i].truncate(0);
                list[i].close();
                try {
                    FileUtils.delete(FileSystemSplit.getFileName(name, i));
                } catch (DbException e) {
                    throw DbException.convertToIOException(e);
                }
            }
            System.arraycopy(list, 0, newList, 0, newList.length);
            list = newList;
        }
        long size = newLength - maxLength * (newFileCount - 1);
        list[list.length - 1].truncate(size);
        this.length = newLength;
    }

    public void sync() throws IOException {
        for (FileObject f : list) {
            f.sync();
        }
    }

    public void write(byte[] b, int off, int len) throws IOException {
        if (filePointer >= length && filePointer > maxLength) {
            // may need to extend and create files
            long oldFilePointer = filePointer;
            long x = length - (length % maxLength) + maxLength;
            for (; x < filePointer; x += maxLength) {
                if (x > length) {
                    position(x - 1);
                    writePart(new byte[1], 0, 1);
                }
                filePointer = oldFilePointer;
            }
        }
        while (true) {
            int l = writePart(b, off, len);
            len -= l;
            if (len <= 0) {
                return;
            }
            off += l;
        }
    }

    private int writePart(byte[] b, int off, int len) throws IOException {
        long offset = filePointer % maxLength;
        int l = (int) Math.min(len, maxLength - offset);
        FileObject fo = getFileObject();
        fo.position(offset);
        fo.write(b, off, l);
        filePointer += l;
        length = Math.max(length, filePointer);
        return l;
    }

    public boolean tryLock() {
        return list[0].tryLock();
    }

    public void releaseLock() {
        list[0].releaseLock();
    }

}
