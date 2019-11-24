/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs.split;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import org.h2.message.DbException;
import org.h2.store.fs.FileBase;
import org.h2.store.fs.FilePath;

/**
 * A file that may be split into multiple smaller files.
 */
class FileSplit extends FileBase {

    private final FilePathSplit file;
    private final String mode;
    private final long maxLength;
    private FileChannel[] list;
    private long filePointer;
    private long length;

    FileSplit(FilePathSplit file, String mode, FileChannel[] list, long length,
            long maxLength) {
        this.file = file;
        this.mode = mode;
        this.list = list;
        this.length = length;
        this.maxLength = maxLength;
    }

    @Override
    public void implCloseChannel() throws IOException {
        for (FileChannel c : list) {
            c.close();
        }
    }

    @Override
    public long position() {
        return filePointer;
    }

    @Override
    public long size() {
        return length;
    }

    @Override
    public synchronized int read(ByteBuffer dst, long position)
            throws IOException {
        int len = dst.remaining();
        if (len == 0) {
            return 0;
        }
        len = (int) Math.min(len, length - position);
        if (len <= 0) {
            return -1;
        }
        long offset = position % maxLength;
        len = (int) Math.min(len, maxLength - offset);
        FileChannel channel = getFileChannel(position);
        return channel.read(dst, offset);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int len = dst.remaining();
        if (len == 0) {
            return 0;
        }
        len = (int) Math.min(len, length - filePointer);
        if (len <= 0) {
            return -1;
        }
        long offset = filePointer % maxLength;
        len = (int) Math.min(len, maxLength - offset);
        FileChannel channel = getFileChannel(filePointer);
        channel.position(offset);
        len = channel.read(dst);
        filePointer += len;
        return len;
    }

    @Override
    public FileChannel position(long pos) {
        filePointer = pos;
        return this;
    }

    private FileChannel getFileChannel(long position) throws IOException {
        int id = (int) (position / maxLength);
        while (id >= list.length) {
            int i = list.length;
            FileChannel[] newList = new FileChannel[i + 1];
            System.arraycopy(list, 0, newList, 0, i);
            FilePath f = file.getBase(i);
            newList[i] = f.open(mode);
            list = newList;
        }
        return list[id];
    }

    @Override
    public FileChannel truncate(long newLength) throws IOException {
        if (newLength >= length) {
            return this;
        }
        filePointer = Math.min(filePointer, newLength);
        int newFileCount = 1 + (int) (newLength / maxLength);
        if (newFileCount < list.length) {
            // delete some of the files
            FileChannel[] newList = new FileChannel[newFileCount];
            // delete backwards, so that truncating is somewhat transactional
            for (int i = list.length - 1; i >= newFileCount; i--) {
                // verify the file is writable
                list[i].truncate(0);
                list[i].close();
                try {
                    file.getBase(i).delete();
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
        return this;
    }

    @Override
    public void force(boolean metaData) throws IOException {
        for (FileChannel c : list) {
            c.force(metaData);
        }
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        if (position >= length && position > maxLength) {
            // may need to extend and create files
            long oldFilePointer = position;
            long x = length - (length % maxLength) + maxLength;
            for (; x < position; x += maxLength) {
                if (x > length) {
                    // expand the file size
                    position(x - 1);
                    write(ByteBuffer.wrap(new byte[1]));
                }
                position = oldFilePointer;
            }
        }
        long offset = position % maxLength;
        int len = src.remaining();
        FileChannel channel = getFileChannel(position);
        int l = (int) Math.min(len, maxLength - offset);
        if (l == len) {
            l = channel.write(src, offset);
        } else {
            int oldLimit = src.limit();
            src.limit(src.position() + l);
            l = channel.write(src, offset);
            src.limit(oldLimit);
        }
        length = Math.max(length, position + l);
        return l;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        if (filePointer >= length && filePointer > maxLength) {
            // may need to extend and create files
            long oldFilePointer = filePointer;
            long x = length - (length % maxLength) + maxLength;
            for (; x < filePointer; x += maxLength) {
                if (x > length) {
                    // expand the file size
                    position(x - 1);
                    write(ByteBuffer.wrap(new byte[1]));
                }
                filePointer = oldFilePointer;
            }
        }
        long offset = filePointer % maxLength;
        int len = src.remaining();
        FileChannel channel = getFileChannel(filePointer);
        channel.position(offset);
        int l = (int) Math.min(len, maxLength - offset);
        if (l == len) {
            l = channel.write(src);
        } else {
            int oldLimit = src.limit();
            src.limit(src.position() + l);
            l = channel.write(src);
            src.limit(oldLimit);
        }
        filePointer += l;
        length = Math.max(length, filePointer);
        return l;
    }

    @Override
    public synchronized FileLock tryLock(long position, long size,
            boolean shared) throws IOException {
        return list[0].tryLock(position, size, shared);
    }

    @Override
    public String toString() {
        return file.toString();
    }

}