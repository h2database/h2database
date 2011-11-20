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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.h2.message.DbException;
import org.h2.util.IOUtils;
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

    public long lastModified() {
        return 0;
    }

    public FilePath getParent() {
        int idx = name.lastIndexOf('/');
        return idx < 0 ? null : getPath(name.substring(0, idx));
    }

    public boolean isAbsolute() {
        String fileName = translateFileName(name);
        return FilePath.get(fileName).isAbsolute();
    }

    public FilePath unwrap() {
        return FilePath.get(name.substring(getScheme().length() + 1));
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

    public ArrayList<FilePath> newDirectoryStream() {
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
        return new FileChannelInputStream(open("r"));
    }

    public FileChannel open(String mode) throws IOException {
        ZipFile file = openZipFile();
        ZipEntry entry = file.getEntry(getEntryName());
        if (entry == null) {
            throw new FileNotFoundException(name);
        }
        return new FileZip(file, entry);
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
        return FilePathDisk.expandUserHomeDirectory(fileName);
    }

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

    public FilePath createTempFile(String suffix, boolean deleteOnExit, boolean inTempDir) throws IOException {
        if (!inTempDir) {
            throw new IOException("File system is read-only");
        }
        return new FilePathDisk().getPath(name).createTempFile(suffix, deleteOnExit, true);
    }

    public String getScheme() {
        return "zip";
    }

}

/**
 * The file is read from a stream. When reading from start to end, the same
 * input stream is re-used, however when reading from end to start, a new input
 * stream is opened for each request.
 */
class FileZip extends FileBase {

    private static final byte[] SKIP_BUFFER = new byte[1024];

    private ZipFile file;
    private ZipEntry entry;
    private long pos;
    private InputStream in;
    private long inPos;
    private long length;
    private boolean skipUsingRead;

    FileZip(ZipFile file, ZipEntry entry) {
        this.file = file;
        this.entry = entry;
        length = entry.getSize();
    }

    public long position() {
        return pos;
    }

    public long size() {
        return length;
    }

    public int read(ByteBuffer dst) throws IOException {
        seek();
        int len = in.read(dst.array(), dst.position(), dst.remaining());
        if (len > 0) {
            dst.position(dst.position() + len);
            pos += len;
            inPos += len;
        }
        return len;
    }

    private void seek() throws IOException {
        if (inPos > pos) {
            if (in != null) {
                in.close();
            }
            in = null;
        }
        if (in == null) {
            in = file.getInputStream(entry);
            inPos = 0;
        }
        if (inPos < pos) {
            long skip = pos - inPos;
            if (!skipUsingRead) {
                try {
                    IOUtils.skipFully(in, skip);
                } catch (NullPointerException e) {
                    // workaround for Android
                    skipUsingRead = true;
                }
            }
            if (skipUsingRead) {
                while (skip > 0) {
                    int s = (int) Math.min(SKIP_BUFFER.length, skip);
                    s = in.read(SKIP_BUFFER, 0, s);
                    skip -= s;
                }
            }
            inPos = pos;
        }
    }

    public FileChannel position(long newPos) {
        this.pos = newPos;
        return this;
    }

    public FileChannel truncate(long newLength) throws IOException {
        throw new IOException("File is read-only");
    }

    public void force(boolean metaData) throws IOException {
        // nothing to do
    }

    public int write(ByteBuffer src) throws IOException {
        throw new IOException("File is read-only");
    }

    public synchronized FileLock tryLock(long position, long size, boolean shared) throws IOException {
        return null;
    }

}
