/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.message.DbException;
import org.h2.store.fs.FileSystem;

/**
 * This utility class contains input/output functions.
 */
public class IOUtils {

    private static final int BUFFER_BLOCK_SIZE = 4 * 1024;

    private IOUtils() {
        // utility class
    }

    /**
     * Close an output stream without throwing an exception.
     *
     * @param out the output stream or null
     */
    public static void closeSilently(OutputStream out) {
        if (out != null) {
            try {
                trace("closeSilently", null, out);
                out.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    /**
     * Skip a number of bytes in an input stream.
     *
     * @param in the input stream
     * @param skip the number of bytes to skip
     * @throws EOFException if the end of file has been reached before all bytes
     *             could be skipped
     * @throws IOException if an IO exception occurred while skipping
     */
    public static void skipFully(InputStream in, long skip) throws IOException {
        while (skip > 0) {
            long skipped = in.skip(skip);
            if (skipped <= 0) {
                throw new EOFException();
            }
            skip -= skipped;
        }
    }

    /**
     * Skip a number of characters in a reader.
     *
     * @param reader the reader
     * @param skip the number of characters to skip
     * @throws EOFException if the end of file has been reached before all
     *             characters could be skipped
     * @throws IOException if an IO exception occurred while skipping
     */
    public static void skipFully(Reader reader, long skip) throws IOException {
        while (skip > 0) {
            long skipped = reader.skip(skip);
            if (skipped <= 0) {
                throw new EOFException();
            }
            skip -= skipped;
        }
    }

    /**
     * Copy all data from the input stream to the output stream and close both
     * streams. Exceptions while closing are ignored.
     *
     * @param in the input stream
     * @param out the output stream
     * @return the number of bytes copied
     */
    public static long copyAndClose(InputStream in, OutputStream out) throws IOException {
        try {
            long len = copyAndCloseInput(in, out);
            out.close();
            return len;
        } finally {
            closeSilently(out);
        }
    }

    /**
     * Copy all data from the input stream to the output stream and close the
     * input stream. Exceptions while closing are ignored.
     *
     * @param in the input stream
     * @param out the output stream
     * @return the number of bytes copied
     */
    public static long copyAndCloseInput(InputStream in, OutputStream out) throws IOException {
        try {
            return copy(in, out);
        } finally {
            closeSilently(in);
        }
    }

    /**
     * Copy all data from the input stream to the output stream. Both streams
     * are kept open.
     *
     * @param in the input stream
     * @param out the output stream
     * @return the number of bytes copied
     */
    public static long copy(InputStream in, OutputStream out) throws IOException {
        long written = 0;
        byte[] buffer = new byte[4 * 1024];
        while (true) {
            int len = in.read(buffer);
            if (len < 0) {
                break;
            }
            out.write(buffer, 0, len);
            written += len;
        }
        return written;
    }

    /**
     * Copy all data from the reader to the writer and close the reader.
     * Exceptions while closing are ignored.
     *
     * @param in the reader
     * @param out the writer
     * @return the number of characters copied
     */
    public static long copyAndCloseInput(Reader in, Writer out) throws IOException {
        long written = 0;
        try {
            char[] buffer = new char[4 * 1024];
            while (true) {
                int len = in.read(buffer);
                if (len < 0) {
                    break;
                }
                out.write(buffer, 0, len);
                written += len;
            }
        } finally {
            in.close();
        }
        return written;
    }

    /**
     * Close an input stream without throwing an exception.
     *
     * @param in the input stream or null
     */
    public static void closeSilently(InputStream in) {
        if (in != null) {
            try {
                trace("closeSilently", null, in);
                in.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    /**
     * Close a reader without throwing an exception.
     *
     * @param reader the reader or null
     */
    public static void closeSilently(Reader reader) {
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    /**
     * Close a writer without throwing an exception.
     *
     * @param writer the writer or null
     */
    public static void closeSilently(Writer writer) {
        if (writer != null) {
            try {
                writer.flush();
                writer.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    /**
     * Read a number of bytes from an input stream and close the stream.
     *
     * @param in the input stream
     * @param length the maximum number of bytes to read, or -1 to read until
     *            the end of file
     * @return the bytes read
     */
    public static byte[] readBytesAndClose(InputStream in, int length) throws IOException {
        try {
            if (length <= 0) {
                length = Integer.MAX_VALUE;
            }
            int block = Math.min(BUFFER_BLOCK_SIZE, length);
            ByteArrayOutputStream out = new ByteArrayOutputStream(block);
            byte[] buff = new byte[block];
            while (length > 0) {
                int len = Math.min(block, length);
                len = in.read(buff, 0, len);
                if (len < 0) {
                    break;
                }
                out.write(buff, 0, len);
                length -= len;
            }
            return out.toByteArray();
        } finally {
            in.close();
        }
    }

    /**
     * Read a number of characters from a reader and close it.
     *
     * @param in the reader
     * @param length the maximum number of characters to read, or -1 to read
     *            until the end of file
     * @return the string read
     */
    public static String readStringAndClose(Reader in, int length) throws IOException {
        try {
            if (length <= 0) {
                length = Integer.MAX_VALUE;
            }
            int block = Math.min(BUFFER_BLOCK_SIZE, length);
            StringWriter out = new StringWriter(length == Integer.MAX_VALUE ? block : length);
            char[] buff = new char[block];
            while (length > 0) {
                int len = Math.min(block, length);
                len = in.read(buff, 0, len);
                if (len < 0) {
                    break;
                }
                out.write(buff, 0, len);
                length -= len;
            }
            return out.toString();
        } finally {
            in.close();
        }
    }

    /**
     * Try to read the given number of bytes to the buffer. This method reads
     * until the maximum number of bytes have been read or until the end of
     * file.
     *
     * @param in the input stream
     * @param buffer the output buffer
     * @param off the offset in the buffer
     * @param max the number of bytes to read at most
     * @return the number of bytes read, 0 meaning EOF
     */
    public static int readFully(InputStream in, byte[] buffer, int off, int max) throws IOException {
        int len = Math.min(max, buffer.length);
        int result = 0;
        while (len > 0) {
            int l = in.read(buffer, off, len);
            if (l < 0) {
                break;
            }
            result += l;
            off += l;
            len -= l;
        }
        return result;
    }

    /**
     * Try to read the given number of characters to the buffer. This method
     * reads until the maximum number of characters have been read or until the
     * end of file.
     *
     * @param in the reader
     * @param buffer the output buffer
     * @param max the number of characters to read at most
     * @return the number of characters read
     */
    public static int readFully(Reader in, char[] buffer, int max) throws IOException {
        int off = 0, len = Math.min(max, buffer.length);
        if (len == 0) {
            return 0;
        }
        while (true) {
            int l = len - off;
            if (l <= 0) {
                break;
            }
            l = in.read(buffer, off, l);
            if (l < 0) {
                break;
            }
            off += l;
        }
        return off <= 0 ? -1 : off;
    }

    /**
     * Create a reader to read from an input stream using the UTF-8 format. If
     * the input stream is null, this method returns null.
     *
     * @param in the input stream or null
     * @return the reader
     */
    public static Reader getReader(InputStream in) {
        try {
            // InputStreamReader may read some more bytes
            return in == null ? null : new BufferedReader(new InputStreamReader(in, Constants.UTF8));
        } catch (Exception e) {
            // UnsupportedEncodingException
            throw DbException.convert(e);
        }
    }

    /**
     * Create a buffered writer to write to an output stream using the UTF-8
     * format. If the output stream is null, this method returns null.
     *
     * @param out the output stream or null
     * @return the writer
     */
    public static Writer getWriter(OutputStream out) {
        try {
            return out == null ? null : new BufferedWriter(new OutputStreamWriter(out, Constants.UTF8));
        } catch (Exception e) {
            // UnsupportedEncodingException
            throw DbException.convert(e);
        }
    }

    /**
     * Create an input stream to read from a string. The string is converted to
     * a byte array using UTF-8 encoding.
     * If the string is null, this method returns null.
     *
     * @param s the string
     * @return the input stream
     */
    public static InputStream getInputStream(String s) {
        if (s == null) {
            return null;
        }
        return new ByteArrayInputStream(StringUtils.utf8Encode(s));
    }

    /**
     * Create a reader to read from a string.
     * If the string is null, this method returns null.
     *
     * @param s the string or null
     * @return the reader
     */
    public static Reader getReader(String s) {
        return s == null ? null : new StringReader(s);
    }

    /**
     * Wrap an input stream in a reader. The bytes are converted to characters
     * using the US-ASCII character set.
     *
     * @param in the input stream
     * @return the reader
     */
    public static Reader getAsciiReader(InputStream in) {
        try {
            return in == null ? null : new InputStreamReader(in, "US-ASCII");
        } catch (Exception e) {
            // UnsupportedEncodingException
            throw DbException.convert(e);
        }
    }

    /**
     * Create the directory and all parent directories if required.
     *
     * @param directory the directory
     * @throws IOException
     */
    public static void mkdirs(File directory) throws IOException {
        // loop, to deal with race conditions (if another thread creates or
        // deletes the same directory at the same time).
        for (int i = 0; i < 5; i++) {
            if (directory.exists()) {
                if (directory.isDirectory()) {
                    return;
                }
                throw new IOException("Could not create directory, because a file with the same name already exists: " + directory.getAbsolutePath());
            }
            if (directory.mkdirs()) {
                return;
            }
        }
        throw new IOException("Could not create directory: " + directory.getAbsolutePath());
    }

    /**
     * Change the length of the file.
     *
     * @param file the random access file
     * @param newLength the new length
     */
    public static void setLength(RandomAccessFile file, long newLength) throws IOException {
        try {
            trace("setLength", null, file);
            file.setLength(newLength);
        } catch (IOException e) {
            long length = file.length();
            if (newLength < length) {
                throw e;
            }
            long pos = file.getFilePointer();
            file.seek(length);
            long remaining = newLength - length;
            int maxSize = 1024 * 1024;
            int block = (int) Math.min(remaining, maxSize);
            byte[] buffer = new byte[block];
            while (remaining > 0) {
                int write = (int) Math.min(remaining, maxSize);
                file.write(buffer, 0, write);
                remaining -= write;
            }
            file.seek(pos);
        }
    }

    /**
     * Get the absolute file path of a file in the user home directory.
     *
     * @param fileName the file name
     * @return the absolute path
     */
    public static String getFileInUserHome(String fileName) {
        String userDir = SysProperties.USER_HOME;
        if (userDir == null) {
            return fileName;
        }
        File file = new File(userDir, fileName);
        return file.getAbsolutePath();
    }

    /**
     * Get the file name (without directory part).
     *
     * @param name the directory and file name
     * @return just the file name
     */
    public static String getFileName(String name) {
        return FileSystem.getInstance(name).getFileName(name);
    }

    /**
     * Normalize a file name.
     *
     * @param fileName the file name
     * @return the normalized file name
     */
    public static String normalize(String fileName) {
        return FileSystem.getInstance(fileName).normalize(fileName);
    }

    /**
     * Try to delete a file.
     *
     * @param fileName the file name
     * @return true if it worked
     */
    public static boolean tryDelete(String fileName) {
        return FileSystem.getInstance(fileName).tryDelete(fileName);
    }

    /**
     * Check if a file is read-only.
     *
     * @param fileName the file name
     * @return if it is read only
     */
    public static boolean isReadOnly(String fileName) {
        return FileSystem.getInstance(fileName).isReadOnly(fileName);
    }

    /**
     * Checks if a file exists.
     *
     * @param fileName the file name
     * @return true if it exists
     */
    public static boolean exists(String fileName) {
        return FileSystem.getInstance(fileName).exists(fileName);
    }

    /**
     * Get the length of a file.
     *
     * @param fileName the file name
     * @return the length in bytes
     */
    public static long length(String fileName) {
        return FileSystem.getInstance(fileName).length(fileName);
    }

    /**
     * Create a new temporary file.
     *
     * @param prefix the prefix of the file name (including directory name if
     *            required)
     * @param suffix the suffix
     * @param deleteOnExit if the file should be deleted when the virtual
     *            machine exists
     * @param inTempDir if the file should be stored in the temporary directory
     * @return the name of the created file
     */
    public static String createTempFile(String prefix, String suffix, boolean deleteOnExit, boolean inTempDir)
            throws IOException {
        return FileSystem.getInstance(prefix).createTempFile(prefix, suffix, deleteOnExit, inTempDir);
    }

    /**
     * Get the parent directory of a file or directory.
     *
     * @param fileName the file or directory name
     * @return the parent directory name
     */
    public static String getParent(String fileName) {
        return FileSystem.getInstance(fileName).getParent(fileName);
    }

    /**
     * List the files in the given directory.
     *
     * @param path the directory
     * @return the list of fully qualified file names
     */
    public static String[] listFiles(String path) {
        return FileSystem.getInstance(path).listFiles(path);
    }

    /**
     * Check if it is a file or a directory.
     *
     * @param fileName the file or directory name
     * @return true if it is a directory
     */
    public static boolean isDirectory(String fileName) {
        return FileSystem.getInstance(fileName).isDirectory(fileName);
    }

    /**
     * Check if the file name includes a path.
     *
     * @param fileName the file name
     * @return if the file name is absolute
     */
    public static boolean isAbsolute(String fileName) {
        return FileSystem.getInstance(fileName).isAbsolute(fileName);
    }

    /**
     * Get the absolute file name.
     *
     * @param fileName the file name
     * @return the absolute file name
     */
    public static String getAbsolutePath(String fileName) {
        return FileSystem.getInstance(fileName).getAbsolutePath(fileName);
    }

    /**
     * Check if a file starts with a given prefix.
     *
     * @param fileName the complete file name
     * @param prefix the prefix
     * @return true if it starts with the prefix
     */
    public static boolean fileStartsWith(String fileName, String prefix) {
        return FileSystem.getInstance(fileName).fileStartsWith(fileName, prefix);
    }

    /**
     * Create an input stream to read from the file.
     *
     * @param fileName the file name
     * @return the input stream
     */
    public static InputStream openFileInputStream(String fileName) throws IOException {
        return FileSystem.getInstance(fileName).openFileInputStream(fileName);
    }

    /**
     * Create an output stream to write into the file.
     *
     * @param fileName the file name
     * @param append if true, the file will grow, if false, the file will be
     *            truncated first
     * @return the output stream
     */
    public static OutputStream openFileOutputStream(String fileName, boolean append) {
        return FileSystem.getInstance(fileName).openFileOutputStream(fileName, append);
    }

    /**
     * Rename a file if this is allowed.
     *
     * @param oldName the old fully qualified file name
     * @param newName the new fully qualified file name
     */
    public static void rename(String oldName, String newName) {
        FileSystem.getInstance(oldName).rename(oldName, newName);
    }

    /**
     * Create all required directories that are required for this file.
     *
     * @param fileName the file name (not directory name)
     */
    public static void createDirs(String fileName) {
        FileSystem.getInstance(fileName).createDirs(fileName);
    }

    /**
     * Delete a file.
     *
     * @param fileName the file name
     */
    public static void delete(String fileName) {
        FileSystem.getInstance(fileName).delete(fileName);
    }

    /**
     * Get the last modified date of a file.
     *
     * @param fileName the file name
     * @return the last modified date
     */
    public static long getLastModified(String fileName) {
        return FileSystem.getInstance(fileName).getLastModified(fileName);
    }

    /**
     * Trace input or output operations if enabled.
     *
     * @param method the method from where this method was called
     * @param fileName the file name
     * @param o the object to append to the message
     */
    static void trace(String method, String fileName, Object o) {
        if (SysProperties.TRACE_IO) {
            System.out.println("IOUtils." + method + " " + fileName + " " + o);
        }
    }

    /**
     * Checks if a file is below a given directory
     *
     * @param file the file to check
     * @param dir the directory the file must be in
     * @return true if the file is within the directory
     */
    public static boolean isInDir(File file, File dir) {
        try {
            String canonicalFileName = file.getCanonicalPath();
            String canonicalDirName = dir.getCanonicalPath();
            if (canonicalFileName.equals(canonicalDirName)) {
                // the file is the directory: not allowed (file "../test" in dir "test")
                return false;
            }
            return canonicalFileName.startsWith(canonicalDirName);
        } catch (IOException e) {
            return false;
        }
    }

}
