/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.h2.engine.Constants;

/**
 * This utility class contains utility functions that use the file system
 * abstraction.
 */
public class FileUtils {

    /**
     * {@link StandardOpenOption#READ}.
     */
    public static final Set<? extends OpenOption> R = Collections.singleton(StandardOpenOption.READ);

    /**
     * {@link StandardOpenOption#READ}, {@link StandardOpenOption#WRITE}, and
     * {@link StandardOpenOption#CREATE}.
     */
    public static final Set<? extends OpenOption> RW = Collections
            .unmodifiableSet(EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE));

    /**
     * {@link StandardOpenOption#READ}, {@link StandardOpenOption#WRITE},
     * {@link StandardOpenOption#CREATE}, and {@link StandardOpenOption#SYNC}.
     */
    public static final Set<? extends OpenOption> RWS = Collections.unmodifiableSet(EnumSet.of(StandardOpenOption.READ,
            StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.SYNC));

    /**
     * {@link StandardOpenOption#READ}, {@link StandardOpenOption#WRITE},
     * {@link StandardOpenOption#CREATE}, and {@link StandardOpenOption#DSYNC}.
     */
    public static final Set<? extends OpenOption> RWD = Collections.unmodifiableSet(EnumSet.of(StandardOpenOption.READ,
            StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.DSYNC));

    /**
     * No file attributes.
     */
    public static final FileAttribute<?>[] NO_ATTRIBUTES = new FileAttribute[0];

    /**
     * Checks if a file exists.
     * This method is similar to Java 7 <code>java.nio.file.Path.exists</code>.
     *
     * @param fileName the file name
     * @return true if it exists
     */
    public static boolean exists(String fileName) {
        return FilePath.get(fileName).exists();
    }

    /**
     * Create a directory (all required parent directories must already exist).
     * This method is similar to Java 7
     * <code>java.nio.file.Path.createDirectory</code>.
     *
     * @param directoryName the directory name
     */
    public static void createDirectory(String directoryName) {
        FilePath.get(directoryName).createDirectory();
    }

    /**
     * Create a new file. This method is similar to Java 7
     * <code>java.nio.file.Path.createFile</code>, but returns false instead of
     * throwing a exception if the file already existed.
     *
     * @param fileName the file name
     * @return true if creating was successful
     */
    public static boolean createFile(String fileName) {
        return FilePath.get(fileName).createFile();
    }

    /**
     * Delete a file or directory if it exists.
     * Directories may only be deleted if they are empty.
     * This method is similar to Java 7
     * <code>java.nio.file.Path.deleteIfExists</code>.
     *
     * @param path the file or directory name
     */
    public static void delete(String path) {
        FilePath.get(path).delete();
    }

    /**
     * Get the canonical file or directory name. This method is similar to Java
     * 7 <code>java.nio.file.Path.toRealPath</code>.
     *
     * @param fileName the file name
     * @return the normalized file name
     */
    public static String toRealPath(String fileName) {
        return FilePath.get(fileName).toRealPath().toString();
    }

    /**
     * Get the parent directory of a file or directory. This method returns null
     * if there is no parent. This method is similar to Java 7
     * <code>java.nio.file.Path.getParent</code>.
     *
     * @param fileName the file or directory name
     * @return the parent directory name
     */
    public static String getParent(String fileName) {
        FilePath p = FilePath.get(fileName).getParent();
        return p == null ? null : p.toString();
    }

    /**
     * Check if the file name includes a path. This method is similar to Java 7
     * <code>java.nio.file.Path.isAbsolute</code>.
     *
     * @param fileName the file name
     * @return if the file name is absolute
     */
    public static boolean isAbsolute(String fileName) {
        return FilePath.get(fileName).isAbsolute()
                // Allows Windows to recognize "/path" as absolute.
                // Makes the same configuration work on all platforms.
                || fileName.startsWith(File.separator)
                // Just in case of non-normalized path on Windows
                || fileName.startsWith("/");
    }

    /**
     * Rename a file if this is allowed. This method is similar to Java 7
     * <code>java.nio.file.Files.move</code>.
     *
     * @param source the old fully qualified file name
     * @param target the new fully qualified file name
     */
    public static void move(String source, String target) {
        FilePath.get(source).moveTo(FilePath.get(target), false);
    }

    /**
     * Rename a file if this is allowed, and try to atomically replace an
     * existing file. This method is similar to Java 7
     * <code>java.nio.file.Files.move</code>.
     *
     * @param source the old fully qualified file name
     * @param target the new fully qualified file name
     */
    public static void moveAtomicReplace(String source, String target) {
        FilePath.get(source).moveTo(FilePath.get(target), true);
    }

    /**
     * Get the file or directory name (the last element of the path).
     * This method is similar to Java 7 <code>java.nio.file.Path.getName</code>.
     *
     * @param path the directory and file name
     * @return just the file name
     */
    public static String getName(String path) {
        return FilePath.get(path).getName();
    }

    /**
     * List the files and directories in the given directory.
     * This method is similar to Java 7
     * <code>java.nio.file.Path.newDirectoryStream</code>.
     *
     * @param path the directory
     * @return the list of fully qualified file names
     */
    public static List<String> newDirectoryStream(String path) {
        List<FilePath> list = FilePath.get(path).newDirectoryStream();
        int len = list.size();
        List<String> result = new ArrayList<>(len);
        for (FilePath filePath : list) {
            result.add(filePath.toString());
        }
        return result;
    }

    /**
     * Get the last modified date of a file.
     * This method is similar to Java 7
     * <code>java.nio.file.attribute.Attributes.
     * readBasicFileAttributes(file).lastModified().toMillis()</code>
     *
     * @param fileName the file name
     * @return the last modified date
     */
    public static long lastModified(String fileName) {
        return FilePath.get(fileName).lastModified();
    }

    /**
     * Get the size of a file in bytes
     * This method is similar to Java 7
     * <code>java.nio.file.attribute.Attributes.
     * readBasicFileAttributes(file).size()</code>
     *
     * @param fileName the file name
     * @return the size in bytes
     */
    public static long size(String fileName) {
        return FilePath.get(fileName).size();
    }

    /**
     * Check if it is a file or a directory.
     * <code>java.nio.file.attribute.Attributes.
     * readBasicFileAttributes(file).isDirectory()</code>
     *
     * @param fileName the file or directory name
     * @return true if it is a directory
     */
    public static boolean isDirectory(String fileName) {
        return FilePath.get(fileName).isDirectory();
    }

    /**
     * Tests whether a file is a regular file.
     *
     * @param fileName the file or directory name
     * @return true if it is a regular file
     */
    public static boolean isRegularFile(String fileName) {
        return FilePath.get(fileName).isRegularFile();
    }

    /**
     * Open a random access file object.
     * This method is similar to Java 7
     * <code>java.nio.channels.FileChannel.open</code>.
     *
     * @param fileName the file name
     * @param mode the access mode. Supported are r, rw, rws, rwd
     * @return the file object
     * @throws IOException on failure
     */
    public static FileChannel open(String fileName, String mode)
            throws IOException {
        return FilePath.get(fileName).open(mode);
    }

    /**
     * Create an input stream to read from the file.
     * This method is similar to Java 7
     * <code>java.nio.file.Files.newInputStream()</code>.
     *
     * @param fileName the file name
     * @return the input stream
     * @throws IOException on failure
     */
    public static InputStream newInputStream(String fileName) throws IOException {
        return FilePath.get(fileName).newInputStream();
    }

    /**
     * Create a buffered reader to read from the file.
     * This method is similar to
     * <code>java.nio.file.Files.newBufferedReader()</code>.
     *
     * @param fileName the file name
     * @param charset the charset
     * @return the buffered reader
     * @throws IOException on failure
     */
    public static BufferedReader newBufferedReader(String fileName, Charset charset) throws IOException {
        return new BufferedReader(new InputStreamReader(newInputStream(fileName), charset), Constants.IO_BUFFER_SIZE);
    }

    /**
     * Create an output stream to write into the file.
     * This method is similar to
     * <code>java.nio.file.Files.newOutputStream()</code>.
     *
     * @param fileName the file name
     * @param append if true, the file will grow, if false, the file will be
     *            truncated first
     * @return the output stream
     * @throws IOException on failure
     */
    public static OutputStream newOutputStream(String fileName, boolean append) throws IOException {
        return FilePath.get(fileName).newOutputStream(append);
    }

    /**
     * Check if the file is writable.
     * This method is similar to Java 7
     * <code>java.nio.file.Path.checkAccess(AccessMode.WRITE)</code>
     *
     * @param fileName the file name
     * @return if the file is writable
     */
    public static boolean canWrite(String fileName) {
        return FilePath.get(fileName).canWrite();
    }

    // special methods =======================================

    /**
     * Disable the ability to write. The file can still be deleted afterwards.
     *
     * @param fileName the file name
     * @return true if the call was successful
     */
    public static boolean setReadOnly(String fileName) {
        return FilePath.get(fileName).setReadOnly();
    }

    /**
     * Get the unwrapped file name (without wrapper prefixes if wrapping /
     * delegating file systems are used).
     *
     * @param fileName the file name
     * @return the unwrapped
     */
    public static String unwrap(String fileName) {
        return FilePath.get(fileName).unwrap().toString();
    }

    // utility methods =======================================

    /**
     * Delete a directory or file and all subdirectories and files.
     *
     * @param path the path
     * @param tryOnly whether errors should  be ignored
     */
    public static void deleteRecursive(String path, boolean tryOnly) {
        if (exists(path)) {
            if (isDirectory(path)) {
                for (String s : newDirectoryStream(path)) {
                    deleteRecursive(s, tryOnly);
                }
            }
            if (tryOnly) {
                tryDelete(path);
            } else {
                delete(path);
            }
        }
    }

    /**
     * Create the directory and all required parent directories.
     *
     * @param dir the directory name
     */
    public static void createDirectories(String dir) {
        if (dir != null) {
            if (exists(dir)) {
                if (!isDirectory(dir)) {
                    // this will fail
                    createDirectory(dir);
                }
            } else {
                String parent = getParent(dir);
                createDirectories(parent);
                createDirectory(dir);
            }
        }
    }

    /**
     * Try to delete a file or directory (ignoring errors).
     *
     * @param path the file or directory name
     * @return true if it worked
     */
    public static boolean tryDelete(String path) {
        try {
            FilePath.get(path).delete();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Create a new temporary file.
     *
     * @param prefix the prefix of the file name (including directory name if
     *            required)
     * @param suffix the suffix
     * @param inTempDir if the file should be stored in the temporary directory
     * @return the name of the created file
     * @throws IOException on failure
     */
    public static String createTempFile(String prefix, String suffix,
            boolean inTempDir) throws IOException {
        return FilePath.get(prefix).createTempFile(suffix, inTempDir).toString();
    }

    /**
     * Fully read from the file. This will read all remaining bytes,
     * or throw an EOFException if not successful.
     *
     * @param channel the file channel
     * @param dst the byte buffer
     * @throws IOException on failure
     */
    public static void readFully(FileChannel channel, ByteBuffer dst)
            throws IOException {
        do {
            int r = channel.read(dst);
            if (r < 0) {
                throw new EOFException();
            }
        } while (dst.remaining() > 0);
    }

    /**
     * Fully write to the file. This will write all remaining bytes.
     *
     * @param channel the file channel
     * @param src the byte buffer
     * @throws IOException on failure
     */
    public static void writeFully(FileChannel channel, ByteBuffer src)
            throws IOException {
        do {
            channel.write(src);
        } while (src.remaining() > 0);
    }

    /**
     * Convert the string representation to a set.
     *
     * @param mode the mode as a string
     * @return the set
     */
    public static Set<? extends OpenOption> modeToOptions(String mode) {
        Set<? extends OpenOption> options;
        switch (mode) {
        case "r":
            options = R;
            break;
        case "rw":
            options = RW;
            break;
        case "rws":
            options = RWS;
            break;
        case "rwd":
            options = RWD;
            break;
        default:
            throw new IllegalArgumentException(mode);
        }
        return options;
    }

}
