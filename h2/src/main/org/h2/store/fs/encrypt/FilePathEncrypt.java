/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs.encrypt;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FilePathWrapper;
import org.h2.store.fs.FileUtils;

/**
 * An encrypted file.
 */
public class FilePathEncrypt extends FilePathWrapper {

    private static final String SCHEME = "encrypt";

    /**
     * Register this file system.
     */
    public static void register() {
        FilePath.register(new FilePathEncrypt());
    }

    @Override
    public FileChannel open(String mode) throws IOException {
        String[] parsed = parse(name);
        FileChannel file = FileUtils.open(parsed[1], mode);
        byte[] passwordBytes = parsed[0].getBytes(StandardCharsets.UTF_8);
        return new FileEncrypt(name, passwordBytes, file);
    }

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    protected String getPrefix() {
        String[] parsed = parse(name);
        return getScheme() + ":" + parsed[0] + ":";
    }

    @Override
    public FilePath unwrap(String fileName) {
        return FilePath.get(parse(fileName)[1]);
    }

    @Override
    public long size() {
        long size = getBase().size() - FileEncrypt.HEADER_LENGTH;
        size = Math.max(0, size);
        if ((size & FileEncrypt.BLOCK_SIZE_MASK) != 0) {
            size -= FileEncrypt.BLOCK_SIZE;
        }
        return size;
    }

    @Override
    public OutputStream newOutputStream(boolean append) throws IOException {
        return newFileChannelOutputStream(open("rw"), append);
    }

    @Override
    public InputStream newInputStream() throws IOException {
        return Channels.newInputStream(open("r"));
    }

    /**
     * Split the file name into algorithm, password, and base file name.
     *
     * @param fileName the file name
     * @return an array with algorithm, password, and base file name
     */
    private String[] parse(String fileName) {
        if (!fileName.startsWith(getScheme())) {
            throw new IllegalArgumentException(fileName +
                    " doesn't start with " + getScheme());
        }
        fileName = fileName.substring(getScheme().length() + 1);
        int idx = fileName.indexOf(':');
        String password;
        if (idx < 0) {
            throw new IllegalArgumentException(fileName +
                    " doesn't contain encryption algorithm and password");
        }
        password = fileName.substring(0, idx);
        fileName = fileName.substring(idx + 1);
        return new String[] { password, fileName };
    }

    /**
     * Convert a char array to a byte array, in UTF-16 format. The char array is
     * not cleared after use (this must be done by the caller).
     *
     * @param passwordChars the password characters
     * @return the byte array
     */
    public static byte[] getPasswordBytes(char[] passwordChars) {
        // using UTF-16
        int len = passwordChars.length;
        byte[] password = new byte[len * 2];
        for (int i = 0; i < len; i++) {
            char c = passwordChars[i];
            password[i + i] = (byte) (c >>> 8);
            password[i + i + 1] = (byte) c;
        }
        return password;
    }

}
