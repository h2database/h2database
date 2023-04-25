/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs.encrypt;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.h2.mvstore.DataUtils;
import org.h2.security.AES;
import org.h2.security.SHA256;
import org.h2.store.fs.FileBaseDefault;
import org.h2.util.MathUtils;

/**
 * An encrypted file with a read cache.
 */
public class FileEncrypt extends FileBaseDefault {

    /**
     * The block size.
     */
    public static final int BLOCK_SIZE = 4096;

    /**
     * The block size bit mask.
     */
    static final int BLOCK_SIZE_MASK = BLOCK_SIZE - 1;

    /**
     * The length of the file header. Using a smaller header is possible,
     * but would mean reads and writes are not aligned to the block size.
     */
    static final int HEADER_LENGTH = BLOCK_SIZE;

    private static final byte[] HEADER = "H2encrypt\n".getBytes(StandardCharsets.ISO_8859_1);
    private static final int SALT_POS = HEADER.length;

    /**
     * The length of the salt, in bytes.
     */
    private static final int SALT_LENGTH = 8;

    /**
     * The number of iterations. It is relatively low; a higher value would
     * slow down opening files on Android too much.
     */
    private static final int HASH_ITERATIONS = 10;

    private final FileChannel base;

    /**
     * The current file size, from a user perspective.
     */
    private volatile long size;

    private final String name;

    private volatile XTS xts;

    private byte[] encryptionKey;

    private FileEncrypt source;

    public FileEncrypt(String name, byte[] encryptionKey, FileChannel base) {
        // don't do any read or write operations here, because they could
        // fail if the file is locked, and we want to give the caller a
        // chance to lock the file first
        this.name = name;
        this.base = base;
        this.encryptionKey = encryptionKey;
    }

    public FileEncrypt(String name, FileEncrypt source, FileChannel base) {
        // don't do any read or write operations here, because they could
        // fail if the file is locked, and we want to give the caller a
        // chance to lock the file first
        this.name = name;
        this.base = base;
        this.source = source;
        try {
            source.init();
        } catch (IOException e) {
            throw DataUtils.newMVStoreException(DataUtils.ERROR_INTERNAL,
                                        "Can not open {0} using encryption of {1}", name, source.name);
        }
    }

    private XTS init() throws IOException {
        // Keep this method small to allow inlining
        XTS xts = this.xts;
        if (xts == null) {
            xts = createXTS();
        }
        return xts;
    }

    private synchronized XTS createXTS() throws IOException {
        XTS xts = this.xts;
        if (xts != null) {
            return xts;
        }
        assert size == 0;
        long sz = base.size() - HEADER_LENGTH;
        boolean existingFile = sz >= 0;
        if (encryptionKey != null) {
            byte[] salt;
            if (existingFile) {
                salt = new byte[SALT_LENGTH];
                readFully(base, SALT_POS, ByteBuffer.wrap(salt));
            } else {
                byte[] header = Arrays.copyOf(HEADER, BLOCK_SIZE);
                salt = MathUtils.secureRandomBytes(SALT_LENGTH);
                System.arraycopy(salt, 0, header, SALT_POS, salt.length);
                writeFully(base, 0, ByteBuffer.wrap(header));
            }
            AES cipher = new AES();
            cipher.setKey(SHA256.getPBKDF2(encryptionKey, salt, HASH_ITERATIONS, 16));
            encryptionKey = null;
            xts = new XTS(cipher);
        } else {
            if (!existingFile) {
                ByteBuffer byteBuffer = ByteBuffer.allocateDirect(BLOCK_SIZE);
                readFully(source.base, 0, byteBuffer);
                byteBuffer.flip();
                writeFully(base, 0, byteBuffer);
            }
            xts = source.xts;
            source = null;
        }
        if (existingFile) {
            if ((sz & BLOCK_SIZE_MASK) != 0) {
                sz -= BLOCK_SIZE;
            }
            size = sz;
        }
        return this.xts = xts;
    }

    @Override
    protected void implCloseChannel() throws IOException {
        base.close();
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        int len = dst.remaining();
        if (len == 0) {
            return 0;
        }
        XTS xts = init();
        len = (int) Math.min(len, size - position);
        if (position >= size) {
            return -1;
        } else if (position < 0) {
            throw new IllegalArgumentException("pos: " + position);
        }
        if ((position & BLOCK_SIZE_MASK) != 0 || (len & BLOCK_SIZE_MASK) != 0) {
            // either the position or the len is unaligned:
            // read aligned, and then truncate
            long p = position / BLOCK_SIZE * BLOCK_SIZE;
            int offset = (int) (position - p);
            int l = (len + offset + BLOCK_SIZE - 1) / BLOCK_SIZE * BLOCK_SIZE;
            ByteBuffer temp = ByteBuffer.allocate(l);
            readInternal(temp, p, l, xts);
            temp.flip().limit(offset + len).position(offset);
            dst.put(temp);
            return len;
        }
        readInternal(dst, position, len, xts);
        return len;
    }

    private void readInternal(ByteBuffer dst, long position, int len, XTS xts) throws IOException {
        int x = dst.position();
        readFully(base, position + HEADER_LENGTH, dst);
        long block = position / BLOCK_SIZE;
        while (len > 0) {
            xts.decrypt(block++, BLOCK_SIZE, dst.array(), dst.arrayOffset() + x);
            x += BLOCK_SIZE;
            len -= BLOCK_SIZE;
        }
    }

    private static void readFully(FileChannel file, long pos, ByteBuffer dst) throws IOException {
        do {
            int len = file.read(dst, pos);
            if (len < 0) {
                throw new EOFException();
            }
            pos += len;
        } while (dst.remaining() > 0);
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        XTS xts = init();
        int len = src.remaining();
        if ((position & BLOCK_SIZE_MASK) != 0 || (len & BLOCK_SIZE_MASK) != 0) {
            // either the position or the len is unaligned:
            // read aligned, and then truncate
            long p = position / BLOCK_SIZE * BLOCK_SIZE;
            int offset = (int) (position - p);
            int l = (len + offset + BLOCK_SIZE - 1) / BLOCK_SIZE * BLOCK_SIZE;
            ByteBuffer temp = ByteBuffer.allocate(l);
            int available = (int) (size - p + BLOCK_SIZE - 1) / BLOCK_SIZE * BLOCK_SIZE;
            int readLen = Math.min(l, available);
            if (readLen > 0) {
                readInternal(temp, p, readLen, xts);
                temp.rewind();
            }
            temp.limit(offset + len).position(offset);
            temp.put(src).limit(l).rewind();
            writeInternal(temp, p, l, xts);
            long p2 = position + len;
            size = Math.max(size, p2);
            int plus = (int) (size & BLOCK_SIZE_MASK);
            if (plus > 0) {
                temp = ByteBuffer.allocate(plus);
                writeFully(base, p + HEADER_LENGTH + l, temp);
            }
            return len;
        }
        writeInternal(src, position, len, xts);
        long p2 = position + len;
        size = Math.max(size, p2);
        return len;
    }

    private void writeInternal(ByteBuffer src, long position, int len, XTS xts) throws IOException {
        ByteBuffer crypt = ByteBuffer.allocate(len).put(src);
        crypt.flip();
        long block = position / BLOCK_SIZE;
        int x = 0, l = len;
        while (l > 0) {
            xts.encrypt(block++, BLOCK_SIZE, crypt.array(), crypt.arrayOffset() + x);
            x += BLOCK_SIZE;
            l -= BLOCK_SIZE;
        }
        writeFully(base, position + HEADER_LENGTH, crypt);
    }

    private static void writeFully(FileChannel file, long pos, ByteBuffer src) throws IOException {
        do {
            pos += file.write(src, pos);
        } while (src.remaining() > 0);
    }

    @Override
    public long size() throws IOException {
        init();
        return size;
    }

    @Override
    protected void implTruncate(long newSize) throws IOException {
        init();
        if (newSize > size) {
            return;
        }
        if (newSize < 0) {
            throw new IllegalArgumentException("newSize: " + newSize);
        }
        int offset = (int) (newSize & BLOCK_SIZE_MASK);
        if (offset > 0) {
            base.truncate(newSize + HEADER_LENGTH + BLOCK_SIZE);
        } else {
            base.truncate(newSize + HEADER_LENGTH);
        }
        this.size = newSize;
    }

    @Override
    public void force(boolean metaData) throws IOException {
        base.force(metaData);
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        return base.tryLock(position, size, shared);
    }

    @Override
    public String toString() {
        return name;
    }

}
