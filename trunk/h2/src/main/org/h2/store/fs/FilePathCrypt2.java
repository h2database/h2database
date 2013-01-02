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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Arrays;
import org.h2.message.DbException;
import org.h2.mvstore.DataUtils;
import org.h2.security.AES;
import org.h2.security.BlockCipher;
import org.h2.security.SHA256;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;

/**
 * An encrypted file.
 */
public class FilePathCrypt2 extends FilePathWrapper {

    private static final String SCHEME = "crypt2";

    /**
     * Register this file system.
     */
    public static void register() {
        FilePath.register(new FilePathCrypt2());
    }

    public FileChannel open(String mode) throws IOException {
        String[] parsed = parse(name);
        FileChannel file = FileUtils.open(parsed[1], mode);
        byte[] passwordBytes = StringUtils.convertHexToBytes(parsed[0]);
        return new FileCrypt2(passwordBytes, file);
    }

    public String getScheme() {
        return SCHEME;
    }

    protected String getPrefix() {
        String[] parsed = parse(name);
        return getScheme() + ":" + parsed[0] + ":";
    }

    public FilePath unwrap(String fileName) {
        return FilePath.get(parse(fileName)[1]);
    }

    public long size() {
        long len = getBase().size();
        return Math.max(0, len - FileCrypt2.HEADER_LENGTH);
    }

    public OutputStream newOutputStream(boolean append) {
        try {
            return new FileChannelOutputStream(open("rw"), append);
        } catch (IOException e) {
            throw DbException.convertIOException(e, name);
        }
    }

    public InputStream newInputStream() {
        try {
            return new FileChannelInputStream(open("r"));
        } catch (IOException e) {
            throw DbException.convertIOException(e, name);
        }
    }

    /**
     * Split the file name into algorithm, password, and base file name.
     *
     * @param fileName the file name
     * @return an array with algorithm, password, and base file name
     */
    private String[] parse(String fileName) {
        if (!fileName.startsWith(getScheme())) {
            DbException.throwInternalError(fileName + " doesn't start with " + getScheme());
        }
        fileName = fileName.substring(getScheme().length() + 1);
        int idx = fileName.indexOf(':');
        String password;
        if (idx < 0) {
            DbException.throwInternalError(fileName + " doesn't contain encryption algorithm and password");
        }
        password = fileName.substring(0, idx);
        fileName = fileName.substring(idx + 1);
        return new String[] { password, fileName };
    }

    /**
     * An encrypted file with a read cache.
     */
    public static class FileCrypt2 extends FileBase {

        /**
         * The block size.
         */
        // TODO use a block size of 2048 and a header size of 4096
        static final int BLOCK_SIZE = 4096;

        /**
         * The length of the file header. Using a smaller header is possible, but
         * would mean reads and writes are not aligned to the block size.
         */
        static final int HEADER_LENGTH = BLOCK_SIZE;

        // TODO improve the header
        private static final byte[] HEADER = "H2crypt\n".getBytes();
        private static final int SALT_POS = HEADER.length;

        /**
         * The length of the salt, in bytes.
         */
        private static final int SALT_LENGTH = 32;

        /**
         * The number of iterations.
         */
        private static final int HASH_ITERATIONS = 10000;

        private final FileChannel base;

        private final XTS xts;

        private long pos;

        public FileCrypt2(byte[] passwordBytes, FileChannel base) throws IOException {
            // TODO rename 'password' to 'options' (comma separated)
            this.base = base;
            boolean newFile = base.size() < HEADER_LENGTH;
            byte[] salt;
            if (newFile) {
                byte[] header = Arrays.copyOf(HEADER, BLOCK_SIZE);
                salt = MathUtils.secureRandomBytes(SALT_LENGTH);
                System.arraycopy(salt, 0, header, SALT_POS, salt.length);
                DataUtils.writeFully(base, 0, ByteBuffer.wrap(header));
            } else {
                salt = new byte[SALT_LENGTH];
                DataUtils.readFully(base, SALT_POS, ByteBuffer.wrap(salt));
            }
            // TODO support Fog (and maybe Fog2)
            // Fog cipher = new Fog();
            AES cipher = new AES();
            cipher.setKey(SHA256.getPBKDF2(passwordBytes, salt, HASH_ITERATIONS, 16));
            xts = new XTS(cipher);
        }

        protected void implCloseChannel() throws IOException {
            base.close();
        }

        public FileChannel position(long newPosition) throws IOException {
            this.pos = newPosition;
            return this;
        }

        public long position() throws IOException {
            return pos;
        }

        public int read(ByteBuffer dst) throws IOException {
            int len = read(dst, pos);
            if (len > 0) {
                pos += len;
            }
            return len;
        }

        public int read(ByteBuffer dst, long position) throws IOException {
            int len = dst.remaining();
            if (position % BLOCK_SIZE != 0) {
                throw new IllegalArgumentException("pos: " + position);
            }
            if (len % BLOCK_SIZE != 0) {
                throw new IllegalArgumentException("len: " + len);
            }
            int x = dst.position();
            len = base.read(dst, position + HEADER_LENGTH);
            long block = position / BLOCK_SIZE;
            int l = len;
            while (l > 0) {
                xts.decrypt(block++, BLOCK_SIZE, dst.array(), x);
                x += BLOCK_SIZE;
                l -= BLOCK_SIZE;
            }
            return len;
        }

        public int write(ByteBuffer src, long position) throws IOException {
          int len = src.remaining();
          // TODO support non-block aligned file length / reads / writes
          if (position % BLOCK_SIZE != 0) {
              throw new IllegalArgumentException("pos: " + position);
          }
          if (len % BLOCK_SIZE != 0) {
              throw new IllegalArgumentException("len: " + len);
          }
          ByteBuffer crypt = ByteBuffer.allocate(len);
          crypt.put(src);
          crypt.flip();
          long block = position / BLOCK_SIZE;
          int x = 0;
          while (len > 0) {
              xts.encrypt(block++, BLOCK_SIZE, crypt.array(), x);
              x += BLOCK_SIZE;
              len -= BLOCK_SIZE;
          }
          return base.write(crypt, position + BLOCK_SIZE);
      }

      public int write(ByteBuffer src) throws IOException {
          int len = write(src, pos);
          if (len > 0) {
              pos += len;
          }
          return len;
      }

        public long size() throws IOException {
            return base.size() - HEADER_LENGTH;
        }

        public FileChannel truncate(long newSize) throws IOException {
            if (newSize % BLOCK_SIZE != 0) {
                throw new IllegalArgumentException("newSize: " + newSize);
            }
            base.truncate(newSize + BLOCK_SIZE);
            return this;
        }

        public void force(boolean metaData) throws IOException {
            base.force(metaData);
        }

        public FileLock tryLock(long position, long size, boolean shared) throws IOException {
            return base.tryLock(position, size, shared);
        }

        public String toString() {
            return SCHEME + ":" + base.toString();
        }

    }

    /**
     * An XTS implementation as described in
     * IEEE P1619 (Standard Architecture for Encrypted Shared Storage Media).
     * See also
     * http://axelkenzo.ru/downloads/1619-2007-NIST-Submission.pdf
     */
    static class XTS {

        /**
         * Galois Field feedback.
         */
        private static final int GF_128_FEEDBACK = 0x87;

        /**
         * The AES encryption block size.
         */
        private static final int CIPHER_BLOCK_SIZE = 16;

        private final BlockCipher cipher;

        XTS(BlockCipher cipher) {
            this.cipher = cipher;
        }

        void encrypt(long id, int len, byte[] data, int offset) {
            byte[] tweak = initTweak(id);
            int i = 0;
            for (; i + CIPHER_BLOCK_SIZE <= len; i += CIPHER_BLOCK_SIZE) {
                if (i > 0) {
                    updateTweak(tweak);
                }
                xorTweak(data, i + offset, tweak);
                cipher.encrypt(data, i + offset, CIPHER_BLOCK_SIZE);
                xorTweak(data, i + offset, tweak);
            }
            if (i < len) {
                updateTweak(tweak);
                swap(data, i + offset, i - CIPHER_BLOCK_SIZE + offset, len - i);
                xorTweak(data, i - CIPHER_BLOCK_SIZE + offset, tweak);
                cipher.encrypt(data, i - CIPHER_BLOCK_SIZE + offset, CIPHER_BLOCK_SIZE);
                xorTweak(data, i - CIPHER_BLOCK_SIZE + offset, tweak);
            }
        }

        void decrypt(long id, int len, byte[] data, int offset) {
            byte[] tweak = initTweak(id), tweakEnd = tweak;
            int i = 0;
            for (; i + CIPHER_BLOCK_SIZE <= len; i += CIPHER_BLOCK_SIZE) {
                if (i > 0) {
                    updateTweak(tweak);
                    if (i + CIPHER_BLOCK_SIZE + CIPHER_BLOCK_SIZE > len && i + CIPHER_BLOCK_SIZE < len) {
                        tweakEnd = Arrays.copyOf(tweak, CIPHER_BLOCK_SIZE);
                        updateTweak(tweak);
                    }
                }
                xorTweak(data, i + offset, tweak);
                cipher.decrypt(data, i + offset, CIPHER_BLOCK_SIZE);
                xorTweak(data, i + offset, tweak);
            }
            if (i < len) {
                swap(data, i, i - CIPHER_BLOCK_SIZE + offset, len - i + offset);
                xorTweak(data, i - CIPHER_BLOCK_SIZE  + offset, tweakEnd);
                cipher.decrypt(data, i - CIPHER_BLOCK_SIZE + offset, CIPHER_BLOCK_SIZE);
                xorTweak(data, i - CIPHER_BLOCK_SIZE + offset, tweakEnd);
            }
        }

        private byte[] initTweak(long id) {
            byte[] tweak = new byte[CIPHER_BLOCK_SIZE];
            for (int j = 0; j < CIPHER_BLOCK_SIZE; j++, id >>>= 8) {
                tweak[j] = (byte) (id & 0xff);
            }
            cipher.encrypt(tweak, 0, CIPHER_BLOCK_SIZE);
            return tweak;
        }

        private static void xorTweak(byte[] data, int pos, byte[] tweak) {
            for (int i = 0; i < CIPHER_BLOCK_SIZE; i++) {
                data[pos + i] ^= tweak[i];
            }
        }

        static void updateTweak(byte[] tweak) {
            byte ci = 0, co = 0;
            for (int i = 0; i < CIPHER_BLOCK_SIZE; i++) {
                co = (byte) ((tweak[i] >> 7) & 1);
                tweak[i] = (byte) (((tweak[i] << 1) + ci) & 255);
                ci = co;
            }
            if (co != 0) {
                tweak[0] ^= GF_128_FEEDBACK;
            }
        }

        static void swap(byte[] data, int source, int target, int len) {
            for (int i = 0; i < len; i++) {
                byte temp = data[source + i];
                data[source + i] = data[target + i];
                data[target + i] = temp;
            }
        }

    }

}
