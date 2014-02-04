/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;

import org.h2.engine.Constants;
import org.h2.util.New;

/**
 * Utility methods
 */
public class DataUtils {

    /**
     * An error occurred while reading from the file.
     */
    public static final int ERROR_READING_FAILED = 1;

    /**
     * An error occurred when trying to write to the file.
     */
    public static final int ERROR_WRITING_FAILED = 2;

    /**
     * An internal error occurred. This could be a bug, or a memory corruption
     * (for example caused by out of memory).
     */
    public static final int ERROR_INTERNAL = 3;

    /**
     * The object is already closed.
     */
    public static final int ERROR_CLOSED = 4;

    /**
     * The file format is not supported.
     */
    public static final int ERROR_UNSUPPORTED_FORMAT = 5;

    /**
     * The file is corrupt or (for encrypted files) the encryption key is wrong.
     */
    public static final int ERROR_FILE_CORRUPT = 6;

    /**
     * The file is locked.
     */
    public static final int ERROR_FILE_LOCKED = 7;

    /**
     * An error occurred when serializing or de-serializing.
     */
    public static final int ERROR_SERIALIZATION = 8;

    /**
     * The transaction store is corrupt.
     */
    public static final int ERROR_TRANSACTION_CORRUPT = 100;

    /**
     * An entry is still locked by another transaction.
     */
    public static final int ERROR_TRANSACTION_LOCKED = 101;

    /**
     * A very old transaction is still open.
     */
    public static final int ERROR_TRANSACTION_STILL_OPEN = 102;

    /**
     * The type for leaf page.
     */
    public static final int PAGE_TYPE_LEAF = 0;

    /**
     * The type for node page.
     */
    public static final int PAGE_TYPE_NODE = 1;

    /**
     * The bit mask for compressed pages.
     */
    public static final int PAGE_COMPRESSED = 2;

    /**
     * The maximum length of a variable size int.
     */
    public static final int MAX_VAR_INT_LEN = 5;

    /**
     * The maximum length of a variable size long.
     */
    public static final int MAX_VAR_LONG_LEN = 10;

    /**
     * The maximum integer that needs less space when using variable size
     * encoding (only 3 bytes instead of 4).
     */
    public static final int COMPRESSED_VAR_INT_MAX = 0x1fffff;

    /**
     * The maximum long that needs less space when using variable size
     * encoding (only 7 bytes instead of 8).
     */
    public static final long COMPRESSED_VAR_LONG_MAX = 0x1ffffffffffffL;

    /**
     * The estimated number of bytes used per page object.
     */
    public static final int PAGE_MEMORY = 128;

    /**
     * The estimated number of bytes used per child entry.
     */
    public static final int PAGE_MEMORY_CHILD = 16;

    /**
     * The UTF-8 character encoding format.
     */
    public static final Charset UTF8 = Charset.forName("UTF-8");
    
    /**
     * The ISO Latin character encoding format.
     */
    public static final Charset LATIN = Charset.forName("ISO-8859-1");

    /**
     * An 0-size byte array.
     */
    private static final byte[] EMPTY_BYTES = {};

    /**
     * The maximum byte to grow a buffer at a time.
     */
    private static final int MAX_GROW = 16 * 1024 * 1024;

    /**
     * Get the length of the variable size int.
     *
     * @param x the value
     * @return the length in bytes
     */
    public static int getVarIntLen(int x) {
        if ((x & (-1 << 7)) == 0) {
            return 1;
        } else if ((x & (-1 << 14)) == 0) {
            return 2;
        } else if ((x & (-1 << 21)) == 0) {
            return 3;
        } else if ((x & (-1 << 28)) == 0) {
            return 4;
        }
        return 5;
    }

    /**
     * Get the length of the variable size long.
     *
     * @param x the value
     * @return the length in bytes
     */
    public static int getVarLongLen(long x) {
        int i = 1;
        while (true) {
            x >>>= 7;
            if (x == 0) {
                return i;
            }
            i++;
        }
    }

    /**
     * Read a variable size int.
     *
     * @param buff the source buffer
     * @return the value
     */
    public static int readVarInt(ByteBuffer buff) {
        int b = buff.get();
        if (b >= 0) {
            return b;
        }
        // a separate function so that this one can be inlined
        return readVarIntRest(buff, b);
    }

    private static int readVarIntRest(ByteBuffer buff, int b) {
        int x = b & 0x7f;
        b = buff.get();
        if (b >= 0) {
            return x | (b << 7);
        }
        x |= (b & 0x7f) << 7;
        b = buff.get();
        if (b >= 0) {
            return x | (b << 14);
        }
        x |= (b & 0x7f) << 14;
        b = buff.get();
        if (b >= 0) {
            return x | b << 21;
        }
        x |= ((b & 0x7f) << 21) | (buff.get() << 28);
        return x;
    }

    /**
     * Read a variable size long.
     *
     * @param buff the source buffer
     * @return the value
     */
    public static long readVarLong(ByteBuffer buff) {
        long x = buff.get();
        if (x >= 0) {
            return x;
        }
        x &= 0x7f;
        for (int s = 7; s < 64; s += 7) {
            long b = buff.get();
            x |= (b & 0x7f) << s;
            if (b >= 0) {
                break;
            }
        }
        return x;
    }

    /**
     * Write a variable size int.
     *
     * @param out the output stream
     * @param x the value
     */
    public static void writeVarInt(OutputStream out, int x) throws IOException {
        while ((x & ~0x7f) != 0) {
            out.write((byte) (0x80 | (x & 0x7f)));
            x >>>= 7;
        }
        out.write((byte) x);
    }

    /**
     * Write a variable size int.
     *
     * @param buff the source buffer
     * @param x the value
     */
    public static void writeVarInt(ByteBuffer buff, int x) {
        while ((x & ~0x7f) != 0) {
            buff.put((byte) (0x80 | (x & 0x7f)));
            x >>>= 7;
        }
        buff.put((byte) x);
    }

    /**
     * Write characters from a string (without the length).
     *
     * @param buff the target buffer
     * @param s the string
     * @param len the number of characters
     * @return the byte buffer
     */
    public static ByteBuffer writeStringData(ByteBuffer buff, String s, int len) {
        buff = DataUtils.ensureCapacity(buff, 3 * len);
        for (int i = 0; i < len; i++) {
            int c = s.charAt(i);
            if (c < 0x80) {
                buff.put((byte) c);
            } else if (c >= 0x800) {
                buff.put((byte) (0xe0 | (c >> 12)));
                buff.put((byte) (((c >> 6) & 0x3f)));
                buff.put((byte) (c & 0x3f));
            } else {
                buff.put((byte) (0xc0 | (c >> 6)));
                buff.put((byte) (c & 0x3f));
            }
        }
        return buff;
    }

    /**
     * Read a string.
     *
     * @param buff the source buffer
     * @param len the number of characters
     * @return the value
     */
    public static String readString(ByteBuffer buff, int len) {
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            int x = buff.get() & 0xff;
            if (x < 0x80) {
                chars[i] = (char) x;
            } else if (x >= 0xe0) {
                chars[i] = (char) (((x & 0xf) << 12) + ((buff.get() & 0x3f) << 6) + (buff.get() & 0x3f));
            } else {
                chars[i] = (char) (((x & 0x1f) << 6) + (buff.get() & 0x3f));
            }
        }
        return new String(chars);
    }

    /**
     * Write a variable size long.
     *
     * @param buff the target buffer
     * @param x the value
     */
    public static void writeVarLong(ByteBuffer buff, long x) {
        while ((x & ~0x7f) != 0) {
            buff.put((byte) (0x80 | (x & 0x7f)));
            x >>>= 7;
        }
        buff.put((byte) x);
    }

    /**
     * Write a variable size long.
     *
     * @param out the output stream
     * @param x the value
     */
    public static void writeVarLong(OutputStream out, long x) throws IOException {
        while ((x & ~0x7f) != 0) {
            out.write((byte) (0x80 | (x & 0x7f)));
            x >>>= 7;
        }
        out.write((byte) x);
    }

    /**
     * Copy the elements of an array, with a gap.
     *
     * @param src the source array
     * @param dst the target array
     * @param oldSize the size of the old array
     * @param gapIndex the index of the gap
     */
    public static void copyWithGap(Object src, Object dst, int oldSize, int gapIndex) {
        if (gapIndex > 0) {
            System.arraycopy(src, 0, dst, 0, gapIndex);
        }
        if (gapIndex < oldSize) {
            System.arraycopy(src, gapIndex, dst, gapIndex + 1, oldSize - gapIndex);
        }
    }

    /**
     * Copy the elements of an array, and remove one element.
     *
     * @param src the source array
     * @param dst the target array
     * @param oldSize the size of the old array
     * @param removeIndex the index of the entry to remove
     */
    public static void copyExcept(Object src, Object dst, int oldSize, int removeIndex) {
        if (removeIndex > 0 && oldSize > 0) {
            System.arraycopy(src, 0, dst, 0, removeIndex);
        }
        if (removeIndex < oldSize) {
            System.arraycopy(src, removeIndex + 1, dst, removeIndex, oldSize - removeIndex - 1);
        }
    }

    /**
     * Read from a file channel until the buffer is full, or end-of-file
     * has been reached. The buffer is rewind after reading.
     *
     * @param file the file channel
     * @param pos the absolute position within the file
     * @param dst the byte buffer
     */
    public static void readFully(FileChannel file, long pos, ByteBuffer dst) {
        try {
            do {
                int len = file.read(dst, pos);
                if (len < 0) {
                    throw new EOFException();
                }
                pos += len;
            } while (dst.remaining() > 0);
            dst.rewind();
        } catch (IOException e) {
            long size;
            try {
                size = file.size();
            } catch (IOException e2) {
                size = -1;
            }
            throw newIllegalStateException(
                    ERROR_READING_FAILED,
                    "Reading from {0} failed; file length {1} read length {2} at {3}",
                    file, size, dst.remaining(), pos, e);
        }
    }

    /**
     * Write to a file channel.
     *
     * @param file the file channel
     * @param pos the absolute position within the file
     * @param src the source buffer
     */
    public static void writeFully(FileChannel file, long pos, ByteBuffer src) {
        try {
            int off = 0;
            do {
                int len = file.write(src, pos + off);
                off += len;
            } while (src.remaining() > 0);
        } catch (IOException e) {
            throw newIllegalStateException(
                    ERROR_WRITING_FAILED,
                    "Writing to {0} failed; length {1} at {2}",
                    file, src.remaining(), pos, e);
        }
    }

    /**
     * Convert the length to a length code 0..31. 31 means more than 1 MB.
     *
     * @param len the length
     * @return the length code
     */
    public static int encodeLength(int len) {
        if (len <= 32) {
            return 0;
        }
        int code = Integer.numberOfLeadingZeros(len);
        int remaining = len << (code + 1);
        code += code;
        if ((remaining & (1 << 31)) != 0) {
            code--;
        }
        if ((remaining << 1) != 0) {
            code--;
        }
        code = Math.min(31, 52 - code);
        // alternative code (slower):
        // int x = len;
        // int shift = 0;
        // while (x > 3) {
        //    shift++;
        //    x = (x >>> 1) + (x & 1);
        // }
        // shift = Math.max(0,  shift - 4);
        // int code = (shift << 1) + (x & 1);
        // code = Math.min(31, code);
        return code;
    }

    /**
     * Get the chunk id from the position.
     *
     * @param pos the position
     * @return the chunk id
     */
    public static int getPageChunkId(long pos) {
        return (int) (pos >>> 38);
    }

    /**
     * Get the maximum length for the given code.
     * For the code 31, Integer.MAX_VALUE is returned.
     *
     * @param pos the position
     * @return the maximum length
     */
    public static int getPageMaxLength(long pos) {
        int code = (int) ((pos >> 1) & 31);
        if (code == 31) {
            return Integer.MAX_VALUE;
        }
        return (2 + (code & 1)) << ((code >> 1) + 4);
    }

    /**
     * Get the offset from the position.
     *
     * @param pos the position
     * @return the offset
     */
    public static int getPageOffset(long pos) {
        return (int) (pos >> 6);
    }

    /**
     * Get the page type from the position.
     *
     * @param pos the position
     * @return the page type (PAGE_TYPE_NODE or PAGE_TYPE_LEAF)
     */
    public static int getPageType(long pos) {
        return ((int) pos) & 1;
    }

    /**
     * Get the position of this page. The following information is encoded in
     * the position: the chunk id, the offset, the maximum length, and the type
     * (node or leaf).
     *
     * @param chunkId the chunk id
     * @param offset the offset
     * @param length the length
     * @param type the page type (1 for node, 0 for leaf)
     * @return the position
     */
    public static long getPagePos(int chunkId, int offset, int length, int type) {
        long pos = (long) chunkId << 38;
        pos |= (long) offset << 6;
        pos |= encodeLength(length) << 1;
        pos |= type;
        return pos;
    }

    /**
     * Calculate a check value for the given integer. A check value is mean to
     * verify the data is consistent with a high probability, but not meant to
     * protect against media failure or deliberate changes.
     *
     * @param x the value
     * @return the check value
     */
    public static short getCheckValue(int x) {
        return (short) ((x >> 16) ^ x);
    }

    /**
     * Append a map to the string builder, sorted by key.
     *
     * @param buff the target buffer
     * @param map the map
     * @return the string builder
     */
    public static StringBuilder appendMap(StringBuilder buff, HashMap<String, ?> map) {
        ArrayList<String> list = New.arrayList(map.keySet());
        Collections.sort(list);
        for (String k : list) {
            appendMap(buff, k, map.get(k));
        }
        return buff;
    }
    
    /**
     * Append a key-value pair to the string builder. Keys may not contain a
     * colon. Values that contain a comma or a double quote are enclosed in
     * double quotes, with special characters escaped using a backslash.
     *
     * @param buff the target buffer
     * @param key the key
     * @param value the value
     */
    public static void appendMap(StringBuilder buff, String key, Object value) {
        if (buff.length() > 0) {
            buff.append(',');
        }
        buff.append(key).append(':');
        String v;
        if (value instanceof Long) {
            v = Long.toHexString((Long) value);
        } else if (value instanceof Integer) {
            v = Integer.toHexString((Integer) value);
        } else {
            v = value.toString();
        }
        if (v.indexOf(',') < 0 && v.indexOf('\"') < 0) {
            buff.append(v);
        } else {
            buff.append('\"');
            for (int i = 0, size = v.length(); i < size; i++) {
                char c = v.charAt(i);
                if (c == '\"') {
                    buff.append('\\');
                }
                buff.append(c);
            }
            buff.append('\"');
        }
    }

    /**
     * Parse a key-value pair list.
     *
     * @param s the list
     * @return the map
     * @throws IllegalStateException if parsing failed
     */
    public static HashMap<String, String> parseMap(String s) {
        HashMap<String, String> map = New.hashMap();
        for (int i = 0, size = s.length(); i < size;) {
            int startKey = i;
            i = s.indexOf(':', i);
            if (i < 0) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_FILE_CORRUPT, "Not a map: {0}", s);
            }
            String key = s.substring(startKey, i++);
            StringBuilder buff = new StringBuilder();
            while (i < size) {
                char c = s.charAt(i++);
                if (c == ',') {
                    break;
                } else if (c == '\"') {
                    while (i < size) {
                        c = s.charAt(i++);
                        if (c == '\\') {
                            if (i == size) {
                                throw DataUtils.newIllegalStateException(
                                        DataUtils.ERROR_FILE_CORRUPT,
                                        "Not a map: {0}", s);
                            }
                            c = s.charAt(i++);
                        } else if (c == '\"') {
                            break;
                        }
                        buff.append(c);
                    }
                } else {
                    buff.append(c);
                }
            }
            map.put(key, buff.toString());
        }
        return map;
    }

    /**
     * Calculate the Fletcher32 checksum.
     *
     * @param bytes the bytes
     * @param length the message length (must be a multiple of 2)
     * @return the checksum
     */
    public static int getFletcher32(byte[] bytes, int length) {
        int s1 = 0xffff, s2 = 0xffff;
        for (int i = 0; i < length;) {
            // reduce after 360 words (each word is two bytes)
            for (int end = Math.min(i + 720, length); i < end;) {
                int x = ((bytes[i++] & 0xff) << 8) | (bytes[i++] & 0xff);
                s2 += s1 += x;
            }
            s1 = (s1 & 0xffff) + (s1 >>> 16);
            s2 = (s2 & 0xffff) + (s2 >>> 16);
        }
        s1 = (s1 & 0xffff) + (s1 >>> 16);
        s2 = (s2 & 0xffff) + (s2 >>> 16);
        return (s2 << 16) | s1;
    }

    /**
     * Throw an IllegalArgumentException if the argument is invalid.
     *
     * @param test true if the argument is valid
     * @param message the message
     * @param arguments the arguments
     * @throws IllegalArgumentException if the argument is invalid
     */
    public static void checkArgument(boolean test, String message, Object... arguments) {
        if (!test) {
            throw newIllegalArgumentException(message, arguments);
        }
    }

    /**
     * Create a new IllegalArgumentException.
     *
     * @param message the message
     * @param arguments the arguments
     * @return the exception
     */
    public static IllegalArgumentException newIllegalArgumentException(
            String message, Object... arguments) {
        return initCause(new IllegalArgumentException(
                formatMessage(0, message, arguments)),
                arguments);
    }

    /**
     * Create a new UnsupportedOperationException.
     *
     * @param message the message
     * @return the exception
     */
    public static UnsupportedOperationException newUnsupportedOperationException(
            String message) {
        return new UnsupportedOperationException(formatMessage(0, message));
    }

    /**
     * Create a new ConcurrentModificationException.
     *
     * @param message the message
     * @return the exception
     */
    public static ConcurrentModificationException newConcurrentModificationException(
            String message) {
        return new ConcurrentModificationException(formatMessage(0, message));
    }

    /**
     * Create a new IllegalStateException.
     *
     * @param errorCode the error code
     * @param message the message
     * @param arguments the arguments
     * @return the exception
     */
    public static IllegalStateException newIllegalStateException(
            int errorCode, String message, Object... arguments) {
        return initCause(new IllegalStateException(
                formatMessage(errorCode, message, arguments)),
                arguments);
    }

    private static <T extends Exception> T initCause(T e, Object... arguments) {
        int size = arguments.length;
        if (size > 0) {
            Object o = arguments[size - 1];
            if (o instanceof Exception) {
                e.initCause((Exception) o);
            }
        }
        return e;
    }

    private static String formatMessage(int errorCode, String message, Object... arguments) {
        // convert arguments to strings, to avoid locale specific formatting
        for (int i = 0; i < arguments.length; i++) {
            Object a = arguments[i];
            if (!(a instanceof Exception)) {
                String s = a == null ? "null" : a.toString();
                if (s.length() > 1000) {
                    s = s.substring(0, 1000) + "...";
                }
                arguments[i] = s;
            }
        }
        return MessageFormat.format(message, arguments) +
                " [" + Constants.VERSION_MAJOR + "." +
                Constants.VERSION_MINOR + "." + Constants.BUILD_ID +
                "/" + errorCode + "]";
    }

    /**
     * Get the error code from an exception message.
     *
     * @param m the message
     * @return the error code, or 0 if none
     */
    public static int getErrorCode(String m) {
        if (m != null && m.endsWith("]")) {
            int dash = m.lastIndexOf('/');
            if (dash >= 0) {
                String s = m.substring(dash + 1, m.length() - 1);
                try {
                    return Integer.parseInt(s);
                } catch (NumberFormatException e) {
                    // no error code
                }
            }
        }
        return 0;
    }

    /**
     * Create an array of bytes with the given size. If this is not possible
     * because not enough memory is available, an OutOfMemoryError with the
     * requested size in the message is thrown.
     * <p>
     * This method should be used if the size of the array is user defined, or
     * stored in a file, so wrong size data can be distinguished from regular
     * out-of-memory.
     *
     * @param len the number of bytes requested
     * @return the byte array
     * @throws OutOfMemoryError if the allocation was too large
     */
    public static byte[] newBytes(int len) {
        if (len == 0) {
            return EMPTY_BYTES;
        }
        try {
            return new byte[len];
        } catch (OutOfMemoryError e) {
            Error e2 = new OutOfMemoryError("Requested memory: " + len);
            e2.initCause(e);
            throw e2;
        }
    }

    /**
     * Ensure the byte buffer has the given capacity, plus 1 KB. If not, a new,
     * larger byte buffer is created and the data is copied.
     *
     * @param buff the byte buffer
     * @param len the minimum remaining capacity
     * @return the byte buffer (possibly a new one)
     */
    public static ByteBuffer ensureCapacity(ByteBuffer buff, int len) {
        len += 1024;
        if (buff.remaining() > len) {
            return buff;
        }
        return grow(buff, len);
    }

    private static ByteBuffer grow(ByteBuffer buff, int len) {
        len = buff.remaining() + len;
        int capacity = buff.capacity();
        len = Math.max(len, Math.min(capacity + MAX_GROW, capacity * 2));
        ByteBuffer temp = ByteBuffer.allocate(len);
        buff.flip();
        temp.put(buff);
        return temp;
    }

    /**
     * Read a hex long value from a map.
     *
     * @param map the map
     * @param key the key
     * @param defaultValue if the value is null
     * @return the parsed value
     * @throws IllegalStateException if parsing fails
     */
    public static long readHexLong(HashMap<String, ? extends Object> map, String key, long defaultValue) {
        Object v = map.get(key);
        if (v == null) {
            return defaultValue;
        } else if (v instanceof Long) {
            return (Long) v;
        }
        try {
            return Long.parseLong((String) v, 16);
        } catch (NumberFormatException e) {
            throw newIllegalStateException(ERROR_FILE_CORRUPT,
                    "Error parsing the value {0}", v, e);
        }
    }
    
    /**
     * Read a hex int value from a map.
     *
     * @param map the map
     * @param key the key
     * @param defaultValue if the value is null
     * @return the parsed value
     * @throws IllegalStateException if parsing fails
     */
    public static int readHexInt(HashMap<String, ? extends Object> map, String key, int defaultValue) {
        Object v = map.get(key);
        if (v == null) {
            return defaultValue;
        } else if (v instanceof Integer) {
            return (Integer) v;
        }
        try {
            // support unsigned hex value
            return (int) Long.parseLong((String) v, 16);
        } catch (NumberFormatException e) {
            throw newIllegalStateException(ERROR_FILE_CORRUPT,
                    "Error parsing the value {0}", v, e);
        }
    }

    /**
     * An entry of a map.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    public static class MapEntry<K, V> implements Map.Entry<K, V> {

        private final K key;
        private V value;

        public MapEntry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            throw DataUtils.newUnsupportedOperationException(
                    "Updating the value is not supported");
        }

    }

}
