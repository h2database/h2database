/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.hash;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * A minimal perfect hash function tool. It needs about 2.0 bits per key.
 * <p>
 * Generating the hash function takes about 2.5 second per million keys with 8
 * cores (multithreaded).
 * <p>
 * The algorithm is recursive: sets that contain no or only one entry are not
 * processed as no conflicts are possible. Sets that contain between 2 and 12
 * entries, a number of hash functions are tested to check if they can store the
 * data without conflict. If no function was found, and for larger sets, the set
 * is split into a (possibly high) number of smaller set, which are processed
 * recursively.
 * <p>
 * At the end of the generation process, the data is compressed using a general
 * purpose compression tool (Deflate / Huffman coding). The uncompressed data is
 * around 2.2 bits per key. With arithmetic coding, about 1.9 bits per key are
 * needed.
 * <p>
 * The algorithm automatically scales with the number of available CPUs (using
 * as many threads as there are processors).
 * <p>
 * At the expense of processing time, a lower number of bits per key would be
 * possible (for example 1.85 bits per key with 33000 keys, using 10 seconds
 * generation time, with Huffman coding).
 * <p>
 * In-place updating of the hash table is possible in theory, by patching the
 * hash function description. This is not implemented.
 */
public class MinimalPerfectHash {

    /**
     * Large buckets are typically divided into buckets of this size.
     */
    private static final int DIVIDE = 6;

    /**
     * The maximum size of a small bucket (one that is not further split if
     * possible).
     */
    private static final int MAX_SIZE = 12;

    /**
     * The maximum offset for hash functions of small buckets. At most that many
     * hash functions are tried for the given size.
     */
    private static final int[] MAX_OFFSETS = { 0, 0, 8, 18, 47, 123, 319, 831, 2162,
            5622, 14617, 38006, 38006 };

    /**
     * The output value to split the bucket into many (more than 2) smaller
     * buckets.
     */
    private static final int SPLIT_MANY = 3;

    /**
     * The minimum output value for a small bucket of a given size.
     */
    private static final int[] SIZE_OFFSETS = new int[MAX_OFFSETS.length + 1];

    static {
        int last = SPLIT_MANY + 1;
        for (int i = 0; i < MAX_OFFSETS.length; i++) {
            SIZE_OFFSETS[i] = last;
            last += MAX_OFFSETS[i];
        }
        SIZE_OFFSETS[SIZE_OFFSETS.length - 1] = last;
    }

    /**
     * The description of the hash function. Used for calculating the hash of a
     * key.
     */
    private final byte[] data;

    /**
     * The offset of the result of the hash function at the given offset within
     * the data array. Used for calculating the hash of a key.
     */
    private final int[] plus;

    /**
     * The position of the given top-level bucket in the data array (in case
     * this bucket needs to be skipped). Used for calculating the hash of a key.
     */
    private final int[] topPos;

    /**
     * Create a hash object to convert keys to hashes.
     *
     * @param desc the data returned by the generate method
     */
    public MinimalPerfectHash(byte[] desc) {
        byte[] b = data = expand(desc);
        plus = new int[data.length];
        for (int pos = 0, p = 0; pos < data.length;) {
            plus[pos] = p;
            int n = readVarInt(b, pos);
            pos += getVarIntLength(b, pos);
            if (n < 2) {
                p += n;
            } else if (n > SPLIT_MANY) {
                int size = getSize(n);
                p += size;
            } else if (n == SPLIT_MANY) {
                pos += getVarIntLength(b, pos);
            }
        }
        if (b[0] == SPLIT_MANY) {
            int split = readVarInt(b, 1);
            topPos = new int[split];
            int pos = 1 + getVarIntLength(b, 1);
            for (int i = 0; i < split; i++) {
                topPos[i] = pos;
                pos = read(pos);
            }
        } else {
            topPos = null;
        }
    }

    /**
     * Calculate the hash from the key.
     *
     * @param x the key
     * @return the hash
     */
    public int get(int x) {
        return get(0, x, 0);
    }

    private int get(int pos, int x, int level) {
        int n = readVarInt(data, pos);
        if (n < 2) {
            return plus[pos];
        } else if (n > SPLIT_MANY) {
            int size = getSize(n);
            int offset = getOffset(n, size);
            return plus[pos] + hash(x, level, offset, size);
        }
        pos++;
        int split;
        if (n == SPLIT_MANY) {
            split = readVarInt(data, pos);
            pos += getVarIntLength(data, pos);
        } else {
            split = n;
        }
        int h = hash(x, level, 0, split);
        if (level == 0 && topPos != null) {
            pos = topPos[h];
        } else {
            for (int i = 0; i < h; i++) {
                pos = read(pos);
            }
        }
        return get(pos, x, level + 1);
    }

    private static void writeSizeOffset(ByteArrayOutputStream out, int size,
            int offset) {
        writeVarInt(out, SIZE_OFFSETS[size] + offset);
    }

    private static int getOffset(int n, int size) {
        return n - SIZE_OFFSETS[size];
    }

    private static int getSize(int n) {
        for (int i = 0; i < SIZE_OFFSETS.length; i++) {
            if (n < SIZE_OFFSETS[i]) {
                return i - 1;
            }
        }
        return 0;
    }

    private int read(int pos) {
        int n = readVarInt(data, pos);
        pos += getVarIntLength(data, pos);
        if (n < 2 || n > SPLIT_MANY) {
            return pos;
        }
        int split;
        if (n == SPLIT_MANY) {
            split = readVarInt(data, pos);
            pos += getVarIntLength(data, pos);
        } else {
            split = n;
        }
        for (int i = 0; i < split; i++) {
            pos = read(pos);
        }
        return pos;
    }

    /**
     * Generate the minimal perfect hash function data from the given set of
     * integers.
     *
     * @param set the data
     * @return the hash function description
     */
    public static byte[] generate(Set<Integer> set) {
        ArrayList<Integer> list = new ArrayList<Integer>();
        list.addAll(set);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        generate(list, 0, out);
        return compress(out.toByteArray());
    }

    /**
     * Generate the perfect hash function data from the given set of integers.
     *
     * @param list the data, in the form of a list
     * @param level the recursion level
     * @param out the output stream
     */
    static void generate(ArrayList<Integer> list, int level,
            ByteArrayOutputStream out) {
        int size = list.size();
        if (size <= 1) {
            writeVarInt(out, size);
            return;
        }
        if (size <= MAX_SIZE) {
            int maxOffset = MAX_OFFSETS[size];
            nextOffset:
            for (int offset = 0; offset < maxOffset; offset++) {
                int bits = 0;
                for (int i = 0; i < size; i++) {
                    int x = list.get(i);
                    int h = hash(x, level, offset, size);
                    if ((bits & (1 << h)) != 0) {
                        continue nextOffset;
                    }
                    bits |= 1 << h;
                }
                writeSizeOffset(out, size, offset);
                return;
            }
        }
        int split;
        if (size > 57 * DIVIDE) {
            split = size / (36 * DIVIDE);
        } else {
            split = (size - 47) / DIVIDE;
        }
        split = Math.max(2, split);
        if (split >= SPLIT_MANY) {
            writeVarInt(out, SPLIT_MANY);
        }
        writeVarInt(out, split);
        ArrayList<ArrayList<Integer>> lists =
                new ArrayList<ArrayList<Integer>>(split);
        for (int i = 0; i < split; i++) {
            lists.add(new ArrayList<Integer>(size / split));
        }
        for (int i = 0; i < size; i++) {
            int x = list.get(i);
            lists.get(hash(x, level, 0, split)).add(x);
        }
        boolean multiThreaded = level == 0 && list.size() > 1000;
        list.clear();
        list.trimToSize();
        if (multiThreaded) {
            generateMultiThreaded(lists, out);
        } else {
            for (ArrayList<Integer> s2 : lists) {
                generate(s2, level + 1, out);
            }
        }
    }

    private static void generateMultiThreaded(
            final ArrayList<ArrayList<Integer>> lists,
            ByteArrayOutputStream out) {
        final ArrayList<ByteArrayOutputStream> outList =
                new ArrayList<ByteArrayOutputStream>();
        int processors = Runtime.getRuntime().availableProcessors();
        Thread[] threads = new Thread[processors];
        for (int i = 0; i < processors; i++) {
            threads[i] = new Thread() {
                @Override
                public void run() {
                    while (true) {
                        ArrayList<Integer> list;
                        ByteArrayOutputStream temp =
                                new ByteArrayOutputStream();
                        synchronized (lists) {
                            if (lists.isEmpty()) {
                                break;
                            }
                            list = lists.remove(0);
                            outList.add(temp);
                        }
                        generate(list, 1, temp);
                    }
                }
            };
        }
        for (Thread t : threads) {
            t.start();
        }
        try {
            for (Thread t : threads) {
                t.join();
            }
            for (ByteArrayOutputStream temp : outList) {
                out.write(temp.toByteArray());
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Calculate the hash of a key. The result depends on the key, the recursion
     * level, and the offset.
     *
     * @param x the key
     * @param level the recursion level
     * @param offset the index of the hash function
     * @param size the size of the bucket
     * @return the hash (a value between 0, including, and the size, excluding)
     */
    private static int hash(int x, int level, int offset, int size) {
        x += level * 16 + offset;
        x = ((x >>> 16) ^ x) * 0x45d9f3b;
        x = ((x >>> 16) ^ x) * 0x45d9f3b;
        x = (x >>> 16) ^ x;
        return Math.abs(x % size);
    }

    private static int writeVarInt(ByteArrayOutputStream out, int x) {
        int len = 0;
        while ((x & ~0x7f) != 0) {
            out.write((byte) (0x80 | (x & 0x7f)));
            x >>>= 7;
            len++;
        }
        out.write((byte) x);
        return ++len;
    }

    private static int readVarInt(byte[] d, int pos) {
        int x = d[pos++];
        if (x >= 0) {
            return x;
        }
        x &= 0x7f;
        for (int s = 7; s < 64; s += 7) {
            int b = d[pos++];
            x |= (b & 0x7f) << s;
            if (b >= 0) {
                break;
            }
        }
        return x;
    }

    private static int getVarIntLength(byte[] d, int pos) {
        int x = d[pos++];
        if (x >= 0) {
            return 1;
        }
        int len = 2;
        for (int s = 7; s < 64; s += 7) {
            int b = d[pos++];
            if (b >= 0) {
                break;
            }
            len++;
        }
        return len;
    }

    /**
     * Compress the hash description using a Huffman coding.
     *
     * @param d the data
     * @return the compressed data
     */
    private static byte[] compress(byte[] d) {
        Deflater deflater = new Deflater();
        deflater.setStrategy(Deflater.HUFFMAN_ONLY);
        deflater.setInput(d);
        deflater.finish();
        ByteArrayOutputStream out2 = new ByteArrayOutputStream(d.length);
        byte[] buffer = new byte[1024];
        while (!deflater.finished()) {
            int count = deflater.deflate(buffer);
            out2.write(buffer, 0, count);
        }
        deflater.end();
        return out2.toByteArray();
    }

    /**
     * Decompress the hash description using a Huffman coding.
     *
     * @param d the data
     * @return the decompressed data
     */
    private static byte[] expand(byte[] d) {
        Inflater inflater = new Inflater();
        inflater.setInput(d);
        ByteArrayOutputStream out = new ByteArrayOutputStream(d.length);
        byte[] buffer = new byte[1024];
        try {
            while (!inflater.finished()) {
                int count = inflater.inflate(buffer);
                out.write(buffer, 0, count);
            }
            inflater.end();
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
        return out.toByteArray();
    }

}
