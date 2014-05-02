/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.fs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * A standalone archive tool to compress directories. It does not have any
 * dependencies except for the Java libraries.
 */
public class ArchiveTool {

    /**
     * The file header.
     */
    private static final byte[] HEADER = {'H', '2', 'A', '1'};

    /**
     * The number of bytes per megabyte (used for the output).
     */
    private static final int MB = 1000 * 1000;

    /**
     * Run the tool.
     *
     * @param args the command line arguments
     */
    public static void main(String... args) throws Exception {
        String arg = args.length != 3 ? null : args[0];
        if ("-compress".equals(arg)) {
            String toFile = args[1];
            String fromDir = args[2];
            compress(fromDir, toFile);
        } else if ("-extract".equals(arg)) {
            String fromFile = args[1];
            String toDir = args[2];
            extract(fromFile, toDir);
        } else {
            System.out.println("An archive tool to efficiently compress large directories");
            System.out.println("Command line options:");
            System.out.println("-compress <file> <sourceDir>");
            System.out.println("-extract <file> <targetDir>");
        }
    }

    private static void compress(String fromDir, String toFile) throws IOException {
        long start = System.currentTimeMillis();
        long size = getSize(new File(fromDir));
        System.out.println("Compressing " + size / MB + " MB");
            InputStream in = getDirectoryInputStream(fromDir);
            String temp = toFile + ".temp";
            OutputStream out =
                    new BufferedOutputStream(
                                    new FileOutputStream(toFile), 32 * 1024);
            Deflater def = new Deflater();
            // def.setLevel(Deflater.BEST_SPEED);
            out = new BufferedOutputStream(
                    new DeflaterOutputStream(out, def));
            sort(in, out, temp, size);
            in.close();
            out.close();
            System.out.println();
            System.out.println("Compressed to " +
                    new File(toFile).length() / MB + " MB in " +
                    (System.currentTimeMillis() - start) / 1000 +
                    " seconds");
            System.out.println();
    }

    private static void extract(String fromFile, String toDir) throws IOException {
        long start = System.currentTimeMillis();
        long size = new File(fromFile).length();
        System.out.println("Extracting " + size / MB + " MB");
        InputStream in =
                new BufferedInputStream(
                        new FileInputStream(fromFile));
        String temp = fromFile + ".temp";
        in = new InflaterInputStream(in);
        OutputStream out = getDirectoryOutputStream(toDir);
        combine(in, out, temp);
        in.close();
        out.close();
        System.out.println();
        System.out.println("Extracted in " +
                (System.currentTimeMillis() - start) / 1000 +
                " seconds");
    }

    private static long getSize(File f) {
        // assume a metadata entry is 40 bytes
        long size = 40;
        if (f.isDirectory()) {
            File[] list = f.listFiles();
            if (list != null) {
                for (File c : list) {
                    size += getSize(c);
                }
            }
        } else {
            size += f.length();
        }
        return size;
    }

    private static InputStream getDirectoryInputStream(final String dir) {

        File f = new File(dir);
        if (!f.isDirectory() || !f.exists()) {
            throw new IllegalArgumentException("Not an existing directory: " + dir);
        }

        return new InputStream() {

            private final String baseDir;
            private final LinkedList<String> files = new LinkedList<String>();
            private String current;
            private ByteArrayInputStream meta;
            private DataInputStream fileIn;
            private long remaining;

            {
                File f = new File(dir);
                baseDir = f.getAbsolutePath();
                addDirectory(f);
            }

            private void addDirectory(File f) {
                File[] list = f.listFiles();
                // breadth-first traversal
                // first all files, then all directories
                if (list != null) {
                    for (File c : list) {
                        if (c.isFile()) {
                            files.add(c.getAbsolutePath());
                        }
                    }
                    for (File c : list) {
                        if (c.isDirectory()) {
                            files.add(c.getAbsolutePath());
                        }
                    }
                }
            }

            // int: metadata length
            // byte: 0: directory, 1: file
            // varLong: lastModified
            // byte: 0: read-write, 1: read-only
            // (file only) varLong: file length
            // utf-8: file name

            @Override
            public int read() throws IOException {
                if (meta != null) {
                    // read from the metadata
                    int x = meta.read();
                    if (x >= 0) {
                        return x;
                    }
                    meta = null;
                }
                if (fileIn != null) {
                    if (remaining > 0) {
                        // read from the file
                        int x = fileIn.read();
                        remaining--;
                        if (x < 0) {
                            throw new EOFException();
                        }
                        return x;
                    }
                    fileIn.close();
                    fileIn = null;
                }
                if (files.size() == 0) {
                    // EOF
                    return -1;
                }
                // fetch the next file or directory
                current = files.remove();
                File f = new File(current);
                if (f.isDirectory()) {
                    addDirectory(f);
                }
                ByteArrayOutputStream metaOut = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(metaOut);
                boolean isFile = f.isFile();
                out.writeInt(0);
                out.write(isFile ? 1 : 0);
                out.write(!f.canWrite() ? 1 : 0);
                writeVarLong(out, f.lastModified());
                if (isFile) {
                    remaining = f.length();
                    writeVarLong(out, remaining);
                    fileIn = new DataInputStream(new BufferedInputStream(
                            new FileInputStream(current)));
                }
                if (!current.startsWith(baseDir)) {
                    throw new IOException("File " + current + " does not start with " + baseDir);
                }
                String n = current.substring(baseDir.length() + 1);
                out.writeUTF(n);
                out.writeInt(metaOut.size());
                out.flush();
                byte[] bytes = metaOut.toByteArray();
                // copy metadata length to beginning
                System.arraycopy(bytes, bytes.length - 4, bytes, 0, 4);
                // cut the length
                bytes = Arrays.copyOf(bytes, bytes.length - 4);
                meta = new ByteArrayInputStream(bytes);
                return meta.read();
            }

            @Override
            public int read(byte[] buff, int offset, int length) throws IOException {
                if (meta != null || fileIn == null || remaining == 0) {
                    return super.read(buff, offset, length);
                }
                int l = (int) Math.min(length, remaining);
                fileIn.readFully(buff, offset, l);
                remaining -= l;
                return l;
            }

        };

    }

    private static OutputStream getDirectoryOutputStream(final String dir) {
        new File(dir).mkdirs();
        return new OutputStream() {

            private ByteArrayOutputStream meta = new ByteArrayOutputStream();
            private OutputStream fileOut;
            private File file;
            private long remaining = 4;
            private long modified;
            private boolean readOnly;

            @Override
            public void write(byte[] buff, int offset, int length) throws IOException {
                while (length > 0) {
                    if (fileOut == null || remaining <= 1) {
                        write(buff[offset] & 255);
                        offset++;
                        length--;
                    } else {
                        int l = (int) Math.min(length, remaining - 1);
                        fileOut.write(buff, offset, l);
                        remaining -= l;
                        offset += l;
                        length -= l;
                    }
                }
            }

            @Override
            public void write(int b) throws IOException {
                if (fileOut != null) {
                    fileOut.write(b);
                    if (--remaining > 0) {
                        return;
                    }
                    // this can be slow, but I don't know a way to avoid it
                    fileOut.close();
                    fileOut = null;
                    file.setLastModified(modified);
                    if (readOnly) {
                        file.setReadOnly();
                    }
                    remaining = 4;
                    return;
                }
                meta.write(b);
                if (--remaining > 0) {
                    return;
                }
                DataInputStream in = new DataInputStream(
                        new ByteArrayInputStream(meta.toByteArray()));
                if (meta.size() == 4) {
                    // metadata is next
                    remaining = in.readInt() - 4;
                    if (remaining > 16 * 1024) {
                        throw new IOException("Illegal directory stream");
                    }
                    return;
                }
                // read and ignore the length
                in.readInt();
                boolean isFile = in.read() == 1;
                readOnly = in.read() == 1;
                modified = readVarLong(in);
                if (isFile) {
                    remaining = readVarLong(in);
                } else {
                    remaining = 4;
                }
                String name = dir + "/" + in.readUTF();
                file = new File(name);
                if (isFile) {
                    if (remaining == 0) {
                        new File(name).createNewFile();
                        remaining = 4;
                    } else {
                        fileOut = new BufferedOutputStream(new FileOutputStream(name));
                    }
                } else {
                    file.mkdirs();
                    file.setLastModified(modified);
                    if (readOnly) {
                        file.setReadOnly();
                    }
                }
                meta.reset();
            }
        };
    }

    private static void sort(InputStream in, OutputStream out,
            String tempFileName, long size) throws IOException {
        long lastTime = System.currentTimeMillis();
        int bufferSize = 16 * 1024 * 1024;
        DataOutputStream tempOut = new DataOutputStream(new BufferedOutputStream(
                new FileOutputStream(tempFileName)));
        byte[] bytes = new byte[bufferSize];
        ArrayList<Long> segmentStart = new ArrayList<Long>();
        long inPos = 0;
        long outPos = 0;
        long id = 1;

        // Temp file: segment* 0
        // Segment: chunk* 0
        // Chunk: pos* 0 sortKey data

        while (true) {
            int len = readFully(in, bytes, bytes.length);
            if (len == 0) {
                break;
            }
            inPos += len;
            lastTime = printProgress(lastTime, 0, 50, inPos, size);
            TreeMap<Chunk, Chunk> map = new TreeMap<Chunk, Chunk>();
            for (int pos = 0; pos < len;) {
                int[] key = getKey(bytes, pos, len);
                int l = key[3];
                byte[] buff = new byte[l];
                System.arraycopy(bytes, pos, buff, 0, l);
                pos += l;
                Chunk c = new Chunk(null, key, buff);
                Chunk old = map.get(c);
                if (old == null) {
                    // new entry
                    c.idList = new ArrayList<Long>();
                    c.idList.add(id);
                    map.put(c, c);
                } else {
                    old.idList.add(id);
                }
                id++;
            }
            segmentStart.add(outPos);
            for (Chunk c : map.keySet()) {
                outPos += c.write(tempOut, true);
            }
            // end of segment
            outPos += writeVarLong(tempOut, 0);
        }
        tempOut.close();
        size = outPos;
        inPos = 0;
        ArrayList<ChunkStream> segmentIn = new ArrayList<ChunkStream>();
        for (int i = 0; i < segmentStart.size(); i++) {
            in = new FileInputStream(tempFileName);
            in.skip(segmentStart.get(i));
            ChunkStream s = new ChunkStream();
            s.readKey = true;
            s.in = new DataInputStream(new BufferedInputStream(in));
            inPos += s.readNext();
            if (s.current != null) {
                segmentIn.add(s);
            }
        }

        DataOutputStream dataOut = new DataOutputStream(out);
        dataOut.write(HEADER);
        writeVarLong(dataOut, size);
        Chunk last = null;

        // File: header length chunk* 0
        // chunk: pos* 0 data

        while (segmentIn.size() > 0) {
            Collections.sort(segmentIn);
            ChunkStream s = segmentIn.get(0);
            Chunk c = s.current;
            if (last == null) {
                last = c;
            } else if (last.compareTo(c) == 0) {
                for (long x : c.idList) {
                    last.idList.add(x);
                }
            } else {
                last.write(dataOut, false);
                last = c;
            }
            inPos += s.readNext();
            lastTime = printProgress(lastTime, 50, 100, inPos, size);
            if (s.current == null) {
                segmentIn.remove(0);
            }
        }
        if (last != null) {
            last.write(dataOut, false);
        }
        new File(tempFileName).delete();
        writeVarLong(dataOut, 0);
        dataOut.flush();
    }

    /**
     * Read a number of bytes. This method repeats reading until
     * either the bytes have been read, or EOF.
     *
     * @param in the input stream
     * @param buffer the target buffer
     * @param max the number of bytes to read
     * @return the number of bytes read (max unless EOF has been reached)
     */
    private static int readFully(InputStream in, byte[] buffer, int max)
            throws IOException {
        int result = 0, len = Math.min(max, buffer.length);
        while (len > 0) {
            int l = in.read(buffer, result, len);
            if (l < 0) {
                break;
            }
            result += l;
            len -= l;
        }
        return result;
    }

    /**
     * Get the sort key and length of a chunk.
     */
    private static int[] getKey(byte[] data, int start, int maxPos) {
        int minLen = 4 * 1024;
        int mask = 4 * 1024 - 1;
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        int pos = start;
        long bytes = 0;
        for (int j = 0; pos < maxPos; pos++, j++) {
            bytes = (bytes << 8) | (data[pos] & 255);
            int hash = getHash(bytes);
            if (hash < min) {
                min = hash;
            }
            if (hash > max) {
                max = hash;
            }
            if (j > minLen) {
                if ((hash & mask) == 1) {
                    break;
                }
                if (j > minLen * 4 && (hash & (mask >> 1)) == 1) {
                    break;
                }
                if (j > minLen * 16) {
                    break;
                }
            }
        }
        int len = pos - start;
        int[] counts = new int[8];
        for (int i = start; i < pos; i++) {
            int x = data[i] & 0xff;
            counts[x >> 5]++;
        }
        int cs = 0;
        for (int i = 0; i < 8; i++) {
            cs *= 2;
            if (counts[i] > (len / 32)) {
                cs += 1;
            }
        }
        int[] key = new int[4];
        key[0] = cs;
        key[1] = min;
        key[2] = max;
        key[3] = len;
        return key;
    }

    private static int getHash(long key) {
        int hash = (int) ((key >>> 32) ^ key);
        hash = ((hash >>> 16) ^ hash) * 0x45d9f3b;
        hash = ((hash >>> 16) ^ hash) * 0x45d9f3b;
        hash = (hash >>> 16) ^ hash;
        return hash;
    }

    private static void combine(InputStream in, OutputStream out,
            String tempFileName) throws IOException {
        long lastTime = System.currentTimeMillis();
        int bufferSize = 16 * 1024 * 1024;
        DataOutputStream tempOut =
                new DataOutputStream(
                        new BufferedOutputStream(
                                new FileOutputStream(tempFileName)));

        // File: header length chunk* 0
        // chunk: pos* 0 data

        DataInputStream dataIn = new DataInputStream(in);
        byte[] header = new byte[4];
        dataIn.readFully(header);
        if (!Arrays.equals(header, HEADER)) {
            throw new IOException("Invalid header");
        }
        long size = readVarLong(dataIn);
        long outPos = 0;
        long inPos = 0;
        ArrayList<Long> segmentStart = new ArrayList<Long>();
        boolean end = false;

        // Temp file: segment* 0
        // Segment: chunk* 0
        // Chunk: pos* 0 data

        while (!end) {
            int segmentSize = 0;
            TreeMap<Long, byte[]> map = new TreeMap<Long, byte[]>();
            while (segmentSize < bufferSize) {
                Chunk c = Chunk.read(dataIn, false);
                if (c == null) {
                    end = true;
                    break;
                }
                int length = c.value.length;
                inPos += length;
                lastTime = printProgress(lastTime, 0, 50, inPos, size);
                segmentSize += length;
                for (long x : c.idList) {
                    map.put(x, c.value);
                }
            }
            if (map.size() == 0) {
                break;
            }
            segmentStart.add(outPos);
            for (Long x : map.keySet()) {
                outPos += writeVarLong(tempOut, x);
                outPos += writeVarLong(tempOut, 0);
                byte[] v = map.get(x);
                outPos += writeVarLong(tempOut, v.length);
                tempOut.write(v);
                outPos += v.length;
            }
            outPos += writeVarLong(tempOut, 0);
        }
        tempOut.close();
        size = outPos;
        inPos = 0;
        ArrayList<ChunkStream> segmentIn = new ArrayList<ChunkStream>();
        for (int i = 0; i < segmentStart.size(); i++) {
            FileInputStream f = new FileInputStream(tempFileName);
            f.skip(segmentStart.get(i));
            ChunkStream s = new ChunkStream();
            s.in = new DataInputStream(new BufferedInputStream(f));
            inPos += s.readNext();
            if (s.current != null) {
                segmentIn.add(s);
            }
        }
        DataOutputStream dataOut = new DataOutputStream(out);
        while (segmentIn.size() > 0) {
            Collections.sort(segmentIn);
            ChunkStream s = segmentIn.get(0);
            Chunk c = s.current;
            dataOut.write(c.value);
            inPos += s.readNext();
            lastTime = printProgress(lastTime, 50, 100, inPos, size);
            if (s.current == null) {
                segmentIn.remove(0);
            }
        }
        new File(tempFileName).delete();
        dataOut.flush();
    }

    /**
     * A stream of chunks.
     */
    static class ChunkStream implements Comparable<ChunkStream> {
        Chunk current;
        DataInputStream in;
        boolean readKey;

        /**
         * Read the next chunk.
         *
         * @return the number of bytes read
         */
        int readNext() throws IOException {
            current = Chunk.read(in, readKey);
            if (current == null) {
                return 0;
            }
            return current.value.length;
        }

        @Override
        public int compareTo(ChunkStream o) {
            return current.compareTo(o.current);
        }
    }

    /**
     * A chunk of data.
     */
    static class Chunk implements Comparable<Chunk> {
        ArrayList<Long> idList;
        final byte[] value;
        private int[] sortKey;

        Chunk(ArrayList<Long> idList, int[] sortKey, byte[] value) {
            this.idList = idList;
            this.sortKey = sortKey;
            this.value = value;
        }

        /**
         * Read a chunk.
         *
         * @param in the input stream
         * @param readKey whether to read the sort key
         * @return the chunk, or null if 0 has been read
         */
        public static Chunk read(DataInputStream in, boolean readKey) throws IOException {
            ArrayList<Long> idList = new ArrayList<Long>();
            while (true) {
                long x = readVarLong(in);
                if (x == 0) {
                    break;
                }
                idList.add(x);
            }
            if (idList.size() == 0) {
                // eof
                return null;
            }
            int[] key = null;
            if (readKey) {
                key = new int[4];
                for (int i = 0; i < key.length; i++) {
                    key[i] = in.readInt();
                }
            }
            int len = (int) readVarLong(in);
            byte[] value = new byte[len];
            in.readFully(value);
            return new Chunk(idList, key, value);
        }

        /**
         * Write a chunk.
         *
         * @param out the output stream
         * @param writeKey whether to write the sort key
         * @return the number of bytes written
         */
        int write(DataOutputStream out, boolean writeKey) throws IOException {
            int len = 0;
            for (long x : idList) {
                len += writeVarLong(out, x);
            }
            len += writeVarLong(out, 0);
            if (writeKey) {
                for (int i = 0; i < 4; i++) {
                    out.writeInt(sortKey[i]);
                    len += 4;
                }
            }
            len += writeVarLong(out, value.length);
            out.write(value);
            len += value.length;
            return len;
        }

        @Override
        public int compareTo(Chunk o) {
            if (sortKey == null) {
                // sort by id
                long a = idList.get(0);
                long b = o.idList.get(0);
                if (a < b) {
                    return -1;
                } else if (a > b) {
                    return 1;
                }
            }
            for (int i = 0; i < sortKey.length; i++) {
                if (sortKey[i] < o.sortKey[i]) {
                    return -1;
                } else if (sortKey[i] > o.sortKey[i]) {
                    return 1;
                }
            }
            if (value.length < o.value.length) {
                return -1;
            } else if (value.length > o.value.length) {
                return 1;
            }
            for (int i = 0; i < value.length; i++) {
                int a = value[i] & 255;
                int b = o.value[i] & 255;
                if (a < b) {
                    return -1;
                } else if (a > b) {
                    return 1;
                }
            }
            return 0;
        }
    }

    /**
     * Write a variable size long value.
     *
     * @param out the output stream
     * @param x the value
     * @return the number of bytes written
     */
    static int writeVarLong(OutputStream out, long x)
            throws IOException {
        int len = 0;
        while ((x & ~0x7f) != 0) {
            out.write((byte) (0x80 | (x & 0x7f)));
            x >>>= 7;
            len++;
        }
        out.write((byte) x);
        return ++len;
    }

    /**
     * Read a variable size long value.
     *
     * @param in the input stream
     * @return the value
     */
    static long readVarLong(InputStream in) throws IOException {
        long x = in.read();
        if (x < 0) {
            throw new EOFException();
        }
        x = (byte) x;
        if (x >= 0) {
            return x;
        }
        x &= 0x7f;
        for (int s = 7; s < 64; s += 7) {
            long b = in.read();
            if (b < 0) {
                throw new EOFException();
            }
            b = (byte) b;
            x |= (b & 0x7f) << s;
            if (b >= 0) {
                break;
            }
        }
        return x;
    }

    private static long printProgress(long lastTime, int low, int high,
            long current, long total) {
        long now = System.currentTimeMillis();
        if (now - lastTime > 3000) {
            System.out.print((low + (high - low) * current / total) + "% ");
            lastTime = now;
        }
        return lastTime;
    }

}
