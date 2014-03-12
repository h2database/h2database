/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.fs;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;

import org.h2.mvstore.Cursor;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.store.fs.FileUtils;
import org.h2.util.New;

/**
 * An archive tool that de-duplicates and compresses files.
 */
public class Dedup {

    private static final int[] RANDOM = new int[256];
    private static final int MB = 1024 * 1024;
    private long lastTime;
    private long start;
    private int bucket;
    private String fileName;

    static {
        Random r = new Random(1);
        for (int i = 0; i < RANDOM.length; i++) {
            RANDOM[i] = r.nextInt();
        }
    }

    /**
     * Run the tool.
     *
     * @param args the command line arguments
     */
    public static void main(String... args) throws Exception {
        Dedup app = new Dedup();
        String arg = args.length != 3 ? null : args[0];
        if ("-compress".equals(arg)) {
            app.fileName = args[1];
            app.compress(args[2]);
        } else if ("-extract".equals(arg)) {
            app.fileName = args[1];
            app.expand(args[2]);
        } else {
            System.out.println("Command line options:");
            System.out.println("-compress <file> <sourceDir>");
            System.out.println("-extract <file> <targetDir>");
        }
    }

    private void compress(String sourceDir) throws Exception {
        start();
        long tempSize = 8 * 1024 * 1024;
        String tempFileName = fileName + ".temp";
        ArrayList<String> fileNames = New.arrayList();

        System.out.println("Reading the file list");
        long totalSize = addFiles(sourceDir, fileNames);
        System.out.println("Compressing " + totalSize / MB + " MB");

        FileUtils.delete(tempFileName);
        FileUtils.delete(fileName);
        MVStore storeTemp = new MVStore.Builder().
                fileName(tempFileName).
                compress().
                autoCommitDisabled().
                open();
        MVStore store = new MVStore.Builder().
                fileName(fileName).
                pageSplitSize(2 * 1024 * 1024).
                compressHigh().
                autoCommitDisabled().
                open();
        MVMap<String, int[]> files = store.openMap("files");
        long currentSize = 0;
        int segmentId = 1;
        int segmentLength = 0;
        ByteBuffer buff = ByteBuffer.allocate(1024 * 1024);
        for (String s : fileNames) {
            String name = s.substring(sourceDir.length() + 1);
            if (FileUtils.isDirectory(s)) {
                // directory
                files.put(name, new int[1]);
                continue;
            }
            buff.clear();
            buff.flip();
            ArrayList<Integer> posList = new ArrayList<Integer>();
            FileChannel fc = FileUtils.open(s, "r");
            try {
                boolean eof = false;
                while (true) {
                    while (!eof && buff.remaining() < 512 * 1024) {
                        int remaining = buff.remaining();
                        buff.compact();
                        buff.position(remaining);
                        int l = fc.read(buff);
                        if (l < 0) {
                            eof = true;
                        }
                        buff.flip();
                    }
                    if (buff.remaining() == 0) {
                        break;
                    }
                    int c = getChunkLength(buff.array(), buff.position(),
                            buff.limit()) - buff.position();
                    byte[] bytes = new byte[c];
                    System.arraycopy(buff.array(), buff.position(), bytes, 0, c);
                    buff.position(buff.position() + c);
                    int[] key = getKey(bucket, bytes);
                    key[3] = segmentId;
                    while (true) {
                        MVMap<int[], byte[]> data = storeTemp.
                                openMap("data" + segmentId);
                        byte[] old = data.get(key);
                        if (old == null) {
                            // new
                            data.put(key, bytes);
                            break;
                        }
                        if (old != null && Arrays.equals(old, bytes)) {
                            // duplicate
                            break;
                        }
                        // same checksum: change checksum
                        key[2]++;
                    }
                    for (int i = 0; i < key.length; i++) {
                        posList.add(key[i]);
                    }
                    segmentLength += c;
                    currentSize += c;
                    if (segmentLength > tempSize) {
                        storeTemp.commit();
                        segmentId++;
                        segmentLength = 0;
                    }
                    printProgress("Stage 1/2", currentSize, totalSize);
                }
            } finally {
                fc.close();
            }
            int[] posArray = new int[posList.size()];
            for (int i = 0; i < posList.size(); i++) {
                posArray[i] = posList.get(i);
            }
            files.put(name, posArray);
        }
        storeTemp.commit();
        store.commit();
        ArrayList<Cursor<int[], byte[]>> list = New.arrayList();
        totalSize = 0;
        for (int i = 1; i <= segmentId; i++) {
            MVMap<int[], byte[]> data = storeTemp.openMap("data" + i);
            totalSize += data.sizeAsLong();
            Cursor<int[], byte[]> c = data.cursor(null);
            if (c.hasNext()) {
                c.next();
                list.add(c);
            }
        }
        segmentId = 1;
        segmentLength = 0;
        currentSize = 0;
        TreeMap<Integer, int[]> ranges = new TreeMap<Integer, int[]>();
        MVMap<int[], byte[]> data = store.openMap("data" + segmentId);
        while (list.size() > 0) {
            Collections.sort(list, new Comparator<Cursor<int[], byte[]>>() {

                @Override
                public int compare(Cursor<int[], byte[]> o1,
                        Cursor<int[], byte[]> o2) {
                    int[] k1 = o1.getKey();
                    int[] k2 = o2.getKey();
                    int comp = 0;
                    for (int i = 0; i < k1.length - 1; i++) {
                        long x1 = k1[i];
                        long x2 = k2[i];
                        if (x1 > x2) {
                            comp = 1;
                            break;
                        } else if (x1 < x2) {
                            comp = -1;
                            break;
                        }
                    }
                    return comp;
                }

            });
            Cursor<int[], byte[]> top = list.get(0);
            int[] key = top.getKey();
            byte[] bytes = top.getValue();
            int[] k2 = Arrays.copyOf(key, key.length);
            k2[key.length - 1] = 0;
            byte[] old = data.get(k2);
            if (old == null) {
                key = k2;
                // new entry
                if (segmentLength > tempSize) {
                    // switch only for new entries
                    // where segmentId is 0,
                    // so that entries with the same
                    // key but different segmentId
                    // are in the same segment
                    store.commit();
                    segmentLength = 0;
                    segmentId++;
                    ranges.put(segmentId, key);
                    data = store.openMap("data" + segmentId);
                }
                data.put(key, bytes);
                segmentLength += bytes.length;
            } else if (Arrays.equals(old, bytes)) {
                // duplicate
            } else {
                // keep segment id
                data.put(key, bytes);
                segmentLength += bytes.length;
            }
            if (!top.hasNext()) {
                list.remove(0);
            } else {
                top.next();
            }
            currentSize++;
            printProgress("Stage 2/2", currentSize, totalSize);
        }
        MVMap<int[], Integer> rangeMap = store.openMap("ranges");
        for (Entry<Integer, int[]> range : ranges.entrySet()) {
            rangeMap.put(range.getValue(), range.getKey());
        }
        storeTemp.close();
        FileUtils.delete(tempFileName);
        store.close();
        System.out.println("Compressed to " +
                FileUtils.size(fileName) / MB + " MB");
        printDone();
    }

    private void start() {
        this.start = System.currentTimeMillis();
        this.lastTime = start;
    }

    private void printProgress(String message, long current, long total) {
        long now = System.currentTimeMillis();
        if (now - lastTime > 5000) {
            System.out.println(message + ": " +
                    100 * current / total + "%");
            lastTime = now;
        }
    }

    private void printDone() {
        System.out.println("Done in " +
                (System.currentTimeMillis() - start) / 1000 +
                " seconds");
    }

    private static long addFiles(String dir, ArrayList<String> list) {
        long size = 0;
        for (String s : FileUtils.newDirectoryStream(dir)) {
            if (FileUtils.isDirectory(s)) {
                size += addFiles(s, list);
            } else {
                size += FileUtils.size(s);
            }
            list.add(s);
        }
        return size;
    }

    private void expand(String targetDir) throws Exception {
        start();
        String tempFileName = fileName + ".temp";
        long chunkSize = 8 * 1024 * 1024;
        FileUtils.createDirectories(targetDir);
        MVStore store = new MVStore.Builder().
                fileName(fileName).open();
        MVMap<String, int[]> files = store.openMap("files");
        System.out.println("Extracting " + files.size() + " files");
        MVMap<int[], Integer> ranges = store.openMap("ranges");
        int lastSegment;
        if (ranges.size() == 0) {
            lastSegment = 1;
        } else {
            lastSegment = ranges.get(ranges.lastKey());
        }
        MVStore storeTemp = null;
        FileUtils.delete(tempFileName);
        storeTemp = new MVStore.Builder().
                fileName(tempFileName).
                cacheSize(64).
                autoCommitDisabled().
                open();
        MVMap<int[], byte[]> data = storeTemp.openMap("data");
        long chunkLength = 0;
        long totalSize = 0;
        for (int i = 1; i <= lastSegment; i++) {
            MVMap<int[], byte[]> segmentData = store.openMap("data" + i);
            totalSize += segmentData.sizeAsLong();
        }
        long currentSize = 0;
        for (int i = 1; i <= lastSegment; i++) {
            MVMap<int[], byte[]> segmentData = store.openMap("data" + i);
            for (Entry<int[], byte[]> e : segmentData.entrySet()) {
                int[] key = e.getKey();
                byte[] bytes = e.getValue();
                chunkLength += bytes.length;
                data.put(key, bytes);
                if (chunkLength > chunkSize) {
                    storeTemp.commit();
                    chunkLength = 0;
                }
                currentSize++;
                printProgress("Stage 1/2", currentSize, totalSize);
            }
        }
        totalSize = 0;
        for (Entry<String, int[]> e : files.entrySet()) {
            int[] keys = e.getValue();
            totalSize += keys.length / 4;
        }
        currentSize = 0;
        for (Entry<String, int[]> e : files.entrySet()) {
            String f = e.getKey();
            int[] keys = e.getValue();
            String p = FileUtils.getParent(f);
            if (p != null) {
                FileUtils.createDirectories(targetDir + "/" + p);
            }
            String fn = targetDir + "/" + f;
            if (keys.length == 1) {
                // directory
                FileUtils.createDirectory(fn);
                continue;
            }
            OutputStream file = new BufferedOutputStream(new FileOutputStream(fn));
            for (int i = 0; i < keys.length; i += 4) {
                int[] dk = new int[4];
                dk[0] = keys[i];
                dk[1] = keys[i + 1];
                dk[2] = keys[i + 2];
                dk[3] = keys[i + 3];
                byte[] bytes;
                bytes = data.get(dk);
                if (bytes == null) {
                    dk[3] = 0;
                    bytes = data.get(dk);
                }
                file.write(bytes);
                currentSize++;
                printProgress("Stage 2/2", currentSize, totalSize);
            }
            file.close();
        }
        store.close();
        if (storeTemp != null) {
            storeTemp.close();
            FileUtils.delete(tempFileName);
        }
        printDone();
    }

    private int getChunkLength(byte[] data, int start, int maxPos) {
        int minLen = 4 * 1024;
        int mask = 4 * 1024 - 1;
        int factor = 31;
        int hash = 0, mul = 1, offset = 8;
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        int i = start;
        int[] rand = RANDOM;
        for (int j = 0; i < maxPos; i++, j++) {
            hash = hash * factor + rand[data[i] & 255];
            if (j >= offset) {
                hash -= mul * rand[data[i - offset] & 255];
            } else {
                mul *= factor;
            }
            if (hash < min) {
                min = hash;
            }
            if (hash > max) {
                max = hash;
            }
            if (j > minLen) {
                if (j > minLen * 4) {
                    break;
                }
                if ((hash & mask) == 1) {
                    break;
                }
            }
        }
        bucket = min;
        return i;
    }

    private static int[] getKey(int bucket, byte[] buff) {
        int[] key = new int[4];
        int[] counts = new int[8];
        int len = buff.length;
        for (int i = 0; i < len; i++) {
            int x = buff[i] & 0xff;
            counts[x >> 5]++;
        }
        int cs = 0;
        for (int i = 0; i < 8; i++) {
            cs *= 2;
            if (counts[i] > (len / 32)) {
                cs += 1;
            }
        }
        key[0] = cs;
        key[1] = bucket;
        key[2] = DataUtils.getFletcher32(buff, buff.length);
        return key;
    }

}
