/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.h2.mvstore.type.StringDataType;
import org.h2.store.fs.FilePath;

/**
 * Utility methods used in combination with the MVStore.
 */
public class MVStoreTool {

    /**
     * Runs this tool.
     * Options are case sensitive. Supported options are:
     * <table>
     * <tr><td>[-dump &lt;dir&gt;]</td>
     * <td>Dump the contends of the file</td></tr>
     * </table>
     *
     * @param args the command line arguments
     */
    public static void main(String... args) {
        for (int i = 0; i < args.length; i++) {
            if ("-dump".equals(args[i])) {
                String fileName = args[++i];
                dump(fileName, new PrintWriter(System.out));
            }
        }
    }

    /**
     * Read the contents of the file and write them to system out.
     *
     * @param fileName the name of the file
     */
    public static void dump(String fileName) {
        dump(fileName, new PrintWriter(System.out));
    }

    /**
     * Read the contents of the file and display them in a human-readable
     * format.
     *
     * @param fileName the name of the file
     * @param writer the print writer
     */
    public static void dump(String fileName, Writer writer) {
        PrintWriter pw = new PrintWriter(writer, true);
        if (!FilePath.get(fileName).exists()) {
            pw.println("File not found: " + fileName);
            return;
        }
        FileChannel file = null;
        int blockSize = MVStore.BLOCK_SIZE;
        try {
            file = FilePath.get(fileName).open("r");
            long fileSize = file.size();
            int len = Long.toHexString(fileSize).length();
            ByteBuffer block = ByteBuffer.allocate(4096);
            for (long pos = 0; pos < fileSize;) {
                block.rewind();
                DataUtils.readFully(file, pos, block);
                block.rewind();
                int headerType = block.get();
                if (headerType == 'H') {
                    pw.printf("%0" + len + "x fileHeader %s%n",
                            pos,
                            new String(block.array(), DataUtils.LATIN).trim());
                    pos += blockSize;
                    continue;
                }
                if (headerType != 'c') {
                    pos += blockSize;
                    continue;
                }
                block.position(0);
                Chunk c = Chunk.readChunkHeader(block, pos);
                int length = c.len * MVStore.BLOCK_SIZE;
                pw.printf("%n%0" + len + "x chunkHeader %s%n",
                        pos, c.toString());
                ByteBuffer chunk = ByteBuffer.allocate(length);
                DataUtils.readFully(file, pos, chunk);
                int p = block.position();
                pos += length;
                int remaining = c.pageCount;
                while (remaining > 0) {
                    chunk.position(p);
                    int pageSize = chunk.getInt();
                    // check value (ignored)
                    chunk.getShort();
                    int mapId = DataUtils.readVarInt(chunk);
                    int entries = DataUtils.readVarInt(chunk);
                    int type = chunk.get();
                    boolean compressed = (type & 2) != 0;
                    boolean node = (type & 1) != 0;
                    pw.printf(
                            "+%0" + len +
                            "x %s, map %x, %d entries, %d bytes%n",
                            p,
                            (node ? "node" : "leaf") +
                            (compressed ? " compressed" : ""),
                            mapId,
                            node ? entries + 1 : entries,
                            pageSize);
                    p += pageSize;
                    remaining--;
                    long[] children = null;
                    long[] counts = null;
                    if (node) {
                        children = new long[entries + 1];
                        for (int i = 0; i <= entries; i++) {
                            children[i] = chunk.getLong();
                        }
                        counts = new long[entries + 1];
                        for (int i = 0; i <= entries; i++) {
                            long s = DataUtils.readVarLong(chunk);
                            counts[i] = s;
                        }
                    }
                    String[] keys = new String[entries];
                    if (mapId == 0) {
                        if (!compressed) {
                            for (int i = 0; i < entries; i++) {
                                String k = StringDataType.INSTANCE.read(chunk);
                                keys[i] = k;
                            }
                        }
                        if (node) {
                            // meta map node
                            for (int i = 0; i < entries; i++) {
                                long cp = children[i];
                                pw.printf("    %d children < %s @ " +
                                        "chunk %x +%0" +
                                        len + "x%n",
                                        counts[i],
                                        keys[i],
                                        DataUtils.getPageChunkId(cp),
                                        DataUtils.getPageOffset(cp));
                            }
                            long cp = children[entries];
                            pw.printf("    %d children >= %s @ chunk %x +%0" +
                                    len + "x%n",
                                    counts[entries],
                                    keys[entries],
                                    DataUtils.getPageChunkId(cp),
                                    DataUtils.getPageOffset(cp));
                        } else if (!compressed) {
                            // meta map leaf
                            String[] values = new String[entries];
                            for (int i = 0; i < entries; i++) {
                                String v = StringDataType.INSTANCE.read(chunk);
                                values[i] = v;
                            }
                            for (int i = 0; i < entries; i++) {
                                pw.println("    " + keys[i] +
                                        " = " + values[i]);
                            }
                        }
                    } else {
                        if (node) {
                            for (int i = 0; i <= entries; i++) {
                                long cp = children[i];
                                pw.printf("    %d children @ chunk %x +%0" +
                                        len + "x%n",
                                        counts[i],
                                        DataUtils.getPageChunkId(cp),
                                        DataUtils.getPageOffset(cp));
                            }
                        }
                    }
                }
                int footerPos = chunk.limit() - Chunk.FOOTER_LENGTH;
                chunk.position(footerPos);
                pw.printf(
                        "+%0" + len + "x chunkFooter %s%n",
                        footerPos,
                        new String(chunk.array(), chunk.position(),
                                Chunk.FOOTER_LENGTH, DataUtils.LATIN).trim());
            }
            pw.printf("%n%0" + len + "x eof%n", fileSize);
        } catch (IOException e) {
            pw.println("ERROR: " + e);
            e.printStackTrace(pw);
        } finally {
            if (file != null) {
                try {
                    file.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
        pw.flush();
    }

}
