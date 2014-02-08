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
            long fileLength = file.size();
            pw.println("file " + fileName);
            pw.println("    length " + Long.toHexString(fileLength));
            ByteBuffer block = ByteBuffer.allocate(4096);
            for (long pos = 0; pos < fileLength;) {
                block.rewind();
                DataUtils.readFully(file, pos, block);
                block.rewind();
                int headerType = block.get();
                if (headerType == 'H') {
                    pw.println("    store header at " + Long.toHexString(pos));
                    pw.println("    " + new String(block.array(), "UTF-8").trim());
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
                pw.println("    " + c.toString());
                ByteBuffer chunk = ByteBuffer.allocate(length);
                DataUtils.readFully(file, pos, chunk);
                int p = block.position();
                pos += length;
                int remaining = c.pageCount;
                while (remaining > 0) {
                    chunk.position(p);
                    int pageLength = chunk.getInt();
                    // check value (ignored)
                    chunk.getShort();
                    int mapId = DataUtils.readVarInt(chunk);
                    int len = DataUtils.readVarInt(chunk);
                    int type = chunk.get();
                    boolean compressed = (type & 2) != 0;
                    boolean node = (type & 1) != 0;
                    pw.println(
                            "        map " + Integer.toHexString(mapId) + 
                            " at " + Long.toHexString(p) + " " +
                            (node ? " node" : " leaf") + 
                            (compressed ? " compressed" : "") +
                            " len: " + Integer.toHexString(pageLength) + 
                            " entries: " + Integer.toHexString(len));
                    p += pageLength;
                    remaining--;
                    if (mapId == 0 && !compressed) {
                        String[] keys = new String[len];
                        for (int i = 0; i < len; i++) {
                            String k = StringDataType.INSTANCE.read(chunk);
                            keys[i] = k;
                        }
                        if (node) {
                            long[] children = new long[len + 1];
                            for (int i = 0; i <= len; i++) {
                                children[i] = chunk.getLong();
                            }
                            long[] counts = new long[len + 1];
                            for (int i = 0; i <= len; i++) {
                                long s = DataUtils.readVarLong(chunk);
                                counts[i] = s;
                            }
                            for (int i = 0; i < len; i++) {
                                pw.println("          < " + keys[i] + ": " +
                                        counts[i] + " -> " + getPosString(children[i]));
                            }
                            pw.println("          >= : " +
                                    counts[len] + " -> " + getPosString(children[len]));
                        } else {
                            // meta map leaf
                            String[] values = new String[len];
                            for (int i = 0; i < len; i++) {
                                String v = StringDataType.INSTANCE.read(chunk);
                                values[i] = v;
                            }
                            for (int i = 0; i < len; i++) {
                                pw.println("          " + keys[i] + "=" + values[i]);
                            }
                        }
                    }
                }
                chunk.position(chunk.limit() - Chunk.FOOTER_LENGTH);
                pw.println("      chunk footer");
                pw.println("      " + new String(chunk.array(), chunk.position(), Chunk.FOOTER_LENGTH, "UTF-8").trim());
            }
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
        pw.println();
        pw.flush();
    }

    private static String getPosString(long pos) {
        return "pos " + Long.toHexString(pos) + 
                ", chunk " + Integer.toHexString(DataUtils.getPageChunkId(pos)) +
                ", offset " + Integer.toHexString(DataUtils.getPageOffset(pos));

    }

}
