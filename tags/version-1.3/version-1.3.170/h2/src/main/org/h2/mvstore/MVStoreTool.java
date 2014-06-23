/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Properties;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FileUtils;

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
    public static void main(String... args) throws IOException {
        for (int i = 0; i < args.length; i++) {
            if ("-dump".equals(args[i])) {
                String fileName = args[++i];
                dump(fileName, new PrintWriter(System.out));
            }
        }
    }

    /**
     * Read the contents of the file and display them in a human-readable
     * format.
     *
     * @param fileName the name of the file
     * @param writer the print writer
     */
    public static void dump(String fileName, PrintWriter writer) throws IOException {
        if (!FileUtils.exists(fileName)) {
            writer.println("File not found: " + fileName);
            return;
        }
        FileChannel file = null;
        int blockSize = MVStore.BLOCK_SIZE;
        try {
            file = FilePath.get(fileName).open("r");
            long fileLength = file.size();
            byte[] header = new byte[blockSize];
            file.read(ByteBuffer.wrap(header), 0);
            Properties prop = new Properties();
            prop.load(new ByteArrayInputStream(header));
            prop.load(new StringReader(new String(header, "UTF-8")));
            writer.println("file " + fileName);
            writer.println("    length " + fileLength);
            writer.println("    " + prop);
            ByteBuffer block = ByteBuffer.allocate(40);
            for (long pos = 0; pos < fileLength;) {
                block.rewind();
                DataUtils.readFully(file, pos, block);
                block.rewind();
                if (block.get() != 'c') {
                    pos += blockSize;
                    continue;
                }
                int chunkLength = block.getInt();
                int chunkId = block.getInt();
                int pageCount = block.getInt();
                long metaRootPos = block.getLong();
                long maxLength = block.getLong();
                long maxLengthLive = block.getLong();
                writer.println("    chunk " + chunkId +
                        " at " + pos +
                        " length " + chunkLength +
                        " pageCount " + pageCount +
                        " root " + metaRootPos +
                        " maxLength " + maxLength +
                        " maxLengthLive " + maxLengthLive);
                ByteBuffer chunk = ByteBuffer.allocate(chunkLength);
                DataUtils.readFully(file, pos, chunk);
                int p = block.position();
                pos = (pos + chunkLength + blockSize) / blockSize * blockSize;
                chunkLength -= p;
                while (chunkLength > 0) {
                    chunk.position(p);
                    int pageLength = chunk.getInt();
                    // check value (ignored)
                    chunk.getShort();
                    long mapId = DataUtils.readVarInt(chunk);
                    int len = DataUtils.readVarInt(chunk);
                    int type = chunk.get();
                    boolean compressed = (type & 2) != 0;
                    boolean node = (type & 1) != 0;
                    writer.println("        map " + mapId + " at " + p + " " +
                            (node ? "node" : "leaf") + " " +
                            (compressed ? "compressed " : "") +
                            "len: " + pageLength + " entries: " + len);
                    p += pageLength;
                    chunkLength -= pageLength;
                }
            }
        } catch (IOException e) {
            writer.println("ERROR: " + e);
            throw e;
        } finally {
            if (file != null) {
                try {
                    file.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
        writer.println();
        writer.flush();
    }

}
