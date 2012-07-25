/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store.btree;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Properties;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FileUtils;

/**
 * Convert a database file to a human-readable text dump.
 */
public class Dump {

    private static int blockSize = 4 * 1024;

    public static void main(String... args) {
        String fileName = "test.h3";
        for (int i = 0; i < args.length; i++) {
            if ("-file".equals(args[i])) {
                fileName = args[++i];
            }
        }
        dump(fileName);
    }

    public static void dump(String fileName) {
        if (!FileUtils.exists(fileName)) {
            System.out.println("File not found: " + fileName);
            return;
        }
        FileChannel file = null;
        try {
            file = FilePath.get(fileName).open("r");
            long fileLength = file.size();
            file.position(0);
            byte[] header = new byte[blockSize];
            file.read(ByteBuffer.wrap(header));
            Properties prop = new Properties();
            prop.load(new ByteArrayInputStream(header));
            prop.load(new StringReader(new String(header, "UTF-8")));
            System.out.println("file " + fileName);
            System.out.println("    length " + fileLength);
            System.out.println("    " + prop);
            ByteBuffer block = ByteBuffer.wrap(new byte[16]);
            for (long pos = 0; pos < fileLength;) {
                file.position(pos);
                block.rewind();
                FileUtils.readFully(file,  block);
                block.rewind();
                if (block.get() != 'c') {
                    pos += blockSize;
                    continue;
                }
                int length = block.getInt();
                int chunkId = block.getInt();
                int metaRootOffset = block.getInt();
                System.out.println("    chunk " + chunkId + " at " + pos +
                        " length " + length + " offset " + metaRootOffset);
                ByteBuffer chunk = ByteBuffer.allocate(length);
                file.position(pos);
                FileUtils.readFully(file, chunk);
                int p = block.position();
                pos = (pos + length + blockSize) / blockSize * blockSize;
                length -= p;
                while (length > 0) {
                    chunk.position(p);
                    int len = chunk.getInt();
                    long mapId = chunk.getLong();
                    int type = chunk.get();
                    int count = DataUtils.readVarInt(chunk);
                    if (type == 1) {
                        long[] children = new long[count];
                        for (int i = 0; i < count; i++) {
                            children[i] = chunk.getLong();
                        }
                        System.out.println("        map " + mapId + " at " + p + " node, " + count + " children: " + Arrays.toString(children));
                    } else {
                        System.out.println("        map " + mapId + " at " + p + " leaf, " + count + " rows");
                    }
                    p += len;
                    length -= len;
                }
            }
        } catch (IOException e) {
            System.out.println("ERROR: " + e);
        } finally {
            try {
                file.close();
            } catch (IOException e) {
                // ignore
            }
        }
        System.out.println();
    }

}
