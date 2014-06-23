/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store.btree;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * A block of data.
 */
class Block {

    /**
     * The block id.
     */
    int id;

    /**
     * The start position within the file.
     */
    long start;

    /**
     * The length in bytes.
     */
    long length;

    /**
     * The entry count.
     */
    int entryCount;

    /**
     * The number of life (non-garbage) objects.
     */
    int liveCount;

    /**
     * The garbage collection priority.
     */
    int collectPriority;

    Block(int id) {
        this.id = id;
    }

    /**
     * Build a block from the given string.
     *
     * @param s the string
     * @return the block
     */
    static Block fromString(String s) {
        Block b = new Block(0);
        Properties prop = new Properties();
        try {
            prop.load(new ByteArrayInputStream(s.getBytes("UTF-8")));
            b.id = Integer.parseInt(prop.get("id").toString());
            b.start = Long.parseLong(prop.get("start").toString());
            b.length = Long.parseLong(prop.get("length").toString());
            b.entryCount = Integer.parseInt(prop.get("entryCount").toString());
            b.liveCount = Integer.parseInt(prop.get("liveCount").toString());
            return b;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int getFillRate() {
        return entryCount == 0 ? 0 : 100 * liveCount / entryCount;
    }

    public int hashCode() {
        return id;
    }

    public boolean equals(Object o) {
        return o instanceof Block && ((Block) o).id == id;
    }

    public String toString() {
        return
            "id:" + id + "\n" +
            "start:" + start + "\n" +
            "length:" + length + "\n" +
            "entryCount:" + entryCount + "\n" +
            "liveCount:" + liveCount + "\n";
    }

}

