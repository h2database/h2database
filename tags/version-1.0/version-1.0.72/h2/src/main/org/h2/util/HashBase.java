/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.sql.SQLException;

/**
 * The base for other hash classes.
 */
public abstract class HashBase {

    /**
     * The bit mask to get the index from the hash code.
     */
    protected int mask;

    /**
     * The number of slots in the table.
     */
    protected int len;

    /**
     * The number of occupied slots, excluding the zero key (if any).
     */
    protected int size;

    /**
     * The number of deleted slots.
     */
    protected int deletedCount;

    /**
     * The level. The number of slots is 2 ^ level.
     */
    protected int level;

    /**
     * Whether the zero key is used.
     */
    protected boolean zeroKey;

    private int maxSize, minSize, maxDeleted;
    private static final int MAX_LOAD = 90;

    /**
     * Increase the size of the underlying table and re-distribute the elements.
     *
     * @param newLevel the new level
     */
    protected abstract void rehash(int newLevel) throws SQLException;

    public HashBase() {
        reset(2);
    }

    public int size() {
        return size + (zeroKey ? 1 : 0);
    }

    protected void checkSizePut() throws SQLException {
        if (deletedCount > size) {
            rehash(level);
        }
        if (size + deletedCount >= maxSize) {
            rehash(level + 1);
        }
    }

    protected void checkSizeRemove() throws SQLException {
        if (size < minSize && level > 0) {
            rehash(level - 1);
        } else if (deletedCount > maxDeleted) {
            rehash(level);
        }
    }

    protected void reset(int newLevel) {
        minSize = size * 3 / 4;
        size = 0;
        level = newLevel;
        len = 2 << level;
        mask = len - 1;
        maxSize = (int) (len * MAX_LOAD / 100L);
        deletedCount = 0;
        maxDeleted = 20 + len / 2;
    }

    protected int getIndex(int hash) {
        return hash & mask;
    }

}
