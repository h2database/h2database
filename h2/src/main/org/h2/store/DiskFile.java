/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

/**
 * This class represents a file that is usually written to disk.
 */
public class DiskFile {

    /**
     * The number of bits to shift to divide a position to get the page number.
     */
    public static final int BLOCK_PAGE_PAGE_SHIFT = 6;

    /**
     * The size of a page in blocks.
     * Each page contains blocks from the same storage.
     */
    public static final int BLOCKS_PER_PAGE = 1 << BLOCK_PAGE_PAGE_SHIFT;

    /**
     * The size of a block in bytes.
     * A block is the minimum row size.
     */
    public static final int BLOCK_SIZE = 128;

}
