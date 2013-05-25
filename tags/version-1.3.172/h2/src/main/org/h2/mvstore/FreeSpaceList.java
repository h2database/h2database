/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.util.ArrayList;
import java.util.List;

/**
 * A list that maintains ranges of free space (in pages) in a file.
 */
public class FreeSpaceList {

    /**
     * The first 2 pages are occupied by the file header.
     */
    private static final int FIRST_FREE_PAGE = 2;

    /**
     * The maximum number of pages. Smaller than than MAX_VALUE to avoid
     * overflow errors during arithmetic operations.
     */
    private static final int MAX_PAGE_COUNT = Integer.MAX_VALUE / 2;

    private List<PageRange> freeSpaceList = new ArrayList<PageRange>();

    public FreeSpaceList() {
        clear();
    }

    /**
     * Reset the list.
     */
    public synchronized void clear() {
        freeSpaceList.clear();
        freeSpaceList.add(new PageRange(FIRST_FREE_PAGE, MAX_PAGE_COUNT));
    }

    /**
     * Allocate a number of pages.
     *
     * @param length the number of bytes to allocate
     * @return the position in pages
     */
    public synchronized int allocatePages(long length) {
        int required = (int) (length / MVStore.BLOCK_SIZE) + 1;
        for (PageRange pr : freeSpaceList) {
            if (pr.length >= required) {
                return pr.start;
            }
        }
        throw DataUtils.newIllegalStateException(
                "Could not find a free page to allocate");
    }

    /**
     * Mark a chunk as used.
     *
     * @param c the chunk
     */
    public synchronized void markUsed(Chunk c) {
        int chunkStart = (int) (c.start / MVStore.BLOCK_SIZE);
        int required = (int) ((c.start + c.length) / MVStore.BLOCK_SIZE) + 2 - chunkStart;
        PageRange found = null;
        int i = 0;
        for (PageRange pr : freeSpaceList) {
            if (chunkStart >= pr.start && chunkStart < (pr.start + pr.length)) {
                found = pr;
                break;
            }
            i++;
        }
        if (found == null) {
            throw DataUtils.newIllegalStateException(
                    "Cannot find spot to mark chunk as used in free list: {0}", c);
        }
        if (chunkStart + required > found.start + found.length) {
            throw DataUtils.newIllegalStateException(
                    "Chunk runs over edge of free space: {0}", c);
        }
        if (found.start == chunkStart) {
            // if the used-chunk is at the beginning of a free-space-range
            found.start += required;
            found.length -= required;
            if (found.length == 0) {
                // if the free-space-range is now empty, remove it
                freeSpaceList.remove(i);
            }
        } else if (found.start + found.length == chunkStart + required) {
            // if the used-chunk is at the end of a free-space-range
            found.length -= required;
            if (found.length == 0) {
                // if the free-space-range is now empty, remove it
                freeSpaceList.remove(i);
            }
        } else {
            // it's in the middle, so split the existing entry
            int length1 = chunkStart - found.start;
            int start2 = chunkStart + required;
            int length2 = found.start + found.length - chunkStart - required;

            found.length = length1;
            PageRange newRange = new PageRange(start2, length2);
            freeSpaceList.add(i + 1, newRange);
        }
    }

    /**
     * Mark the chunk as free.
     *
     * @param c the chunk
     */
    public synchronized void markFree(Chunk c) {
        int chunkStart = (int) (c.start / MVStore.BLOCK_SIZE);
        int required = (c.length / MVStore.BLOCK_SIZE) + 1;
        PageRange found = null;
        int i = 0;
        for (PageRange pr : freeSpaceList) {
            if (pr.start > chunkStart) {
                found = pr;
                break;
            }
            i++;
        }
        if (found == null) {
            throw DataUtils.newIllegalStateException(
                    "Cannot find spot to mark chunk as unused in free list: {0}", c);
        }
        if (chunkStart + required + 1 == found.start) {
            // if the used-chunk is adjacent to the beginning of a
            // free-space-range
            found.start = chunkStart;
            found.length += required;
            // compact: merge the previous entry into this one if
            // they are now adjacent
            if (i > 0) {
                PageRange previous = freeSpaceList.get(i - 1);
                if (previous.start + previous.length + 1 == found.start) {
                    previous.length += found.length;
                    freeSpaceList.remove(i);
                }
            }
            return;
        }
        if (i > 0) {
            // if the used-chunk is adjacent to the end of a free-space-range
            PageRange previous = freeSpaceList.get(i - 1);
            if (previous.start + previous.length + 1 == chunkStart) {
                previous.length += required;
                // compact: merge the next entry into this one if
                // they are now adjacent
                if (previous.start + previous.length + 1 == found.start) {
                    previous.length += found.length;
                    freeSpaceList.remove(i);
                }
                return;
            }
        }

        // it is between 2 entries, so add a new one
        PageRange newRange = new PageRange(chunkStart, required);
        freeSpaceList.add(i, newRange);
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        boolean first = true;
        for (PageRange r : freeSpaceList) {
            if (first) {
                first = false;
            } else {
                buff.append(", ");
            }
            buff.append(r.start + "-" + (r.start + r.length - 1));
        }
        return buff.toString();
    }

    /**
     * A range of free pages.
     */
    private static final class PageRange {

        /**
         * The starting point, in pages.
         */
        public int start;

        /**
         * The length, in pages.
         */
        public int length;

        public PageRange(int start, int length) {
            this.start = start;
            this.length = length;
        }

        @Override
        public String toString() {
            return "start:" + start + " length:" + length;
        }
    }

}
