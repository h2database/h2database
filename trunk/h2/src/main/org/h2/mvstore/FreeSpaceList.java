package org.h2.mvstore;

import java.util.ArrayList;
import java.util.List;

public class FreeSpaceList {

    /** the first 2 pages are occupied by the file header. */
    private static final int FIRST_FREE_PAGE = 2;
    /** I use max_value/2 to avoid overflow errors during arithmetic. */
    private static final int MAX_NO_PAGES = Integer.MAX_VALUE / 2;

    private List<PageRange> freeSpaceList = new ArrayList<PageRange>();

    public FreeSpaceList() {
        freeSpaceList.add(new PageRange(FIRST_FREE_PAGE, MAX_NO_PAGES));
    }

    private static final class PageRange {
        /** the start point, in pages */
        public int start;
        /** the length, in pages */
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

    /**
     * @return position in pages
     */
    public synchronized int allocatePages(long length) {
        int required = (int) (length / MVStore.BLOCK_SIZE) + 1;
        for (PageRange pr : freeSpaceList) {
            if (pr.length >= required) {
                return pr.start;
            }
        }
        throw DataUtils.newIllegalStateException("could not find a free page to allocate");
    }

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
            throw DataUtils.newIllegalStateException("cannot find spot to mark chunk as used in free list, "
                    + c.toString());
        }
        if (chunkStart + required > found.start + found.length) {
            throw DataUtils.newIllegalStateException("chunk runs over edge of free space, " + c.toString());
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
            throw DataUtils.newIllegalStateException("cannot find spot to mark chunk as unused in free list, "
                    + c.toString());
        }
        if (chunkStart + required + 1 == found.start) {
            // if the used-chunk is adjacent to the beginning of a
            // free-space-range
            found.start = chunkStart;
            found.length += required;
            // compactify-free-list: merge the previous entry into this one if
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
                // compactify-free-list: merge the next entry into this one if
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

    public synchronized void clear() {
        freeSpaceList.clear();
        freeSpaceList.add(new PageRange(FIRST_FREE_PAGE, MAX_NO_PAGES));
    }

    /** debug method */
    public void dumpFreeList() {
        StringBuilder buf = new StringBuilder("free list : ");
        boolean first = true;
        for (PageRange pr : freeSpaceList) {
            if (first) {
                first = false;
            } else {
                buf.append(", ");
            }
            buf.append(pr.start + "-" + (pr.start + pr.length - 1));
        }
        System.out.println(buf.toString());
    }
}
