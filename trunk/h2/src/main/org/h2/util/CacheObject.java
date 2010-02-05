/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.Comparator;

import org.h2.constant.SysProperties;
import org.h2.message.Message;

/**
 * The base object for all cached objects.
 */
public abstract class CacheObject {

    /**
     * Compare cache objects by position.
     */
    static class CacheComparator implements Comparator<CacheObject> {
        public int compare(CacheObject a, CacheObject b) {
            return MathUtils.compareInt(a.getPos(), b.getPos());
        }
    }

    /**
     * Ensure the class is loaded when initialized, so that sorting is possible
     * even when loading new classes is not allowed any more. This can occur
     * when stopping a web application.
     */
    static {
        new CacheComparator();
    }

    /**
     * The previous element in the LRU linked list. If the previous element is
     * the head, then this element is the most recently used object.
     */
    public CacheObject cachePrevious;

    /**
     * The next element in the LRU linked list. If the next element is the head,
     * then this element is the least recently used object.
     */
    public CacheObject cacheNext;

    /**
     * The next element in the hash chain.
     */
    public CacheObject cacheChained;

    /**
     * The cache queue identifier. This field is only used for the 2Q cache
     * algorithm.
     */
    public int cacheQueue;

    private int pos;
    private boolean changed;

    /**
     * Check if the object can be removed from the cache.
     * For example pinned objects can not be removed.
     *
     * @return true if it can be removed
     */
    public abstract boolean canRemove();

    /**
     * Get the estimated memory size.
     *
     * @return number of double words (4 bytes)
     */
    public abstract int getMemorySize();

    /**
     * Order the given list of cache objects by position.
     *
     * @param recordList the list of cache objects
     */
    public static void sort(ObjectArray<CacheObject> recordList) {
        recordList.sort(new CacheComparator());
    }

    public void setPos(int pos) {
        if (SysProperties.CHECK && (cachePrevious != null || cacheNext != null || cacheChained != null)) {
            Message.throwInternalError("setPos too late");
        }
        this.pos = pos;
    }

    public int getPos() {
        return pos;
    }

    /**
     * Check if this cache object has been changed and thus needs to be written
     * back to the storage.
     *
     * @return if it has been changed
     */
    public boolean isChanged() {
        return changed;
    }

    public void setChanged(boolean b) {
        changed = b;
    }

}
