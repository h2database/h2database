/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.message.DbException;

/**
 * A cache implementation based on the last recently used (LRU) algorithm.
 */
public class CacheLRU implements Cache {

    static final String TYPE_NAME = "LRU";

    private final CacheWriter writer;
    private final CacheObject head = new CacheHead();
    private final int mask;
    private CacheObject[] values;
    private int recordCount;

    /**
     * The number of cache buckets.
     */
    private final int len;

    /**
     * The maximum memory, in words (4 bytes each).
     */
    private int maxSize;

    /**
     * The current memory used in this cache, in words (4 bytes each).
     */
    private int sizeMemory;

    private CacheLRU(CacheWriter writer, int maxKb) {
        this.maxSize = maxKb * 1024 / 4;
        this.writer = writer;
        this.len = MathUtils.nextPowerOf2(maxSize / 64);
        this.mask = len - 1;
        MathUtils.checkPowerOf2(len);
        clear();
    }

    /**
     * Create a cache of the given type and size.
     *
     * @param writer the cache writer
     * @param cacheType the cache type
     * @param cacheSize the size
     * @return the cache object
     */
    public static Cache getCache(CacheWriter writer, String cacheType, int cacheSize) {
        Map<Integer, CacheObject> secondLevel = null;
        if (cacheType.startsWith("SOFT_")) {
            secondLevel = new SoftHashMap<Integer, CacheObject>();
            cacheType = cacheType.substring("SOFT_".length());
        }
        Cache cache;
        if (CacheLRU.TYPE_NAME.equals(cacheType)) {
            cache = new CacheLRU(writer, cacheSize);
        } else {
            throw DbException.getInvalidValueException(cacheType, "CACHE_TYPE");
        }
        if (secondLevel != null) {
            cache = new CacheSecondLevel(cache, secondLevel);
        }
        return cache;
    }

    public void clear() {
        head.cacheNext = head.cachePrevious = head;
        // first set to null - avoiding out of memory
        values = null;
        values = new CacheObject[len];
        recordCount = 0;
        sizeMemory = 0;
    }

    public void put(CacheObject rec) {
        if (SysProperties.CHECK) {
            int pos = rec.getPos();
            CacheObject old = find(pos);
            if (old != null) {
                DbException.throwInternalError("try to add a record twice pos:" + pos);
            }
        }
        int index = rec.getPos() & mask;
        rec.cacheChained = values[index];
        values[index] = rec;
        recordCount++;
        sizeMemory += rec.getMemorySize();
        addToFront(rec);
        removeOldIfRequired();
    }

    public CacheObject update(int pos, CacheObject rec) {
        CacheObject old = find(pos);
        if (old == null) {
            put(rec);
        } else {
            if (SysProperties.CHECK) {
                if (old != rec) {
                    DbException.throwInternalError("old!=record pos:" + pos + " old:" + old + " new:" + rec);
                }
            }
            removeFromLinkedList(rec);
            addToFront(rec);
        }
        return old;
    }

    private void removeOldIfRequired() {
        // a small method, to allow inlining
        if (sizeMemory >= maxSize) {
            removeOld();
        }
    }

    private void removeOld() {
        int i = 0;
        ArrayList<CacheObject> changed = New.arrayList();
        int mem = sizeMemory;
        int rc = recordCount;
        boolean flushed = false;
        CacheObject next = head.cacheNext;
        while (mem * 4 > maxSize * 3 && rc > Constants.CACHE_MIN_RECORDS) {
            CacheObject check = next;
            next = check.cacheNext;
            i++;
            if (i >= recordCount) {
                if (!flushed) {
                    writer.flushLog();
                    flushed = true;
                    i = 0;
                } else {
                    // can't remove any record, because the records can not be removed
                    // hopefully this does not happen frequently, but it can happen
                    writer.getTrace().info("Cannot remove records, cache size too small? records:" + recordCount + " memory:" + sizeMemory);
                    break;
                }
            }
            if (SysProperties.CHECK && check == head) {
                DbException.throwInternalError("try to remove head");
            }
            // we are not allowed to remove it if the log is not yet written
            // (because we need to log before writing the data)
            // also, can't write it if the record is pinned
            if (!check.canRemove()) {
                removeFromLinkedList(check);
                addToFront(check);
                continue;
            }
            rc--;
            mem -= check.getMemorySize();
            if (check.isChanged()) {
                changed.add(check);
            } else {
                remove(check.getPos());
            }
        }
        if (changed.size() > 0) {
            Collections.sort(changed);
            int max = maxSize;
            try {
                // temporary disable size checking,
                // to avoid stack overflow
                maxSize = Integer.MAX_VALUE;
                for (i = 0; i < changed.size(); i++) {
                    CacheObject rec = changed.get(i);
                    writer.writeBack(rec);
                }
            } finally {
                maxSize = max;
            }
            for (i = 0; i < changed.size(); i++) {
                CacheObject rec = changed.get(i);
                remove(rec.getPos());
                if (SysProperties.CHECK) {
                    if (rec.cacheNext != null) {
                        throw DbException.throwInternalError();
                    }
                }
            }
        }
    }

    private void addToFront(CacheObject rec) {
        if (SysProperties.CHECK && rec == head) {
            DbException.throwInternalError("try to move head");
        }
        rec.cacheNext = head;
        rec.cachePrevious = head.cachePrevious;
        rec.cachePrevious.cacheNext = rec;
        head.cachePrevious = rec;
    }

    private void removeFromLinkedList(CacheObject rec) {
        if (SysProperties.CHECK && rec == head) {
            DbException.throwInternalError("try to remove head");
        }
        rec.cachePrevious.cacheNext = rec.cacheNext;
        rec.cacheNext.cachePrevious = rec.cachePrevious;
        // TODO cache: mystery: why is this required? needs more memory if we
        // don't do this
        rec.cacheNext = null;
        rec.cachePrevious = null;
    }

    public void remove(int pos) {
        int index = pos & mask;
        CacheObject rec = values[index];
        if (rec == null) {
            return;
        }
        if (rec.getPos() == pos) {
            values[index] = rec.cacheChained;
        } else {
            CacheObject last;
            do {
                last = rec;
                rec = rec.cacheChained;
                if (rec == null) {
                    return;
                }
            } while (rec.getPos() != pos);
            last.cacheChained = rec.cacheChained;
        }
        recordCount--;
        sizeMemory -= rec.getMemorySize();
        removeFromLinkedList(rec);
        if (SysProperties.CHECK) {
            rec.cacheChained = null;
            CacheObject o = find(pos);
            if (o != null) {
                DbException.throwInternalError("not removed: " + o);
            }
        }
    }

    public CacheObject find(int pos) {
        CacheObject rec = values[pos & mask];
        while (rec != null && rec.getPos() != pos) {
            rec = rec.cacheChained;
        }
        return rec;
    }

    public CacheObject get(int pos) {
        CacheObject rec = find(pos);
        if (rec != null) {
            removeFromLinkedList(rec);
            addToFront(rec);
        }
        return rec;
    }

//    private void testConsistency() {
//        int s = size;
//        HashSet set = new HashSet();
//        for(int i=0; i<values.length; i++) {
//            Record rec = values[i];
//            if(rec == null) {
//                continue;
//            }
//            set.add(rec);
//            while(rec.chained != null) {
//                rec = rec.chained;
//                set.add(rec);
//            }
//        }
//        Record rec = head.next;
//        while(rec != head) {
//            set.add(rec);
//            rec = rec.next;
//        }
//        rec = head.previous;
//        while(rec != head) {
//            set.add(rec);
//            rec = rec.previous;
//        }
//        if(set.size() != size) {
//            System.out.println("size="+size+" but el.size="+set.size());
//        }
//    }

    public ArrayList<CacheObject> getAllChanged() {
//        if(Database.CHECK) {
//            testConsistency();
//        }
        ArrayList<CacheObject> list = New.arrayList();
        CacheObject rec = head.cacheNext;
        while (rec != head) {
            if (rec.isChanged()) {
                list.add(rec);
            }
            rec = rec.cacheNext;
        }
        return list;
    }

    public void setMaxSize(int maxKb) {
        int newSize = maxKb * 1024 / 4;
        maxSize = newSize < 0 ? 0 : newSize;
        // can not resize, otherwise existing records are lost
        // resize(maxSize);
        removeOldIfRequired();
    }

    public int getMaxSize() {
        return maxSize * 4 / 1024;
    }

    public int getSize() {
        return sizeMemory * 4 / 1024;
    }

}

// Unmaintained reference code (very old)
//import java.util.Iterator;
//import java.util.LinkedHashMap;
//import java.util.Map;
//
//public class Cache extends LinkedHashMap {
//
//    final static int MAX_SIZE = 1 << 10;
//    private CacheWriter writer;
//
//    public Cache(CacheWriter writer) {
//        super(16, (float) 0.75, true);
//        this.writer = writer;
//    }
//
//    protected boolean removeEldestEntry(Map.Entry eldest) {
//        if(size() <= MAX_SIZE) {
//            return false;
//        }
//        Record entry = (Record) eldest.getValue();
//        if(entry.getDeleted()) {
//            return true;
//        }
//        if(entry.isChanged()) {
//            try {
////System.out.println("cache write "+entry.getPos());
//                writer.writeBack(entry);
//            } catch(SQLException e) {
//                // printStackTrace not needed
//                // if we use our own hashtable
//                e.printStackTrace();
//            }
//        }
//        return true;
//    }
//
//    public void put(Record rec) {
//        put(new Integer(rec.getPos()), rec);
//    }
//
//    public Record get(int pos) {
//        return (Record)get(new Integer(pos));
//    }
//
//    public void remove(int pos) {
//        remove(new Integer(pos));
//    }
//
//    public ObjectArray getAllChanged() {
//        Iterator it = values().iterator();
//        ObjectArray list = New.arrayList();
//        while(it.hasNext()) {
//            Record rec = (Record)it.next();
//            if(rec.isChanged()) {
//                list.add(rec);
//            }
//        }
//        return list;
//    }
//}

