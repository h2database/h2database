/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.sql.SQLException;
import java.util.Map;
import java.util.WeakHashMap;

import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.message.Message;

/**
 * A cache implementation based on the last recently used (LRU) algorithm.
 */
public class CacheLRU implements Cache {

    static final String TYPE_NAME = "LRU";

    private final CacheWriter writer;
    private final CacheObject head = new CacheHead();
    private final int len;
    private final int mask;
    private int maxSize;
    private CacheObject[] values;
    private int recordCount;
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
    public static Cache getCache(CacheWriter writer, String cacheType, int cacheSize) throws SQLException {
        Map<Integer, CacheObject> secondLevel = null;
        String prefix = null;
        if (cacheType.startsWith("SOFT_")) {
            secondLevel = new SoftHashMap<Integer, CacheObject>();
            cacheType = cacheType.substring("SOFT_".length());
            prefix = "SOFT_";
        } else if (cacheType.startsWith("WEAK_")) {
            secondLevel = new WeakHashMap<Integer, CacheObject>();
            cacheType = cacheType.substring("WEAK_".length());
            prefix = "WEAK_";
        }
        Cache cache;
        if ("TQ".equals(cacheType)) {
            // for backward compatibility - actually using LRU not TQ
            cache = new CacheLRU(writer, cacheSize);
        } else if (CacheLRU.TYPE_NAME.equals(cacheType)) {
            cache = new CacheLRU(writer, cacheSize);
        } else {
            throw Message.getInvalidValueException(cacheType, "CACHE_TYPE");
        }
        if (secondLevel != null) {
            cache = new CacheSecondLevel(cache, prefix, secondLevel);
        }
        return cache;
    }

    public void clear() {
        head.next = head.previous = head;
        // first set to null - avoiding out of memory
        values = null;
        values = new CacheObject[len];
        recordCount = 0;
        sizeMemory = 0;
    }

    public void put(CacheObject rec) throws SQLException {
        if (SysProperties.CHECK) {
            int pos = rec.getPos();
            for (int i = 0; i < rec.getBlockCount(); i++) {
                CacheObject old = find(pos + i);
                if (old != null) {
                    Message.throwInternalError("try to add a record twice pos:" + pos + " i:" + i);
                }
            }
        }
        int index = rec.getPos() & mask;
        rec.chained = values[index];
        values[index] = rec;
        recordCount++;
        sizeMemory += rec.getMemorySize();
        addToFront(rec);
        removeOldIfRequired();
    }

    public CacheObject update(int pos, CacheObject rec) throws SQLException {
        CacheObject old = find(pos);
        if (old == null) {
            put(rec);
        } else {
            if (SysProperties.CHECK) {
                if (old != rec) {
                    Message.throwInternalError("old!=record pos:" + pos + " old:" + old + " new:" + rec);
                }
            }
            removeFromLinkedList(rec);
            addToFront(rec);
        }
        return old;
    }

    private void removeOldIfRequired() throws SQLException {
        // a small method, to allow inlining
        if (sizeMemory >= maxSize) {
            removeOld();
        }
    }

    private void removeOld() throws SQLException {
        int i = 0;
        ObjectArray<CacheObject> changed = ObjectArray.newInstance();
        int mem = sizeMemory;
        int rc = recordCount;
        boolean flushed = false;
        CacheObject next = head.next;
        while (mem * 4 > maxSize * 3 && rc > Constants.CACHE_MIN_RECORDS) {
            CacheObject check = next;
            next = check.next;
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
                Message.throwInternalError("try to remove head");
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
            CacheObject.sort(changed);
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
                    if (rec.next != null) {
                        throw Message.throwInternalError();
                    }
                }
            }
        }
    }

    private void addToFront(CacheObject rec) {
        if (SysProperties.CHECK && rec == head) {
            Message.throwInternalError("try to move head");
        }
        rec.next = head;
        rec.previous = head.previous;
        rec.previous.next = rec;
        head.previous = rec;
    }

    private void removeFromLinkedList(CacheObject rec) {
        if (SysProperties.CHECK && rec == head) {
            Message.throwInternalError("try to remove head");
        }
        rec.previous.next = rec.next;
        rec.next.previous = rec.previous;
        // TODO cache: mystery: why is this required? needs more memory if we
        // don't do this
        rec.next = null;
        rec.previous = null;
    }

    public void remove(int pos) {
        int index = pos & mask;
        CacheObject rec = values[index];
        if (rec == null) {
            return;
        }
        if (rec.getPos() == pos) {
            values[index] = rec.chained;
        } else {
            CacheObject last;
            do {
                last = rec;
                rec = rec.chained;
                if (rec == null) {
                    return;
                }
            } while (rec.getPos() != pos);
            last.chained = rec.chained;
        }
        recordCount--;
        sizeMemory -= rec.getMemorySize();
        removeFromLinkedList(rec);
        if (SysProperties.CHECK) {
            rec.chained = null;
            CacheObject o = find(pos);
            if (o != null) {
                Message.throwInternalError("not removed: " + o);
            }
        }
    }

    public CacheObject find(int pos) {
        CacheObject rec = values[pos & mask];
        while (rec != null && rec.getPos() != pos) {
            rec = rec.chained;
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

    public ObjectArray<CacheObject> getAllChanged() {
//        if(Database.CHECK) {
//            testConsistency();
//        }
        ObjectArray<CacheObject> list = ObjectArray.newInstance();
        CacheObject rec = head.next;
        while (rec != head) {
            if (rec.isChanged()) {
                list.add(rec);
            }
            rec = rec.next;
        }
        return list;
    }

    public void setMaxSize(int maxKb) throws SQLException {
        int newSize = maxKb * 1024 / 4;
        maxSize = newSize < 0 ? 0 : newSize;
        // can not resize, otherwise existing records are lost
        // resize(maxSize);
        removeOldIfRequired();
    }

    public String getTypeName() {
        return TYPE_NAME;
    }

    public int  getMaxSize() {
        return maxSize;
    }

    public int getSize() {
        return sizeMemory;
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
//        ObjectArray list = ObjectArray.newInstance();
//        while(it.hasNext()) {
//            Record rec = (Record)it.next();
//            if(rec.isChanged()) {
//                list.add(rec);
//            }
//        }
//        return list;
//    }
//}

