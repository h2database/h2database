/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.util;

import java.sql.SQLException;

import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.message.Message;
import org.h2.util.Cache;
import org.h2.util.CacheHead;
import org.h2.util.CacheObject;
import org.h2.util.CacheWriter;
import org.h2.util.MathUtils;
import org.h2.util.ObjectArray;

/**
 * A cache implementation based on the 2Q algorithm.
 * For about the algorithm, see
 * http://www.vldb.org/conf/1994/P439.PDF .
 * In this implementation, items are moved from 'in'
 * queue and move to the 'main' queue if the are referenced again.
 */
class CacheTQ implements Cache {

    static final String TYPE_NAME = "TQ";

    private static final int MAIN = 1, IN = 2, OUT = 3;
    private static final int PERCENT_IN = 20, PERCENT_OUT = 50;

    private final CacheWriter writer;
    private final CacheObject headMain = new CacheHead();
    private final CacheObject headIn = new CacheHead();
    private final CacheObject headOut = new CacheHead();
    private final int len;
    private final int mask;
    private int maxSize;
    private int maxMain, maxIn, maxOut;
    private int sizeMain, sizeIn, sizeOut;
    private int recordCount;
    private CacheObject[] values;

    CacheTQ(CacheWriter writer, int maxKb) {
        this.writer = writer;
        this.maxSize = maxKb * 1024 / 4;
        this.len = MathUtils.nextPowerOf2(maxSize / 64);
        this.mask = len - 1;
        MathUtils.checkPowerOf2(len);
        recalculateMax();
        clear();
    }

    public void clear() {
        headMain.cacheNext = headMain.cachePrevious = headMain;
        headIn.cacheNext = headIn.cachePrevious = headIn;
        headOut.cacheNext = headOut.cachePrevious = headOut;
        // first set to null - avoiding out of memory
        values = null;
        values = new CacheObject[len];
        sizeIn = sizeOut = sizeMain = 0;
        recordCount = 0;
    }

    private void recalculateMax() {
        maxMain = maxSize;
        maxIn = Math.max(1, maxSize * PERCENT_IN / 100);
        maxOut = Math.max(1, maxSize * PERCENT_OUT / 100);
    }

    private void addToFront(CacheObject head, CacheObject rec) {
        if (SysProperties.CHECK) {
            if (rec == head) {
                Message.throwInternalError("try to move head");
            }
            if (rec.cacheNext != null || rec.cachePrevious != null) {
                Message.throwInternalError("already linked");
            }
        }
        rec.cacheNext = head;
        rec.cachePrevious = head.cachePrevious;
        rec.cachePrevious.cacheNext = rec;
        head.cachePrevious = rec;
    }

    private void removeFromList(CacheObject rec) {
        if (SysProperties.CHECK && (rec instanceof CacheHead && rec.cacheQueue != OUT)) {
            Message.throwInternalError();
        }
        rec.cachePrevious.cacheNext = rec.cacheNext;
        rec.cacheNext.cachePrevious = rec.cachePrevious;
        // TODO cache: mystery: why is this required? needs more memory if we
        // don't do this
        rec.cacheNext = null;
        rec.cachePrevious = null;
    }

    public CacheObject get(int pos) {
        CacheObject r = findCacheObject(pos);
        if (r == null) {
            return null;
        }
        if (r.cacheQueue == MAIN) {
            removeFromList(r);
            addToFront(headMain, r);
        } else if (r.cacheQueue == OUT) {
            return null;
        } else if (r.cacheQueue == IN) {
            removeFromList(r);
            sizeIn -= r.getMemorySize();
            sizeMain += r.getMemorySize();
            r.cacheQueue = MAIN;
            addToFront(headMain, r);
        }
        return r;
    }

    private CacheObject findCacheObject(int pos) {
        CacheObject rec = values[pos & mask];
        while (rec != null && rec.getPos() != pos) {
            rec = rec.cacheChained;
        }
        return rec;
    }

    private CacheObject removeCacheObject(int pos) {
        int index = pos & mask;
        CacheObject rec = values[index];
        if (rec == null) {
            return null;
        }
        if (rec.getPos() == pos) {
            values[index] = rec.cacheChained;
        } else {
            CacheObject last;
            do {
                last = rec;
                rec = rec.cacheChained;
                if (rec == null) {
                    return null;
                }
            } while (rec.getPos() != pos);
            last.cacheChained = rec.cacheChained;
        }
        if (!(rec instanceof CacheHead)) {
            recordCount--;
        }
        if (SysProperties.CHECK) {
            rec.cacheChained = null;
        }
        return rec;
    }

    public void remove(int pos) {
        CacheObject r = removeCacheObject(pos);
        if (r != null) {
            removeFromList(r);
            if (r.cacheQueue == MAIN) {
                sizeMain -= r.getMemorySize();
            } else if (r.cacheQueue == IN) {
                sizeIn -= r.getMemorySize();
            }
        }
    }

    private void removeOldIfRequired() throws SQLException {
        // a small method, to allow inlining
        if ((sizeIn >= maxIn) || (sizeMain >= maxMain)) {
            removeOld();
        }
    }

    private void removeOld() throws SQLException {
        int i = 0;
        ObjectArray<CacheObject> changed = ObjectArray.newInstance();
        int si = sizeIn, sm = sizeMain, rc = recordCount;
        CacheObject inNext = headIn.cacheNext, mainNext = headMain.cacheNext;
        while (((si * 4 > maxIn * 3) || (sm * 4 > maxMain * 3))
                && rc > Constants.CACHE_MIN_RECORDS) {
            i++;
            if (i == rc) {
                writer.flushLog();
            }
            if (i >= rc * 2) {
                // can't remove any record, because the log is not written yet
                // hopefully this does not happen too much, but it could happen
                // theoretically
                writer.getTrace().info("Cannot remove records, cache size too small?");
                break;
            }
            if (si > maxIn) {
                CacheObject r = inNext;
                inNext = r.cacheNext;
                if (!r.canRemove()) {
                    if (r != headIn) {
                        removeFromList(r);
                        addToFront(headIn, r);
                    }
                    continue;
                }
                rc--;
                si -= r.getMemorySize();
                if (r.isChanged()) {
                    changed.add(r);
                } else {
                    remove(r);
                }
            } else if (sm > 0) {
                CacheObject r = mainNext;
                mainNext = r.cacheNext;
                if (!r.canRemove() && !(r instanceof CacheHead)) {
                    removeFromList(r);
                    addToFront(headMain, r);
                    continue;
                }
                rc--;
                sm -= r.getMemorySize();
                if (r.isChanged()) {
                    changed.add(r);
                } else {
                    remove(r);
                }
            }
        }
        if (changed.size() > 0) {
            int mm = maxMain;
            int mi = maxIn;
            try {
                // temporary disable size checking,
                // to avoid stack overflow
                maxMain = Integer.MAX_VALUE;
                maxIn = Integer.MAX_VALUE;
                CacheObject.sort(changed);
                for (i = 0; i < changed.size(); i++) {
                    CacheObject rec = changed.get(i);
                    writer.writeBack(rec);
                }
            } finally {
                maxMain = mm;
                maxIn = mi;
            }
            for (i = 0; i < changed.size(); i++) {
                CacheObject rec = changed.get(i);
                remove(rec);
            }
        }
    }

    private void remove(CacheObject r) {
        int pos = r.getPos();
        removeCacheObject(pos);
        removeFromList(r);
        if (r.cacheQueue == IN) {
            // remove the record from the IN queue
            sizeIn -= r.getMemorySize();
            // replace it with an OUT record
            r = new CacheHead();
            r.setPos(pos);
            r.cacheQueue = OUT;
            putCacheObject(r);
            addToFront(headOut, r);
            sizeOut++;
            while (sizeOut >= maxOut) {
                r = headOut.cacheNext;
                sizeOut--;
                removeCacheObject(r.getPos());
                removeFromList(r);
            }
        } else if (r.cacheQueue == MAIN) {
            sizeMain -= r.getMemorySize();
        } else {
            throw Message.throwInternalError();
        }
    }

    public ObjectArray<CacheObject> getAllChanged() {
        ObjectArray<CacheObject> list = ObjectArray.newInstance();
        for (CacheObject o = headMain.cacheNext; o != headMain; o = o.cacheNext) {
            if (o.isChanged()) {
                list.add(o);
            }
        }
        for (CacheObject o = headIn.cacheNext; o != headIn; o = o.cacheNext) {
            if (o.isChanged()) {
                list.add(o);
            }
        }
        CacheObject.sort(list);
        return list;
    }

    public CacheObject find(int pos) {
        CacheObject o = findCacheObject(pos);
        if (o != null && o.cacheQueue != OUT) {
            return o;
        }
        return null;
    }

    private void putCacheObject(CacheObject rec) {
        if (SysProperties.CHECK) {
            for (int i = 0; i < rec.getBlockCount(); i++) {
                CacheObject old = find(rec.getPos() + i);
                if (old != null) {
                    Message.throwInternalError("try to add a record twice i=" + i);
                }
            }
        }
        int index = rec.getPos() & mask;
        rec.cacheChained = values[index];
        values[index] = rec;
        if (!(rec instanceof CacheHead)) {
            recordCount++;
        }
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
        int pos = rec.getPos();
        CacheObject r = findCacheObject(pos);
        if (r != null) {
            if (r.cacheQueue == OUT) {
                removeCacheObject(pos);
                removeFromList(r);
                removeOldIfRequired();
                rec.cacheQueue = MAIN;
                putCacheObject(rec);
                addToFront(headMain, rec);
                sizeMain += rec.getMemorySize();
            }
        } else if (sizeMain < maxMain) {
            removeOldIfRequired();
            rec.cacheQueue = MAIN;
            putCacheObject(rec);
            addToFront(headMain, rec);
            sizeMain += rec.getMemorySize();
        } else {
            removeOldIfRequired();
            rec.cacheQueue = IN;
            putCacheObject(rec);
            addToFront(headIn, rec);
            sizeIn += rec.getMemorySize();
        }
    }

    public CacheObject update(int pos, CacheObject rec) throws SQLException {
        CacheObject old = find(pos);
        if (old == null || old.cacheQueue == OUT) {
            put(rec);
        } else {
            if (old == rec) {
                if (rec.cacheQueue == MAIN) {
                    removeFromList(rec);
                    addToFront(headMain, rec);
                }
            }
        }
        return old;
    }

    public void setMaxSize(int maxKb) throws SQLException {
        int newSize = maxKb * 1024 / 4;
        maxSize = newSize < 0 ? 0 : newSize;
        recalculateMax();
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
        return sizeIn + sizeOut + sizeMain;
    }

}
