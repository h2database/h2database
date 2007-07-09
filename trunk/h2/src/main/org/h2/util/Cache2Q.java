/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.sql.SQLException;

import org.h2.engine.Constants;
import org.h2.message.Message;

/**
 * For details about the 2Q algorithm, see http://www.vldb.org/conf/1994/P439.PDF.
 * However, items are moved from 'in' queue and move to the 'main' queue if the are referenced again.
 */
public class Cache2Q implements Cache {
    
    public static final String TYPE_NAME = "TQ";
    private static final int MAIN = 1, IN = 2, OUT = 3;
    
    private final CacheWriter writer;
    private int maxSize;
    private int percentIn = 20, percentOut = 50;
    private int maxMain, maxIn, maxOut; 
    private CacheObject headMain = new CacheHead();
    private CacheObject headIn = new CacheHead();
    private CacheObject headOut = new CacheHead();
    private int sizeMain, sizeIn, sizeOut, sizeRecords;
    private int len;
    private CacheObject[] values;
    private int mask;
    
    public Cache2Q(CacheWriter writer, int maxSize) {
        this.writer = writer;
        resize(maxSize);
    }
    
    private void resize(int maxSize) {
        this.maxSize = maxSize;
        this.len = MathUtils.nextPowerOf2(maxSize / 2);
        this.mask = len - 1;
        MathUtils.checkPowerOf2(len);
        clear();
    }
    
    public void clear() {    
        headMain.next = headMain.previous = headMain;
        headIn.next = headIn.previous = headIn;
        headOut.next = headOut.previous = headOut;
        values = new CacheObject[len];
        sizeIn = sizeOut = sizeMain = 0;
        sizeRecords = 0;
        recalculateMax();
    }
    
    void setPercentIn(int percent) {
        percentIn = percent;
        recalculateMax();
    }

    void setPercentOut(int percent) {
        percentOut = percent;
        recalculateMax();
    }

    private void recalculateMax() {
        maxMain = maxSize;
        maxIn = maxSize * percentIn / 100;
        maxOut = maxSize * percentOut / 100;
    }
    
    private void addToFront(CacheObject head, CacheObject rec) {
        if(Constants.CHECK) {
            if(rec == head) {
                throw Message.getInternalError("try to move head");
            }
            if(rec.next != null || rec.previous != null) {
                throw Message.getInternalError("already linked");
            }
        }
        rec.next = head;
        rec.previous = head.previous;
        rec.previous.next = rec;
        head.previous = rec;
    }
    
    private void removeFromList(CacheObject rec) {
        if(Constants.CHECK && (rec instanceof CacheHead && rec.cacheQueue != OUT)) {
            throw Message.getInternalError();
        }        
        rec.previous.next = rec.next;
        rec.next.previous = rec.previous;
        // TODO cache: mystery: why is this required? needs more memory if we don't do this
        rec.next = null;
        rec.previous = null;
    }    

    public CacheObject get(int pos) {
        CacheObject r = findCacheObject(pos);
        if(r==null) {
            return null;
        }
        if(r.cacheQueue == MAIN) {
            removeFromList(r);
            addToFront(headMain, r);
        } else if(r.cacheQueue == OUT) {
            return null;
        } else if(r.cacheQueue == IN) {
            removeFromList(r);
            sizeIn -= r.getBlockCount();
            sizeMain += r.getBlockCount();
            r.cacheQueue = MAIN;
            addToFront(headMain, r);
        }
        return r;
    }

    private CacheObject findCacheObject(int pos) {
        CacheObject rec = values[pos & mask];
        while(rec != null && rec.getPos() != pos) {
            rec = rec.chained;
        }
        return rec;
    }
    
    private CacheObject removeCacheObject(int pos) {
        int index = pos & mask;
        CacheObject rec = values[index];
        if(rec == null) {
            return null;
        }
        if(rec.getPos() == pos) {
            values[index] = rec.chained;
        } else {
            CacheObject last;
            do {
                last = rec;
                rec = rec.chained;
                if(rec == null) {
                    return null;
                }
            } while(rec.getPos() != pos);
            last.chained = rec.chained;
        }
        sizeRecords--;
        if(Constants.CHECK) {
            rec.chained = null;
        }
        return rec;
    }

    public void remove(int pos) {
        CacheObject r = removeCacheObject(pos);
        if(r != null) {
            removeFromList(r);
            if(r.cacheQueue == MAIN) {
                sizeMain -= r.getBlockCount();
            } else if(r.cacheQueue == IN) {
                sizeIn -= r.getBlockCount();
            }
        }
    }

    private void removeOld() throws SQLException {
        if((sizeIn < maxIn) && (sizeOut < maxOut) && (sizeMain < maxMain)) {
            return;
        }
        int i=0;
        ObjectArray changed = new ObjectArray();        
        while (((sizeIn*4 > maxIn*3) || (sizeOut*4 > maxOut*3) || (sizeMain*4 > maxMain*3)) && sizeRecords > Constants.CACHE_MIN_RECORDS) {        
            if(i++ >= sizeRecords) {
                // can't remove any record, because the log is not written yet
                // hopefully this does not happen too much, but it could happen theoretically
                // TODO log this
                break;
            }            
            if (sizeIn > maxIn) {
                CacheObject r = headIn.next;
                if(!r.canRemove()) {
                    removeFromList(r);
                    addToFront(headIn, r);
                    continue;
                }
                sizeIn -= r.getBlockCount();
                int pos = r.getPos();
                removeCacheObject(pos);
                removeFromList(r);
                if(r.isChanged()) {
                    changed.add(r);
                }
                r = new CacheHead();
                r.setPos(pos);
                r.cacheQueue = OUT;
                putCacheObject(r);
                addToFront(headOut, r);
                sizeOut++;
                if (sizeOut >= maxOut) {
                    r = headOut.next;
                    sizeOut--;
                    removeCacheObject(r.getPos());
                    removeFromList(r);
                }
            } else {
                CacheObject r = headMain.next;
                if(!r.canRemove()) {
                    removeFromList(r);
                    addToFront(headMain, r);
                    continue;
                }
                sizeMain -= r.getBlockCount();                
                removeCacheObject(r.getPos());
                removeFromList(r);
                if(r.isChanged()) {
                    changed.add(r);
                }                
            }
        }
        if(changed.size() > 0) {
            CacheObject.sort(changed);
            for(i=0; i<changed.size(); i++) {
                CacheObject rec = (CacheObject) changed.get(i);
                writer.writeBack(rec);
            }
        }
    }

    public ObjectArray getAllChanged() {
        ObjectArray list = new ObjectArray();
        for(CacheObject o = headMain.next; o != headMain; o = o.next) {
            if(o.isChanged()) {
                list.add(o);
            }
        }
        for(CacheObject o = headIn.next; o != headIn; o = o.next) {
            if(o.isChanged()) {
                list.add(o);
            }
        }
        CacheObject.sort(list);
        return list;
    }

    public CacheObject find(int pos) {
        CacheObject o = findCacheObject(pos);
        if(o != null && o.cacheQueue != OUT) {
            return o;
        }
        return null;
    }
    
    private void putCacheObject(CacheObject rec) {
        if(Constants.CHECK) {
            for(int i=0; i<rec.getBlockCount(); i++) {
                CacheObject old = find(rec.getPos() + i);
                if(old != null)  {
                    throw Message.getInternalError("try to add a record twice i="+i);
                }
            }
        }
        int index = rec.getPos() & mask;
        rec.chained = values[index];
        values[index] = rec;
        sizeRecords++;
    }
    

    public void put(CacheObject rec) throws SQLException {
        int pos = rec.getPos();
        CacheObject r = findCacheObject(pos);
        if(r != null) {
            if(r.cacheQueue == OUT) {
                removeCacheObject(pos);
                removeFromList(r);
                removeOld();
                rec.cacheQueue = MAIN;
                putCacheObject(rec);
                addToFront(headMain, rec);
                sizeMain += rec.getBlockCount();
            }
        } else if(sizeMain < maxMain) {
            removeOld();
            rec.cacheQueue = MAIN;
            putCacheObject(rec);
            addToFront(headMain, rec);
            sizeMain += rec.getBlockCount();
        } else {
            removeOld();
            rec.cacheQueue = IN;
            putCacheObject(rec);
            addToFront(headIn, rec);
            sizeIn += rec.getBlockCount();
        }
    }

    public CacheObject update(int pos, CacheObject rec) throws SQLException {
        CacheObject old = find(pos);
        if(old == null || old.cacheQueue == OUT) {
            put(rec);
        } else {
            if(old == rec) {
                if(rec.cacheQueue == MAIN) {
                    removeFromList(rec);
                    addToFront(headMain, rec);
                }
            }
        }
        return old;
    }
    
    public void setMaxSize(int newSize) throws SQLException {
        maxSize = newSize < 0 ? 0 : newSize;
        recalculateMax();
        // resize(maxSize);
        removeOld();
    }
    
    public String getTypeName() {
        return TYPE_NAME;
    }

}
