/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: Jan Kotek
 */
package org.h2.util;

import java.util.ArrayList;
import java.util.Map;

/**
 * Cache which wraps another cache (proxy pattern) and adds caching using map.
 * This is useful for WeakReference, SoftReference or hard reference cache.
 */
class CacheSecondLevel implements Cache {

    private final Cache baseCache;
    private final Map<Integer, CacheObject> map;

    CacheSecondLevel(Cache cache, Map<Integer, CacheObject> map) {
        this.baseCache = cache;
        this.map = map;
    }

    public void clear() {
        map.clear();
        baseCache.clear();
    }

    public CacheObject find(int pos) {
        CacheObject ret = baseCache.find(pos);
        if (ret == null) {
            ret = map.get(pos);
        }
        return ret;
    }

    public CacheObject get(int pos) {
        CacheObject ret = baseCache.get(pos);
        if (ret == null) {
            ret = map.get(pos);
        }
        return ret;
    }

    public ArrayList<CacheObject> getAllChanged() {
        return baseCache.getAllChanged();
    }

    public int getMaxSize() {
        return baseCache.getMaxSize();
    }

    public int getSize() {
        return baseCache.getSize();
    }

    public void put(CacheObject r) {
        baseCache.put(r);
        map.put(r.getPos(), r);
    }

    public void remove(int pos) {
        baseCache.remove(pos);
        map.remove(pos);
    }

    public void setMaxSize(int size) {
        baseCache.setMaxSize(size);
    }

    public CacheObject update(int pos, CacheObject record) {
        CacheObject oldRec = baseCache.update(pos, record);
        map.put(pos, record);
        return oldRec;
    }

}
