/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.lang.ref.SoftReference;

import org.h2.constant.SysProperties;

/**
 * The string cache helps re-use string objects and therefore save memory.
 * It uses a soft reference cache to keep frequently used strings in memory.
 * The effect is similar to calling String.intern(), but faster.
 */
public class StringCache {
    
    private static final boolean ENABLED = true;
    private static SoftReference softCache = new SoftReference(null);
    
    private StringCache() {
        // utility class
    }

    // testing: cacheHit / miss are public!
    // public static int cacheHit = 0, cacheMiss = 0;

    // 4703
//    public static String get(String s) {
//        if (s == null) {
//            return s;
//        } else if (s.length() == 0) {
//            return "";
//        }
//        if (!Constants.USE_OBJECT_CACHE 
//                || !ENABLED || s.length() > MAX_CACHE_SIZE / 10) {
//            return s;
//        }
//        int hash = s.hashCode();
//        int index = hash & (Constants.OBJECT_CACHE_SIZE - 1);
//        String cached = cache[index];
//        if (cached != null) {
//            if (s.equals(cached)) {
//                // cacheHit++;
//                return cached;
//            }
//        }
//        // cacheMiss++;
//        replace(index, s);
//        return s;
//    }

    // 3500
//    public static String get(String s) {
//        return s;
//    }

    // 3906
    /**
     * Get the string from the cache if possible. If the string has not been
     * found, it is added to the cache. If there is such a string in the cache,
     * that one is returned.
     * 
     * @param s the original string
     * @return a string with the same content, if possible from the cache
     */
    public static String get(String s) {
        if (!SysProperties.OBJECT_CACHE || !ENABLED) {
            return s;
        }
        if (s == null) {
            return s;
        } else if (s.length() == 0) {
            return "";
        }
        String[] cache = (String[]) softCache.get();
        int hash = s.hashCode();
        if (cache == null) {
            cache = new String[SysProperties.OBJECT_CACHE_SIZE];
            softCache = new SoftReference(cache);
        }
        int index = hash & (SysProperties.OBJECT_CACHE_SIZE - 1);
        String cached = cache[index];
        if (cached != null) {
            if (s.equals(cached)) {
                // cacheHit++;
                return cached;
            }
        }
        cache[index] = s;
        return s;
    }

    /**
     * Get a string from the cache, and if no such string has been found, create
     * a new one with only this content. This solves out of memory problems if
     * the string is a substring of another, large string. In Java, strings are
     * shared, which could lead to memory problems. This avoid such problems.
     * 
     * @param s the string
     * @return a string that is guaranteed not be a substring of a large string
     */
    public static String getNew(String s) {
        if (!SysProperties.OBJECT_CACHE || !ENABLED) {
            return s;
        }
        if (s == null) {
            return s;
        } else if (s.length() == 0) {
            return "";
        }
        String[] cache = (String[]) softCache.get();
        int hash = s.hashCode();
        if (cache == null) {
            cache = new String[SysProperties.OBJECT_CACHE_SIZE];
            softCache = new SoftReference(cache);
        }
        int index = hash & (SysProperties.OBJECT_CACHE_SIZE - 1);
        String cached = cache[index];
        if (cached != null) {
            if (s.equals(cached)) {
                // cacheHit++;
                return cached;
            }
        }
        // create a new object that is not shared 
        // (to avoid out of memory if it is a substring of a big String)
        // NOPMD
        s = new String(s); 
        cache[index] = s;
        return s;
    }

    /**
     * Clear the cache. This method is used for testing.
     */
    public static void clearCache() {
        softCache = new SoftReference(null);
    }

}
