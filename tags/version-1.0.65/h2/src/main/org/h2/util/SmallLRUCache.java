/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

//#ifdef JDK14
import java.util.LinkedHashMap;
//#endif
//#ifdef JDK13
/*
import java.util.HashMap;
*/
//#endif
import java.util.Map;

/**
 * This class implements a small LRU object cache.
 */
public class SmallLRUCache
//#ifdef JDK14
extends LinkedHashMap
//#endif
//#ifdef JDK13
/*
extends HashMap
*/
//#endif
{

    private static final long serialVersionUID = 3643268440910181829L;
    private int size;

    public SmallLRUCache(int size) {
        this.size = size;
    }

//#ifdef JDK14
    protected boolean removeEldestEntry(Map.Entry eldest) {
        return size() > size;
     }
//#endif
}
