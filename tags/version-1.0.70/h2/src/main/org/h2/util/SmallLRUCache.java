/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

//## Java 1.4 begin ##
import java.util.LinkedHashMap;
//## Java 1.4 end ##
/*## Java 1.3 only begin ##
import java.util.HashMap;
## Java 1.3 only end ##*/
import java.util.Map;

/**
 * This class implements a small LRU object cache.
 */
public class SmallLRUCache
//## Java 1.4 begin ##
extends LinkedHashMap
//## Java 1.4 end ##
/*## Java 1.3 only begin ##
extends HashMap
## Java 1.3 only end ##*/
{

    private static final long serialVersionUID = 3643268440910181829L;
    private int size;

    public SmallLRUCache(int size) {
        this.size = size;
    }

//## Java 1.4 begin ##
    protected boolean removeEldestEntry(Map.Entry eldest) {
        return size() > size;
     }
//## Java 1.4 end ##
}
