/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.LinkedHashMap;
import java.util.Map;

public class SmallLRUCache extends LinkedHashMap {
    
    private static final long serialVersionUID = 3643268440910181829L;
    private int size;
    
    public SmallLRUCache(int size) {
        this.size = size;
    }
    
    protected boolean removeEldestEntry(Map.Entry eldest) {
        return size() > size;
     }
}
