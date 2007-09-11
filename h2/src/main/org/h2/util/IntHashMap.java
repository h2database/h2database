package org.h2.util;

import java.util.Collection;
import java.util.HashMap;

public class IntHashMap {
    private final HashMap map = new HashMap();
    
    public Object get(int key) {
        return map.get(ObjectUtils.getInteger(key));
    }
    
    public void put(int key, Object value) {
        map.put(ObjectUtils.getInteger(key), value);
    }
    
    public void remove(int key) {
        map.remove(ObjectUtils.getInteger(key));
    }

    public int size() {
        return map.size();
    }

    public void clear() {
        map.clear();
    }
    
    public Collection values() {
        return map.values();
    }
}
