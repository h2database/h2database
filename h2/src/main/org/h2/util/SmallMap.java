/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;

import org.h2.message.Message;

public class SmallMap {
    private HashMap map = new HashMap();
    private Object cache;
    private int cacheId;
    private int lastId;
    private int maxElements;
    
    public SmallMap(int maxElements) {
        this.maxElements = maxElements;
    }

    public int addObject(int id, Object o) {
        if(map.size() > maxElements * 2) {
            Iterator it = map.keySet().iterator();
            while(it.hasNext()) {
                Integer k = (Integer) it.next();
                if(k.intValue() + maxElements < lastId) {
                    it.remove();
                }
            }
        }
        if(id > lastId) {
            lastId = id;
        }
        map.put(ObjectUtils.getInteger(id), o);
        cacheId = id;
        cache = o;
        return id;
    }

    public void freeObject(int id) {
        if (cacheId == id) {
            cacheId = -1;
            cache = null;
        }
        map.remove(ObjectUtils.getInteger(id));
    }

    public Object getObject(int id, boolean ifAvailable) throws SQLException {
        if (id == cacheId) {
            return cache;
        }
        Object obj = map.get(ObjectUtils.getInteger(id));
        if(obj == null && !ifAvailable) {
            throw Message.getSQLException(Message.OBJECT_CLOSED);
        }
        return obj;
    }
    
}
