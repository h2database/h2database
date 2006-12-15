/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.web;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Locale;

public class WebServerSession {
    long lastAccess;
    HashMap map = new HashMap();
    Locale locale;
    WebServer server;
    
    WebServerSession(WebServer server) {
        this.server = server;
    }
    
    public void put(String key, Object value) {
        map.put(key, value);
    }
    
    public Object get(String key) {
        if("sessions".equals(key)) {
            return server.getSessions();
        }
        return map.get(key);
    }

    public void remove(String key) {
        map.remove(key);
    }

    public HashMap getInfo() {
        HashMap m = new HashMap();
        m.putAll(map);
        m.put("lastAccess", new Timestamp(lastAccess).toString());
        return m;
    }
}
