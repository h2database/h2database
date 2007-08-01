/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.Collections;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Vector;

/**
 * Sorted properties file. 
 * This implementation requires that store() internally calls keys().
 */
public class SortedProperties extends Properties {
    
    private static final long serialVersionUID = 5657650728102821923L;

    public synchronized Enumeration keys() {
        Vector v = new Vector(keySet());
        Collections.sort(v);
        return v.elements();
    }
}