/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.web;

import org.h2.util.MathUtils;
import org.h2.util.StringUtils;

/**
 * The connection info object is a wrapper for database connection information
 * such as the database URL, user name and password.
 * This class is used by the H2 Console.
 */
public class ConnectionInfo implements Comparable<ConnectionInfo> {
    /**
     * The driver class name.
     */
    public String driver;

    /**
     * The database URL.
     */
    public String url;

    /**
     * The user name.
     */
    public String user;

    /**
     * The connection display name.
     */
    String name;

    /**
     * The last time this connection was used.
     */
    int lastAccess;

    ConnectionInfo() {
        // nothing to do
    }

    public ConnectionInfo(String data) {
        String[] array = StringUtils.arraySplit(data, '|', false);
        name = get(array, 0);
        driver = get(array, 1);
        url = get(array, 2);
        user = get(array, 3);
    }

    private static String get(String[] array, int i) {
        return array != null && array.length > i ? array[i] : "";
    }

    String getString() {
        return StringUtils.arrayCombine(new String[] { name, driver, url, user }, '|');
    }

    @Override
    public int compareTo(ConnectionInfo o) {
        return -MathUtils.compareInt(lastAccess, o.lastAccess);
    }

}
