/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.web;

import org.h2.util.StringUtils;

public class ConnectionInfo {
    String name, driver, url, user;
    int lastAccess;

    ConnectionInfo() {
    }

    ConnectionInfo(String data) {
        String[] array = StringUtils.arraySplit(data, '|', false);
        name = get(array, 0);
        driver = get(array, 1);
        url  = get(array, 2);
        user = get(array, 3);
    }

    private String get(String[] array, int i) {
        return array != null && array.length>i ? array[i] : "";
    }

    String getString() {
        return StringUtils.arrayCombine(new String[]{name, driver, url, user}, '|');
    }

}
