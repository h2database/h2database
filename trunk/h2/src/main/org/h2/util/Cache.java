/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.sql.SQLException;

public interface Cache {

    ObjectArray getAllChanged();

    void clear();

    CacheObject get(int pos);

    void put(CacheObject r) throws SQLException;

    CacheObject update(int pos, CacheObject record) throws SQLException;

    void remove(int pos);

    CacheObject find(int i);

    /*
     * @param memorySize in number of double words (4 bytes)
     */
    void setMaxSize(int memorySize) throws SQLException;

    String getTypeName();
}
