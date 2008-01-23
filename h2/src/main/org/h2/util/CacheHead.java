/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.sql.SQLException;

import org.h2.store.DataPage;

/**
 * The head element of the linked list.
 */
public class CacheHead extends CacheObject {

    public int getByteCount(DataPage dummy) throws SQLException {
        return 0;
    }

    public void write(DataPage buff) throws SQLException {
    }

    public boolean canRemove() {
        return false;
    }

}
