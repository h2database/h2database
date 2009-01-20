/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.engine.Session;
import org.h2.result.Row;

/**
 * An index that can address individual rows directly.
 */
public interface RowIndex extends Index {

    /**
     * Get the row with the given key.
     *
     * @param session the session
     * @param key the position
     * @return the row
     */
    Row getRow(Session session, int key) throws SQLException;

}
