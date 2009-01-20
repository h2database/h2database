/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.sql.SQLException;
import org.h2.engine.Session;

/**
 * A record reader is able to create a {@link Record} from a {@link DataPage}.
 */
public interface RecordReader {

    /**
     * Read a record from the data page.
     *
     * @param session the session
     * @param s the data page
     * @return the record
     */
    Record read(Session session, DataPage s) throws SQLException;
}
