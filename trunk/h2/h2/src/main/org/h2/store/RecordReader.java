/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.sql.SQLException;
import org.h2.engine.Session;

/**
 * A record reader is able to create a {@link Record} from a {@link DataPage}.
 */
public interface RecordReader {
    Record read(Session session, DataPage s) throws SQLException;
}
