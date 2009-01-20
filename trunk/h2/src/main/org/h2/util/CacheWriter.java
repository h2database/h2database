/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.sql.SQLException;

import org.h2.message.Trace;

/**
 * The cache writer is called by the cache to persist changed data that needs to
 * be removed from the cache.
 */
public interface CacheWriter {

    /**
     * Persist a record.
     *
     * @param entry the cache entry
     */
    void writeBack(CacheObject entry) throws SQLException;

    /**
     * Flush the log file, so that entries can be removed from the cache. This
     * is only required if the cache is full and contains data that is not yet
     * written to the log file. It is required to write the log entries to the
     * log file first, because the log file is 'write ahead'.
     */
    void flushLog() throws SQLException;

    /**
     * Get the trace writer.
     *
     * @return the trace writer
     */
    Trace getTrace();

}
