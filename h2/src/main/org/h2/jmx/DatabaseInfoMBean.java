/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jmx;

/**
 * Information and management operations for the given database.
 *
 * @author Eric Dong
 * @author Thomas Mueller
 */
public interface DatabaseInfoMBean {

    /**
     * Is the database open in exclusive mode?
     *
     * @return true if the database is open in exclusive mode, false otherwise
     */
    boolean isExclusive();

    /**
     * Is the database read-only?
     *
     * @return true if the database is read-only, false otherwise
     */
    boolean isReadOnly();

    /**
     * The database compatibility mode (REGULAR if no compatibility mode is
     * used).
     *
     * @return the database mode
     */
    String getMode();

    /**
     * The number of write operations since the database was opened.
     *
     * @return the write count
     */
    long getFileWriteCount();

    /**
     * The file read count since the database was opened.
     *
     * @return the read count
     */
    long getFileReadCount();

    /**
     * The database file size in KB.
     *
     * @return the number of pages
     */
    long getFileSize();

    /**
     * The maximum cache size in KB.
     *
     * @return the maximum size
     */
    int getCacheSizeMax();

    /**
     * Change the maximum size.
     *
     * @param kb the cache size in KB.
     */
    void setCacheSizeMax(int kb);

    /**
     * The current cache size in KB.
     *
     * @return the current size
     */
    int getCacheSize();

    /**
     * The database version.
     *
     * @return the version
     */
    String getVersion();

    /**
     * The trace level (0 disabled, 1 error, 2 info, 3 debug).
     *
     * @return the level
     */
    int getTraceLevel();

    /**
     * Set the trace level.
     *
     * @param level the new value
     */
    void setTraceLevel(int level);

    /**
     * List the database settings.
     *
     * @return the database settings
     */
    String listSettings();

    /**
     * List sessions, including the queries that are in
     * progress, and locked tables.
     *
     * @return information about the sessions
     */
    String listSessions();

}
