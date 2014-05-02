/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.sql.Connection;
import org.h2.util.SmallLRUCache;
import org.h2.util.TempFileDeleter;

/**
 * A data handler contains a number of callback methods.
 * The most important implementing class is a database.
 */
public interface DataHandler {

    /**
     * Get the database path.
     *
     * @return the database path
     */
    String getDatabasePath();

    /**
     * Open a file at the given location.
     *
     * @param name the file name
     * @param mode the mode
     * @param mustExist whether the file must already exist
     * @return the file
     */
    FileStore openFile(String name, String mode, boolean mustExist);

    /**
     * Check if the simulated power failure occurred.
     * This call will decrement the countdown.
     *
     * @throws SQLException if the simulated power failure occurred
     */
    void checkPowerOff();

    /**
     * Check if writing is allowed.
     *
     * @throws SQLException if it is not allowed
     */
    void checkWritingAllowed();

    /**
     * Free up disk space if possible.
     * This method is called if more space is needed.
     *
     * @throws SQLException if no more space could be freed
     */
    void freeUpDiskSpace();

    /**
     * Get the maximum length of a in-place large object
     *
     * @return the maximum size
     */
    int getMaxLengthInplaceLob();

    /**
     * Get the compression algorithm used for large objects.
     *
     * @param type the data type (CLOB or BLOB)
     * @return the compression algorithm, or null
     */
    String getLobCompressionAlgorithm(int type);

    /**
     * Get the temp file deleter mechanism.
     *
     * @return the temp file deleter
     */
    TempFileDeleter getTempFileDeleter();

    /**
     * Get the synchronization object for lob operations.
     *
     * @return the synchronization object
     */
    Object getLobSyncObject();

    /**
     * Get the lob file list cache if it is used.
     *
     * @return the cache or null
     */
    SmallLRUCache<String, String[]> getLobFileListCache();

    /**
     * Get the lob storage mechanism to use.
     *
     * @return the lob storage mechanism
     */
    LobStorage getLobStorage();

    /**
     * Get a database connection to be used for LOB access.
     *
     * @return the connection or null
     */
    Connection getLobConnection();

}
