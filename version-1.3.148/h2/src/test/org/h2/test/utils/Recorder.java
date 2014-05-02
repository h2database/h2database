/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.utils;

/**
 * A recorder for the recording file system.
 */
public interface Recorder {

    /**
     * Copy a file. The file name contains the source and the target file
     * separated with a colon.
     */
    int COPY = 3;

    /**
     * Create all parent directories.
     */
    int CREATE_DIRS = 4;

    /**
     * Create a new file.
     */
    int CREATE_NEW_FILE = 5;

    /**
     * Create a temporary file.
     */
    int CREATE_TEMP_FILE = 6;

    /**
     * Delete a file.
     */
    int DELETE = 7;

    /**
     * Delete all files and directories recursively.
     */
    int DELETE_RECURSIVE = 8;

    /**
     * Open a file output stream.
     */
    int OPEN_OUTPUT_STREAM = 9;

    /**
     * Rename a file. The file name contains the source and the target file
     * separated with a colon.
     */
    int RENAME = 10;

    /**
     * Set the length of the file.
     */
    int SET_LENGTH = 1;

    /**
     * Try to delete the file.
     */
    int TRY_DELETE = 2;

    /**
     * Write to the file.
     */
    int WRITE = 0;

    /**
     * Record the method.
     *
     * @param op the operation
     * @param fileName the file name or file name list
     * @param data the data or null
     * @param x the value or 0
     */
    void log(int op, String fileName, byte[] data, long x);

}
