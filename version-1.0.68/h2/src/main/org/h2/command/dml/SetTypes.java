/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import org.h2.util.ObjectArray;

/**
 * The list of setting for a SET statement.
 */
public class SetTypes {

    public static final int IGNORECASE = 1, MAX_LOG_SIZE = 2, MODE = 3, READONLY = 4, LOCK_TIMEOUT = 5;
    public static final int DEFAULT_LOCK_TIMEOUT = 6, DEFAULT_TABLE_TYPE = 7;
    public static final int CACHE_SIZE = 8;
    public static final int TRACE_LEVEL_SYSTEM_OUT = 9, TRACE_LEVEL_FILE = 10, TRACE_MAX_FILE_SIZE = 11;
    public static final int COLLATION = 12, CLUSTER = 13, WRITE_DELAY = 14, DATABASE_EVENT_LISTENER = 15;
    public static final int MAX_MEMORY_ROWS = 16, LOCK_MODE = 17, DB_CLOSE_DELAY = 18;
    public static final int LOG = 19, THROTTLE = 20, MAX_MEMORY_UNDO = 21, MAX_LENGTH_INPLACE_LOB = 22;
    public static final int COMPRESS_LOB = 23, ALLOW_LITERALS = 24, MULTI_THREADED = 25, SCHEMA = 26;
    public static final int OPTIMIZE_REUSE_RESULTS = 27, SCHEMA_SEARCH_PATH = 28, UNDO_LOG = 29;
    public static final int REFERENTIAL_INTEGRITY = 30, MVCC = 31, MAX_OPERATION_MEMORY = 32, EXCLUSIVE = 33;
    public static final int CREATE_BUILD = 34, VARIABLE = 35, QUERY_TIMEOUT = 36;

    private static ObjectArray types = new ObjectArray();

    static {
        setType(IGNORECASE, "IGNORECASE");
        setType(MAX_LOG_SIZE, "MAX_LOG_SIZE");
        setType(MODE, "MODE");
        setType(READONLY, "READONLY");
        setType(LOCK_TIMEOUT, "LOCK_TIMEOUT");
        setType(DEFAULT_LOCK_TIMEOUT, "DEFAULT_LOCK_TIMEOUT");
        setType(DEFAULT_TABLE_TYPE, "DEFAULT_TABLE_TYPE");
        setType(CACHE_SIZE, "CACHE_SIZE");
        setType(TRACE_LEVEL_SYSTEM_OUT, "TRACE_LEVEL_SYSTEM_OUT");
        setType(TRACE_LEVEL_FILE, "TRACE_LEVEL_FILE");
        setType(TRACE_MAX_FILE_SIZE, "TRACE_MAX_FILE_SIZE");
        setType(COLLATION, "COLLATION");
        setType(CLUSTER, "CLUSTER");
        setType(WRITE_DELAY, "WRITE_DELAY");
        setType(DATABASE_EVENT_LISTENER, "DATABASE_EVENT_LISTENER");
        setType(MAX_MEMORY_ROWS, "MAX_MEMORY_ROWS");
        setType(LOCK_MODE, "LOCK_MODE");
        setType(DB_CLOSE_DELAY, "DB_CLOSE_DELAY");
        setType(LOG, "LOG");
        setType(THROTTLE, "THROTTLE");
        setType(MAX_MEMORY_UNDO, "MAX_MEMORY_UNDO");
        setType(MAX_LENGTH_INPLACE_LOB, "MAX_LENGTH_INPLACE_LOB");
        setType(COMPRESS_LOB, "COMPRESS_LOB");
        setType(ALLOW_LITERALS, "ALLOW_LITERALS");
        setType(MULTI_THREADED, "MULTI_THREADED");
        setType(SCHEMA, "SCHEMA");
        setType(OPTIMIZE_REUSE_RESULTS, "OPTIMIZE_REUSE_RESULTS");
        setType(SCHEMA_SEARCH_PATH, "SCHEMA_SEARCH_PATH");
        setType(UNDO_LOG, "UNDO_LOG");
        setType(REFERENTIAL_INTEGRITY, "REFERENTIAL_INTEGRITY");
        setType(MVCC, "MVCC");
        setType(MAX_OPERATION_MEMORY, "MAX_OPERATION_MEMORY");
        setType(EXCLUSIVE, "EXCLUSIVE");
        setType(CREATE_BUILD, "CREATE_BUILD");
        setType(VARIABLE, "@");
        setType(QUERY_TIMEOUT, "QUERY_TIMEOUT");
    }

    private static void setType(int type, String name) {
        while (types.size() <= type) {
            types.add(null);
        }
        types.set(type, name);
    }

    /**
     * Get the set type number.
     * 
     * @param name the set type name
     * @return the number
     */
    public static int getType(String name) {
        for (int i = 0; i < types.size(); i++) {
            if (name.equals(types.get(i))) {
                return i;
            }
        }
        return -1;
    }

    public static ObjectArray getSettings() {
        return types;
    }

    /**
     * Get the set type name.
     * 
     * @param type the type number
     * @return the name
     */
    public static String getTypeName(int type) {
        return (String) types.get(type);
    }

}
