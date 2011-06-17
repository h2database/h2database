/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import org.h2.util.ObjectArray;

/**
 * The list of setting for a SET statement.
 */
public class SetTypes {

    /**
     * The type of a SET IGNORECASE statement.
     */
    public static final int IGNORECASE = 1;

    /**
     * The type of a SET MAX_LOG_SIZE statement.
     */
    public static final int MAX_LOG_SIZE = 2;

    /**
     * The type of a SET MODE statement.
     */
    public static final int MODE = 3;

    /**
     * The type of a SET READONLY statement.
     */
    public static final int READONLY = 4;

    /**
     * The type of a SET LOCK_TIMEOUT statement.
     */
    public static final int LOCK_TIMEOUT = 5;

    /**
     * The type of a SET DEFAULT_LOCK_TIMEOUT statement.
     */
    public static final int DEFAULT_LOCK_TIMEOUT = 6;

    /**
     * The type of a SET DEFAULT_TABLE_TYPE statement.
     */
    public static final int DEFAULT_TABLE_TYPE = 7;

    /**
     * The type of a SET CACHE_SIZE statement.
     */
    public static final int CACHE_SIZE = 8;

    /**
     * The type of a SET TRACE_LEVEL_SYSTEM_OUT statement.
     */
    public static final int TRACE_LEVEL_SYSTEM_OUT = 9;

    /**
     * The type of a SET TRACE_LEVEL_FILE statement.
     */
    public static final int TRACE_LEVEL_FILE = 10;

    /**
     * The type of a SET TRACE_MAX_FILE_SIZE statement.
     */
    public static final int TRACE_MAX_FILE_SIZE = 11;

    /**
     * The type of a SET COLLATION  statement.
     */
    public static final int COLLATION = 12;

    /**
     * The type of a SET CLUSTER statement.
     */
    public static final int CLUSTER = 13;

    /**
     * The type of a SET WRITE_DELAY statement.
     */
    public static final int WRITE_DELAY = 14;

    /**
     * The type of a SET DATABASE_EVENT_LISTENER statement.
     */
    public static final int DATABASE_EVENT_LISTENER = 15;

    /**
     * The type of a SET MAX_MEMORY_ROWS statement.
     */
    public static final int MAX_MEMORY_ROWS = 16;

    /**
     * The type of a SET LOCK_MODE statement.
     */
    public static final int LOCK_MODE = 17;

    /**
     * The type of a SET DB_CLOSE_DELAY statement.
     */
    public static final int DB_CLOSE_DELAY = 18;

    /**
     * The type of a SET LOG statement.
     */
    public static final int LOG = 19;

    /**
     * The type of a SET THROTTLE statement.
     */
    public static final int THROTTLE = 20;

    /**
     * The type of a SET MAX_MEMORY_UNDO statement.
     */
    public static final int MAX_MEMORY_UNDO = 21;

    /**
     * The type of a SET MAX_LENGTH_INPLACE_LOB statement.
     */
    public static final int MAX_LENGTH_INPLACE_LOB = 22;

    /**
     * The type of a SET COMPRESS_LOB statement.
     */
    public static final int COMPRESS_LOB = 23;

    /**
     * The type of a SET ALLOW_LITERALS statement.
     */
    public static final int ALLOW_LITERALS = 24;

    /**
     * The type of a SET MULTI_THREADED statement.
     */
    public static final int MULTI_THREADED = 25;

    /**
     * The type of a SET SCHEMA statement.
     */
    public static final int SCHEMA = 26;

    /**
     * The type of a SET OPTIMIZE_REUSE_RESULTS statement.
     */
    public static final int OPTIMIZE_REUSE_RESULTS = 27;

    /**
     * The type of a SET SCHEMA_SEARCH_PATH statement.
     */
    public static final int SCHEMA_SEARCH_PATH = 28;

    /**
     * The type of a SET UNDO_LOG statement.
     */
    public static final int UNDO_LOG = 29;

    /**
     * The type of a SET REFERENTIAL_INTEGRITY statement.
     */
    public static final int REFERENTIAL_INTEGRITY = 30;

    /**
     * The type of a SET MVCC statement.
     */
    public static final int MVCC = 31;

    /**
     * The type of a SET MAX_OPERATION_MEMORY statement.
     */
    public static final int MAX_OPERATION_MEMORY = 32;

    /**
     * The type of a SET EXCLUSIVE statement.
     */
    public static final int EXCLUSIVE = 33;

    /**
     * The type of a SET CREATE_BUILD statement.
     */
    public static final int CREATE_BUILD = 34;

    /**
     * The type of a SET \@VARIABLE statement.
     */
    public static final int VARIABLE = 35;

    /**
     * The type of a SET QUERY_TIMEOUT statement.
     */
    public static final int QUERY_TIMEOUT = 36;

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

    private SetTypes() {
        // utility class
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
