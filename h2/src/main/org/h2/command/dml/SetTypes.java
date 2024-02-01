/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.util.List;

/**
 * The list of setting for a SET statement.
 */
public class SetTypes {

    /**
     * The type of a SET IGNORECASE statement.
     */
    public static final int IGNORECASE = 0;

    /**
     * The type of a SET MAX_LOG_SIZE statement.
     */
    public static final int MAX_LOG_SIZE = IGNORECASE + 1;

    /**
     * The type of a SET MODE statement.
     */
    public static final int MODE = MAX_LOG_SIZE + 1;

    /**
     * The type of a SET READONLY statement.
     */
    public static final int READONLY = MODE + 1;

    /**
     * The type of a SET LOCK_TIMEOUT statement.
     */
    public static final int LOCK_TIMEOUT = READONLY + 1;

    /**
     * The type of a SET DEFAULT_LOCK_TIMEOUT statement.
     */
    public static final int DEFAULT_LOCK_TIMEOUT = LOCK_TIMEOUT + 1;

    /**
     * The type of a SET DEFAULT_TABLE_TYPE statement.
     */
    public static final int DEFAULT_TABLE_TYPE = DEFAULT_LOCK_TIMEOUT + 1;

    /**
     * The type of a SET CACHE_SIZE statement.
     */
    public static final int CACHE_SIZE = DEFAULT_TABLE_TYPE + 1;

    /**
     * The type of a SET TRACE_LEVEL_SYSTEM_OUT statement.
     */
    public static final int TRACE_LEVEL_SYSTEM_OUT = CACHE_SIZE + 1;

    /**
     * The type of a SET TRACE_LEVEL_FILE statement.
     */
    public static final int TRACE_LEVEL_FILE = TRACE_LEVEL_SYSTEM_OUT + 1;

    /**
     * The type of a SET TRACE_MAX_FILE_SIZE statement.
     */
    public static final int TRACE_MAX_FILE_SIZE = TRACE_LEVEL_FILE + 1;

    /**
     * The type of a SET COLLATION  statement.
     */
    public static final int COLLATION = TRACE_MAX_FILE_SIZE + 1;

    /**
     * The type of a SET CLUSTER statement.
     */
    public static final int CLUSTER = COLLATION + 1;

    /**
     * The type of a SET WRITE_DELAY statement.
     */
    public static final int WRITE_DELAY = CLUSTER + 1;

    /**
     * The type of a SET DATABASE_EVENT_LISTENER statement.
     */
    public static final int DATABASE_EVENT_LISTENER = WRITE_DELAY + 1;

    /**
     * The type of a SET MAX_MEMORY_ROWS statement.
     */
    public static final int MAX_MEMORY_ROWS = DATABASE_EVENT_LISTENER + 1;

    /**
     * The type of a SET LOCK_MODE statement.
     */
    public static final int LOCK_MODE = MAX_MEMORY_ROWS + 1;

    /**
     * The type of a SET DB_CLOSE_DELAY statement.
     */
    public static final int DB_CLOSE_DELAY = LOCK_MODE + 1;

    /**
     * The type of a SET THROTTLE statement.
     */
    public static final int THROTTLE = DB_CLOSE_DELAY + 1;

    /**
     * The type of a SET MAX_MEMORY_UNDO statement.
     */
    public static final int MAX_MEMORY_UNDO = THROTTLE + 1;

    /**
     * The type of a SET MAX_LENGTH_INPLACE_LOB statement.
     */
    public static final int MAX_LENGTH_INPLACE_LOB = MAX_MEMORY_UNDO + 1;

    /**
     * The type of a SET ALLOW_LITERALS statement.
     */
    public static final int ALLOW_LITERALS = MAX_LENGTH_INPLACE_LOB + 1;

    /**
     * The type of a SET SCHEMA statement.
     */
    public static final int SCHEMA = ALLOW_LITERALS + 1;

    /**
     * The type of a SET OPTIMIZE_REUSE_RESULTS statement.
     */
    public static final int OPTIMIZE_REUSE_RESULTS = SCHEMA + 1;

    /**
     * The type of a SET SCHEMA_SEARCH_PATH statement.
     */
    public static final int SCHEMA_SEARCH_PATH = OPTIMIZE_REUSE_RESULTS + 1;

    /**
     * The type of a SET REFERENTIAL_INTEGRITY statement.
     */
    public static final int REFERENTIAL_INTEGRITY = SCHEMA_SEARCH_PATH + 1;

    /**
     * The type of a SET MAX_OPERATION_MEMORY statement.
     */
    public static final int MAX_OPERATION_MEMORY = REFERENTIAL_INTEGRITY + 1;

    /**
     * The type of a SET EXCLUSIVE statement.
     */
    public static final int EXCLUSIVE = MAX_OPERATION_MEMORY + 1;

    /**
     * The type of a SET CREATE_BUILD statement.
     */
    public static final int CREATE_BUILD = EXCLUSIVE + 1;

    /**
     * The type of a SET \@VARIABLE statement.
     */
    public static final int VARIABLE = CREATE_BUILD + 1;

    /**
     * The type of a SET QUERY_TIMEOUT statement.
     */
    public static final int QUERY_TIMEOUT = VARIABLE + 1;

    /**
     * The type of a SET REDO_LOG_BINARY statement.
     */
    public static final int REDO_LOG_BINARY = QUERY_TIMEOUT + 1;

    /**
     * The type of a SET JAVA_OBJECT_SERIALIZER statement.
     */
    public static final int JAVA_OBJECT_SERIALIZER = REDO_LOG_BINARY + 1;

    /**
     * The type of a SET RETENTION_TIME statement.
     */
    public static final int RETENTION_TIME = JAVA_OBJECT_SERIALIZER + 1;

    /**
     * The type of a SET QUERY_STATISTICS statement.
     */
    public static final int QUERY_STATISTICS = RETENTION_TIME + 1;

    /**
     * The type of a SET QUERY_STATISTICS_MAX_ENTRIES statement.
     */
    public static final int QUERY_STATISTICS_MAX_ENTRIES = QUERY_STATISTICS + 1;

    /**
     * The type of SET LAZY_QUERY_EXECUTION statement.
     */
    public static final int LAZY_QUERY_EXECUTION = QUERY_STATISTICS_MAX_ENTRIES + 1;

    /**
     * The type of SET BUILTIN_ALIAS_OVERRIDE statement.
     */
    public static final int BUILTIN_ALIAS_OVERRIDE = LAZY_QUERY_EXECUTION + 1;

    /**
     * The type of a SET AUTHENTICATOR statement.
     */
    public static final int AUTHENTICATOR = BUILTIN_ALIAS_OVERRIDE + 1;

    /**
     * The type of a SET IGNORE_CATALOGS statement.
     */
    public static final int IGNORE_CATALOGS = AUTHENTICATOR + 1;

    /**
     * The type of a SET CATALOG statement.
     */
    public static final int CATALOG = IGNORE_CATALOGS + 1;

    /**
     * The type of a SET NON_KEYWORDS statement.
     */
    public static final int NON_KEYWORDS = CATALOG + 1;

    /**
     * The type of a SET TIME ZONE statement.
     */
    public static final int TIME_ZONE = NON_KEYWORDS + 1;

    /**
     * The type of a SET VARIABLE_BINARY statement.
     */
    public static final int VARIABLE_BINARY = TIME_ZONE + 1;

    /**
     * The type of a SET DEFAULT_NULL_ORDERING statement.
     */
    public static final int DEFAULT_NULL_ORDERING = VARIABLE_BINARY + 1;

    /**
     * The type of a SET TRUNCATE_LARGE_LENGTH statement.
     */
    public static final int TRUNCATE_LARGE_LENGTH = DEFAULT_NULL_ORDERING + 1;

    private static final int COUNT = TRUNCATE_LARGE_LENGTH + 1;

    private static final List<String> TYPES;

    private SetTypes() {
        // utility class
    }

    static {
        TYPES = List.of( //
                "IGNORECASE", //
                "MAX_LOG_SIZE", //
                "MODE", //
                "READONLY", //
                "LOCK_TIMEOUT", //
                "DEFAULT_LOCK_TIMEOUT", //
                "DEFAULT_TABLE_TYPE", //
                "CACHE_SIZE", //
                "TRACE_LEVEL_SYSTEM_OUT", //
                "TRACE_LEVEL_FILE", //
                "TRACE_MAX_FILE_SIZE", //
                "COLLATION", //
                "CLUSTER", //
                "WRITE_DELAY", //
                "DATABASE_EVENT_LISTENER", //
                "MAX_MEMORY_ROWS", //
                "LOCK_MODE", //
                "DB_CLOSE_DELAY", //
                "THROTTLE", //
                "MAX_MEMORY_UNDO", //
                "MAX_LENGTH_INPLACE_LOB", //
                "ALLOW_LITERALS", //
                "SCHEMA", //
                "OPTIMIZE_REUSE_RESULTS", //
                "SCHEMA_SEARCH_PATH", //
                "REFERENTIAL_INTEGRITY", //
                "MAX_OPERATION_MEMORY", //
                "EXCLUSIVE", //
                "CREATE_BUILD", //
                "@", //
                "QUERY_TIMEOUT", //
                "REDO_LOG_BINARY", //
                "JAVA_OBJECT_SERIALIZER", //
                "RETENTION_TIME", //
                "QUERY_STATISTICS", //
                "QUERY_STATISTICS_MAX_ENTRIES", //
                "LAZY_QUERY_EXECUTION", //
                "BUILTIN_ALIAS_OVERRIDE", //
                "AUTHENTICATOR", //
                "IGNORE_CATALOGS", //
                "CATALOG", //
                "NON_KEYWORDS", //
                "TIME ZONE", //
                "VARIABLE_BINARY", //
                "DEFAULT_NULL_ORDERING", //
                "TRUNCATE_LARGE_LENGTH");
        assert TYPES.size() == COUNT;
    }

    /**
     * Get the set type number.
     *
     * @param name the set type name
     * @return the number
     */
    public static int getType(String name) {
        return TYPES.indexOf(name);
    }

    public static List<String> getTypes() {
        return TYPES;
    }

    /**
     * Get the set type name.
     *
     * @param type the type number
     * @return the name
     */
    public static String getTypeName(int type) {
        return TYPES.get(type);
    }

}
