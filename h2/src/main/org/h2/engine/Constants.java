/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.ResultSet;

/**
 * Constants are fixed values that are used in the whole database code.
 */
public class Constants {

    /**
     * The build date is updated for each public release.
     */
    public static final String BUILD_DATE = "2023-09-17";

    /**
     * Sequential version number. Even numbers are used for official releases,
     * odd numbers are used for development builds.
     */
    public static final int BUILD_ID = 224;

    /**
     * Whether this is a snapshot version.
     */
    public static final boolean BUILD_SNAPSHOT = false;

    /**
     * If H2 is compiled to be included in a product, this should be set to
     * a unique vendor id (to distinguish from official releases).
     * Additionally, a version number should be set to distinguish releases.
     * Example: ACME_SVN1651_BUILD3
     */
    public static final String BUILD_VENDOR_AND_VERSION = null;

    /**
     * The TCP protocol version number 17.
     * @since 1.4.197 (2018-03-18)
     */
    public static final int TCP_PROTOCOL_VERSION_17 = 17;

    /**
     * The TCP protocol version number 18.
     * @since 1.4.198 (2019-02-22)
     */
    public static final int TCP_PROTOCOL_VERSION_18 = 18;

    /**
     * The TCP protocol version number 19.
     * @since 1.4.200 (2019-10-14)
     */
    public static final int TCP_PROTOCOL_VERSION_19 = 19;

    /**
     * The TCP protocol version number 20.
     * @since 2.0.202 (2021-11-25)
     */
    public static final int TCP_PROTOCOL_VERSION_20 = 20;

    /**
     * Minimum supported version of TCP protocol.
     */
    public static final int TCP_PROTOCOL_VERSION_MIN_SUPPORTED = TCP_PROTOCOL_VERSION_17;

    /**
     * Maximum supported version of TCP protocol.
     */
    public static final int TCP_PROTOCOL_VERSION_MAX_SUPPORTED = TCP_PROTOCOL_VERSION_20;

    /**
     * The major version of this database.
     */
    public static final int VERSION_MAJOR = 2;

    /**
     * The minor version of this database.
     */
    public static final int VERSION_MINOR = 2;

    /**
     * The lock mode that means no locking is used at all.
     */
    public static final int LOCK_MODE_OFF = 0;

    /**
     * The lock mode that means read locks are acquired, but they are released
     * immediately after the statement is executed.
     */
    public static final int LOCK_MODE_READ_COMMITTED = 3;

    /**
     * The lock mode that means table level locking is used for reads and
     * writes.
     */
    public static final int LOCK_MODE_TABLE = 1;

    /**
     * The lock mode that means table level locking is used for reads and
     * writes. If a table is locked, System.gc is called to close forgotten
     * connections.
     */
    public static final int LOCK_MODE_TABLE_GC = 2;

    /**
     * Constant meaning both numbers and text is allowed in SQL statements.
     */
    public static final int ALLOW_LITERALS_ALL = 2;

    /**
     * Constant meaning no literals are allowed in SQL statements.
     */
    public static final int ALLOW_LITERALS_NONE = 0;

    /**
     * Constant meaning only numbers are allowed in SQL statements (but no
     * texts).
     */
    public static final int ALLOW_LITERALS_NUMBERS = 1;

    /**
     * SNAPSHOT isolation level of transaction.
     */
    public static final int TRANSACTION_SNAPSHOT = 6;

    /**
     * Whether searching in Blob values should be supported.
     */
    public static final boolean BLOB_SEARCH = false;

    /**
     * The minimum number of entries to keep in the cache.
     */
    public static final int CACHE_MIN_RECORDS = 16;

    /**
     * The default cache type.
     */
    public static final String CACHE_TYPE_DEFAULT = "LRU";

    /**
     * The value of the cluster setting if clustering is disabled.
     */
    public static final String CLUSTERING_DISABLED = "''";

    /**
     * The value of the cluster setting if clustering is enabled (the actual
     * value is checked later).
     */
    public static final String CLUSTERING_ENABLED = "TRUE";

    /**
     * The database URL used when calling a function if only the column list
     * should be returned.
     */
    public static final String CONN_URL_COLUMNLIST = "jdbc:columnlist:connection";

    /**
     * The database URL used when calling a function if the data should be
     * returned.
     */
    public static final String CONN_URL_INTERNAL = "jdbc:default:connection";

    /**
     * The cost is calculated on rowcount + this offset,
     * to avoid using the wrong or no index if the table
     * contains no rows _currently_ (when preparing the statement)
     */
    public static final int COST_ROW_OFFSET = 1000;

    /**
     * The number of milliseconds after which to check for a deadlock if locking
     * is not successful.
     */
    public static final int DEADLOCK_CHECK = 100;

    /**
     * The default port number of the HTTP server (for the H2 Console).
     * This value is also in the documentation and in the Server javadoc.
     */
    public static final int DEFAULT_HTTP_PORT = 8082;

    /**
     * The default value for the LOCK_MODE setting.
     */
    public static final int DEFAULT_LOCK_MODE = LOCK_MODE_READ_COMMITTED;

    /**
     * The default maximum length of an LOB that is stored with the record
     * itself, and not in a separate place.
     */
    public static final int DEFAULT_MAX_LENGTH_INPLACE_LOB = 256;

    /**
     * The default for the setting MAX_OPERATION_MEMORY.
     */
    public static final int DEFAULT_MAX_OPERATION_MEMORY = 100_000;

    /**
     * The default page size to use for new databases.
     */
    public static final int DEFAULT_PAGE_SIZE = 4096;

    /**
     * The default result set concurrency for statements created with
     * Connection.createStatement() or prepareStatement(String sql).
     */
    public static final int DEFAULT_RESULT_SET_CONCURRENCY =
            ResultSet.CONCUR_READ_ONLY;

    /**
     * The default port of the TCP server.
     * This port is also used in the documentation and in the Server javadoc.
     */
    public static final int DEFAULT_TCP_PORT = 9092;

    /**
     * The default delay in milliseconds before the transaction log is written.
     */
    public static final int DEFAULT_WRITE_DELAY = 500;

    /**
     * The password is hashed this many times
     * to slow down dictionary attacks.
     */
    public static final int ENCRYPTION_KEY_HASH_ITERATIONS = 1024;

    /**
     * The block of a file. It is also the encryption block size.
     */
    public static final int FILE_BLOCK_SIZE = 16;

    /**
     * For testing, the lock timeout is smaller than for interactive use cases.
     * This value could be increased to about 5 or 10 seconds.
     */
    public static final int INITIAL_LOCK_TIMEOUT = 2000;

    /**
     * The block size for I/O operations.
     */
    public static final int IO_BUFFER_SIZE = 4 * 1024;

    /**
     * The block size used to compress data in the LZFOutputStream.
     */
    public static final int IO_BUFFER_SIZE_COMPRESS = 128 * 1024;

    /**
     * The number of milliseconds to wait between checking the .lock.db file
     * still exists once a database is locked.
     */
    public static final int LOCK_SLEEP = 1000;

    /**
     * The maximum allowed length of identifiers.
     */
    public static final int MAX_IDENTIFIER_LENGTH = 256;

    /**
     * The maximum number of columns in a table, select statement or row value.
     */
    public static final int MAX_COLUMNS = 16_384;

    /**
     * The maximum allowed length for character string, binary string, and other
     * data types based on them; excluding LOB data types.
     * <p>
     * This needs to be less than (2^31-8)/2 to avoid running into the limit on
     * encoding data fields when storing rows.
     */
    public static final int MAX_STRING_LENGTH = 1000_000_000;

    /**
     * The maximum allowed precision of numeric data types.
     */
    public static final int MAX_NUMERIC_PRECISION = 100_000;

    /**
     * The maximum allowed cardinality of array.
     */
    public static final int MAX_ARRAY_CARDINALITY = 65_536;

    /**
     * The highest possible parameter index.
     */
    public static final int MAX_PARAMETER_INDEX = 100_000;

    /**
     * The memory needed by a regular object with at least one field.
     */
    // Java 6, 64 bit: 24
    // Java 6, 32 bit: 12
    public static final int MEMORY_OBJECT = 24;

    /**
     * The memory needed by an array.
     */
    public static final int MEMORY_ARRAY = 24;

    /**
     * The memory needed by a pointer.
     */
    // Java 6, 64 bit: 8
    // Java 6, 32 bit: 4
    public static final int MEMORY_POINTER = 8;

    /**
     * The memory needed by a Row.
     */
    public static final int MEMORY_ROW = 40;

    /**
     * The name prefix used for indexes that are not explicitly named.
     */
    public static final String PREFIX_INDEX = "INDEX_";

    /**
     * The name prefix used for synthetic nested join tables.
     */
    public static final String PREFIX_JOIN = "SYSTEM_JOIN_";

    /**
     * The name prefix used for primary key constraints that are not explicitly
     * named.
     */
    public static final String PREFIX_PRIMARY_KEY = "PRIMARY_KEY_";

    /**
     * The name prefix used for query aliases that are not explicitly named.
     */
    public static final String PREFIX_QUERY_ALIAS = "QUERY_ALIAS_";

    /**
     * Every user belongs to this role.
     */
    public static final String PUBLIC_ROLE_NAME = "PUBLIC";

    /**
     * The number of bytes in random salt that is used to hash passwords.
     */
    public static final int SALT_LEN = 8;

    /**
     * The identity of INFORMATION_SCHEMA.
     */
    public static final int INFORMATION_SCHEMA_ID = -1;

    /**
     * The identity of PUBLIC schema.
     */
    public static final int MAIN_SCHEMA_ID = 0;

    /**
     * The name of the default schema.
     */
    public static final String SCHEMA_MAIN = "PUBLIC";

    /**
     * The identity of pg_catalog schema.
     */
    public static final int PG_CATALOG_SCHEMA_ID = -1_000;

    /**
     * The name of the pg_catalog schema.
     */
    public static final String SCHEMA_PG_CATALOG = "PG_CATALOG";

    /**
     * The default selectivity (used if the selectivity is not calculated).
     */
    public static final int SELECTIVITY_DEFAULT = 50;

    /**
     * The number of distinct values to keep in memory when running ANALYZE.
     */
    public static final int SELECTIVITY_DISTINCT_COUNT = 10_000;

    /**
     * The default directory name of the server properties file for the H2
     * Console.
     */
    public static final String SERVER_PROPERTIES_DIR = "~";

    /**
     * The name of the server properties file for the H2 Console.
     */
    public static final String SERVER_PROPERTIES_NAME = ".h2.server.properties";

    /**
     * Queries that take longer than this number of milliseconds are written to
     * the trace file with the level info.
     */
    public static final long SLOW_QUERY_LIMIT_MS = 100;

    /**
     * The database URL prefix of this database.
     */
    public static final String START_URL = "jdbc:h2:";

    /**
     * The file name suffix of file lock files that are used to make sure a
     * database is open by only one process at any time.
     */
    public static final String SUFFIX_LOCK_FILE = ".lock.db";

    /**
     * The file name suffix of a H2 version 1.1 database file.
     */
    public static final String SUFFIX_OLD_DATABASE_FILE = ".data.db";

    /**
     * The file name suffix of a MVStore file.
     */
    public static final String SUFFIX_MV_FILE = ".mv.db";

    /**
     * The file name suffix of a new MVStore file, used when compacting a store.
     */
    public static final String SUFFIX_MV_STORE_NEW_FILE = ".newFile";

    /**
     * The file name suffix of a temporary MVStore file, used when compacting a
     * store.
     */
    public static final String SUFFIX_MV_STORE_TEMP_FILE = ".tempFile";

    /**
     * The file name suffix of temporary files.
     */
    public static final String SUFFIX_TEMP_FILE = ".temp.db";

    /**
     * The file name suffix of trace files.
     */
    public static final String SUFFIX_TRACE_FILE = ".trace.db";

    /**
     * How often we check to see if we need to apply a throttling delay if SET
     * THROTTLE has been used.
     */
    public static final int THROTTLE_DELAY = 50;

    /**
     * The database URL format in simplified Backus-Naur form.
     */
    public static final String URL_FORMAT = START_URL +
            "{ {.|mem:}[name] | [file:]fileName | " +
            "{tcp|ssl}:[//]server[:port][,server2[:port]]/name }[;key=value...]";

    /**
     * The package name of user defined classes.
     */
    public static final String USER_PACKAGE = "org.h2.dynamic";

    /**
     * The maximum time in milliseconds to keep the cost of a view.
     * 10000 means 10 seconds.
     */
    public static final int VIEW_COST_CACHE_MAX_AGE = 10_000;

    /**
     * The name of the index cache that is used for temporary view (subqueries
     * used as tables).
     */
    public static final int VIEW_INDEX_CACHE_SIZE = 64;

    /**
     * The maximum number of entries in query statistics.
     */
    public static final int QUERY_STATISTICS_MAX_ENTRIES = 100;

    /**
     * The minimum number of characters in web admin password.
     */
    public static final int MIN_WEB_ADMIN_PASSWORD_LENGTH = 12;

    /**
     * Announced version for PgServer.
     */
    public static final String PG_VERSION = "8.2.23";

    /**
     * The version of this product, consisting of major version, minor
     * version, and build id.
     */
    public static final String VERSION;

    /**
     * The complete version number of this database, consisting of
     * the major version, the minor version, the build id, and the build date.
     */
    public static final String FULL_VERSION;

    static {
        String version = VERSION_MAJOR + "." + VERSION_MINOR + '.' + BUILD_ID;
        if (BUILD_VENDOR_AND_VERSION != null) {
            version += '_' + BUILD_VENDOR_AND_VERSION;
        }
        if (BUILD_SNAPSHOT) {
            version += "-SNAPSHOT";
        }
        VERSION = version;
        FULL_VERSION = version + (" (" + BUILD_DATE + ')');
    }

    private Constants() {
        // utility class
    }

}
