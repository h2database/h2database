/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

/*
 * Coding rules:
 * - boolean CHECK = x > boolean CHECK = Database.CHECK
 * - Database.CHECK = false (or true for debug build)
 * - System.out > trace messages
 *
 * Release checklist
 * - ant jarClient, check jar file size
 * - try compiling with JDK 1.3, 1.4, 1.5 and 1.6
 * - change FAQ (next release planned, known bugs)
 * - check version, change build number in Constants.java and build.xml
 * - check code coverage
 * - No "  Message.getInternalError" (must be "throw Message.getInternalError")
 * - No TODO in the docs
 * - Run regression test with JDK 1.4 and 1.5
 * - Change version(s) in performance.html; use latest versions of other databases
 * - Run 'ant benchmark' (with JDK 1.4 currently)
 * - copy the benchmark results and update the performance page and diagram
 *   (remove rows 2*open/close, 2*executed statement)
 *
 * - documentation: if there are new files, add them to MergeDocs
 * - documentation: check if all javadoc files are in the index
 * - ant docs
 * - PDF (15 min)
 *      - footer
 *      - front page
 *      - tables (optimal size)
 *      - orphan control, page breaks
 *      - table of contents
 * - switch off auto-build
 * - ant all
 * - make sure odbc files are the
 * - make sure the pdf file is there
 * - make sure the build files are removed
 * - ant zip
 * - windows installer (nsis)
 * - test
 * - test the windows service
 * - TestSystemExit
 * - test with hibernate
 * - scan for viruses
 *
 * - Send a mail to Google Groups
 * - newsletter: prepare, send (always send to BCC!!)
 *
 * @author Thomas
 */
public class Constants {

    public static final int BUILD_ID = 34;
    private static final String BUILD = "2006-12-17";
    public static final int VERSION_MAJOR = 1;
    public static final int VERSION_MINOR = 0;

    public static final int FILE_BLOCK_SIZE = 16;
    public static final String MAGIC_FILE_HEADER_TEXT = "-- H2 0.5/T --      ".substring(0, FILE_BLOCK_SIZE-1) + "\n";
    public static final String MAGIC_FILE_HEADER = "-- H2 0.5/B --      ".substring(0, FILE_BLOCK_SIZE-1) + "\n";
    public static final int TCP_DRIVER_VERSION = 3;

    public static final int VERSION_JDBC_MAJOR = 3;
    public static final int VERSION_JDBC_MINOR = 0;

    public static String getVersion() {
        return VERSION_MAJOR + "." + VERSION_MINOR+ " (" + BUILD + ")";
    }

    public static final int NULL_SORT_LOW = 1, NULL_SORT_HIGH = 2;
    public static final int NULL_SORT_START = 3, NULL_SORT_END = 4;
    public static final int NULL_SORT_DEFAULT = NULL_SORT_LOW;

    public static final int DEFAULT_SERVER_PORT = 9092; // this is also in the docs

    public static final String START_URL = "jdbc:h2:";

    public static final String URL_FORMAT = START_URL + "{ {.|mem:}[name] | [file:]fileName | {tcp|ssl}:[//]server[:port][,server2[:port]]/name }[;key=value...]";

    public static final String PRODUCT_NAME = "H2";

    public static final String DRIVER_NAME = "H2 JDBC Driver";

    public static final int IO_BUFFER_SIZE = 4 * 1024;
    public static final int IO_BUFFER_SIZE_COMPRESS = 128 * 1024;

    public static final int DEFAULT_CACHE_SIZE = 1 << 16;
    public static final int CACHE_SIZE_INDEX_SHIFT = 3;
    public static int CACHE_MIN_RECORDS = 16;
    public static final int DEFAULT_CACHE_SIZE_INDEX = DEFAULT_CACHE_SIZE >> CACHE_SIZE_INDEX_SHIFT;

    public static final int DEFAULT_CACHE_SIZE_LINEAR_INDEX = 1 << 8;

    public static final String SUFFIX_DATA_FILE = ".data.db";
    public static final String SUFFIX_LOG_FILE = ".log.db";
    public static final String SUFFIX_INDEX_FILE = ".index.db";
    public static final String SUFFIX_HASH_FILE = ".hash.db";
    public static final String SUFFIX_LOCK_FILE = ".lock.db";
    public static final String SUFFIX_TEMP_FILE = ".temp.db";
    public static final String SUFFIX_TRACE_FILE = ".trace.db";
    public static final String SUFFIX_LOB_FILE = ".lob.db";
    public static final String SUFFIX_TRACE_START_FILE = ".start";
    public static final String SUFFIX_SUMMARY_FILE = ".sum.db";
    public static final String SUFFIX_LOBS_DIRECTORY = ".lobs.db";

    public static final String UTF8 = "UTF8";
    public static final int DEFAULT_TABLE_TYPE = 0;
    public static final int DEFAULT_MAX_LENGTH_INPLACE_LOB = 128;
    public static final int DEFAULT_MAX_LENGTH_CLIENTSIDE_LOB = 65536;
    public static final int SALT_LEN = 8;

    public static final int DEFAULT_DATA_PAGE_SIZE = 512;

    public static final boolean USE_OBJECT_CACHE = true;
    public static final int OBJECT_CACHE_SIZE = 1024;
    public static final int OBJECT_CACHE_MAX_PER_ELEMENT_SIZE = 4096;

    public static final String PRIMARY_KEY_PREFIX = "PRIMARY_KEY_";

    public static final int LOCK_SLEEP = 1000;

    // TODO for testing, the lock timeout is smaller than for interactive use cases
    // public static final int INITIAL_LOCK_TIMEOUT = 60 * 1000;
    public static final int INITIAL_LOCK_TIMEOUT = 1000;

    public static final char DEFAULT_ESCAPE_CHAR = '\\';
    public static final int DEFAULT_HTTP_PORT = 8082; // also in the docs
    public static final boolean DEFAULT_HTTP_SSL = false;
    public static final boolean DEFAULT_HTTP_ALLOW_OTHERS = false;
    public static final int DEFAULT_FTP_PORT = 8021;

    public static boolean MULTI_THREADED_KERNEL;

    public static final int DEFAULT_MAX_MEMORY_ROWS = 10000;

    public static final int DEFAULT_MAX_MEMORY_UNDO = Integer.MAX_VALUE;

    public static final int DEFAULT_WRITE_DELAY = 500;

    public static final String SERVER_PROPERTIES_TITLE = "H2 Server Properties";

    public static final String SERVER_PROPERTIES_FILE = ".h2.server.properties";

    public static final long LONG_QUERY_LIMIT_MS = 100;

    public static final String PUBLIC_ROLE_NAME = "PUBLIC";

    public static final String TEMP_TABLE_PREFIX = "TEMP_TABLE_";
    public static final String TEMP_TABLE_TRANSACTION_PREFIX = "TEMP_TRANS_TABLE_";

    public static final int BIGDECIMAL_SCALE_MAX = 100000;

    public static final String SCHEMA_MAIN = "PUBLIC";
    public static final String SCHEMA_INFORMATION = "INFORMATION_SCHEMA";

    public static final String DBA_NAME = "DBA";

    public static final String CHARACTER_SET_NAME = "Unicode";

    public static final String CLUSTERING_DISABLED = "''";

    public static final int EMERGENCY_SPACE_INITIAL = 1 * 1024 * 1024;
    public static final int EMERGENCY_SPACE_MIN = 128 * 1024;

    public static final int LOCK_MODE_OFF = 0;
    public static final int LOCK_MODE_TABLE = 1;
    public static final int LOCK_MODE_TABLE_GC = 2;
    public static final int LOCK_MODE_READ_COMMITTED = 3;

    public static final int SELECTIVITY_DISTINCT_COUNT = 10000;
    public static final int SELECTIVITY_DEFAULT = 50;
    public static final int SELECTIVITY_ANALYZE_SAMPLE_ROWS = 10000;

    public static final int SERVER_CACHED_OBJECTS = 64;
    public static final int SERVER_SMALL_RESULTSET_SIZE = 100;

    public static final boolean LOG_ALL_ERRORS = false;

    // the cost is calculated on rowcount + this offset, to avoid using the wrong or no index
    // if the table contains no rows _currently_ (when preparing the statement)
    public static final int COST_ROW_OFFSET = 1000;
    public static final long FLUSH_INDEX_DELAY = 0;
    public static final int THROTTLE_DELAY = 50;
    public static boolean RUN_FINALIZERS = true;
    // TODO performance: change this values and recompile for higher performance
    public static boolean CHECK = true;
    public static boolean CHECK2;

    public static final String MANAGEMENT_DB_PREFIX = "management_db_";
    public static final String MANAGEMENT_DB_USER = "sa";
    public static int REDO_BUFFER_SIZE = 256 * 1024;

    public static final boolean SERIALIZE_JAVA_OBJECTS = true;
    public static boolean RECOMPILE_ALWAYS;

    public static boolean OPTIMIZE_SUBQUERY_CACHE = true;
    public static boolean OVERFLOW_EXCEPTIONS = true;

    public static boolean LOB_FILES_IN_DIRECTORIES;
    // TODO: also remove DataHandler.allocateObjectId, createTempFile when setting this to true and removing it
    public static int LOB_FILES_PER_DIRECTORY = 256;

    public static boolean OPTIMIZE_MIN_MAX = true;
    public static boolean OPTIMIZE_IN = true;

    // TODO there is a bug currently, need to refactor & debug the code to fix this (and add more tests!)
    public static boolean OPTIMIZE_EVALUATABLE_SUBQUERIES;
    public static final long DEFAULT_MAX_LOG_SIZE = 32 * 1024 * 1024;
    public static final long LOG_SIZE_DIVIDER = 10;

    public static final int ALLOW_LITERALS_NONE = 0;
    public static final int ALLOW_LITERALS_NUMBERS = 1;
    public static final int ALLOW_LITERALS_ALL = 2;

    public static final int DEFAULT_ALLOW_LITERALS = ALLOW_LITERALS_ALL;
    public static boolean AUTO_CONVERT_LOB_TO_FILES = true;
    public static int MIN_WRITE_DELAY = 5;

    public static final boolean ALLOW_EMTPY_BTREE_PAGES = true;
    public static final String CONN_URL_INTERNAL = "jdbc:default:connection";
    public static final String CONN_URL_COLUMNLIST = "jdbc:columnlist:connection";
    
    public static final int VIEW_COST_CACHE_SIZE = 64;
    public static final int VIEW_COST_CACHE_MAX_AGE = 10000; // 10 seconds
    public static final int MAX_PARAMETER_INDEX = 100000;
    
    // to slow down dictionary attacks
    public static final int ENCRYPTION_KEY_HASH_ITERATIONS = 1024;
    public static final String SCRIPT_SQL = "script.sql";

}
