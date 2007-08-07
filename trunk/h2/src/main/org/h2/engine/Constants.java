/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import org.h2.message.TraceSystem;

/*
 * Coding rules:
 * - boolean CHECK = x > boolean CHECK = Database.CHECK
 * - Database.CHECK = false (or true for debug build)
 * - System.out > trace messages
 *
 * Release checklist
 * - Run FindBugs
 * - Update latest version in build.html: http://mirrors.ibiblio.org/pub/mirrors/maven2/com/h2database/h2/
 * - ant jarClient, check jar file size
 * 
 * - Compile with JDK 1.4, 1.5 and 1.6:
 *   set path=C:\Programme\Java\jdk1.6.0\bin;%PATH%
 *   set JAVA_HOME=C:\Programme\Java\jdk1.6.0
 *   ant codeswitchJdk16
 *   ant compile
 *   ant codeswitchJdk14
 *   
 * - Change FAQ (next release planned, known bugs)
 * - Check version, change build number in Constants.java and ant-build.properties
 * - Maybe increase TCP_DRIVER_VERSION
 * - Check code coverage
 * - No "  Message.getInternalError" (must be "throw Message.getInternalError")
 * - No TODO in the docs
 * - Run regression test with JDK 1.4 and 1.5
 * - Change version(s) in performance.html; use latest versions of other databases
 * - Run 'ant benchmark' (with JDK 1.4 currently)
 * - Copy the benchmark results and update the performance page and diagram
 *
 * - Documentation: if there are new files, add them to MergeDocs
 * - Documentation: check if all javadoc files are in the index
 * - run PrepareTranslation (or add to ant docs)
 * - ant docs
 * - PDF (15 min)
 *      - footer
 *      - front page
 *      - orphan control
 *      - tables (optimal size), page breaks
 *      - table of contents
 * - Switch off auto-build
 * - ant all
 * - Copy the pdf file to h2/docs
 * - Make sure the build files are removed
 * - ant zip
 * - Windows installer (nsis)
 * - Test
 * - Test the windows service
 * - TestSystemExit
 * - Test with hibernate
 * - Scan for viruses
 * - Newsletter: send to h2database-news@googlegroups.com (http://groups.google.com/group/h2database-news)
 * - newsletter: prepare, send (always send to BCC!!)
 * - http://maven.apache.org/guides/mini/guide-ibiblio-upload.html
 * - Add to freshmeat, http://code.google.com/p/h2database/downloads/list
 * 
 * @author Thomas
 */
public class Constants {

    public static final int BUILD_ID = 56;
    private static final String BUILD = "2007-08-02";
    public static final int VERSION_MAJOR = 1;
    public static final int VERSION_MINOR = 0;

    public static final int FILE_BLOCK_SIZE = 16;
    public static final String MAGIC_FILE_HEADER_TEXT = "-- H2 0.5/T --      ".substring(0, FILE_BLOCK_SIZE-1) + "\n";
    public static final String MAGIC_FILE_HEADER = "-- H2 0.5/B --      ".substring(0, FILE_BLOCK_SIZE-1) + "\n";
    public static final int TCP_DRIVER_VERSION = 4;
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
    public static final int DEFAULT_CACHE_SIZE_LINEAR_INDEX = 64 * 1024;
    public static final String SUFFIX_DB_FILE = ".db";
    public static final String SUFFIX_DATA_FILE = ".data.db";
    public static final String SUFFIX_LOG_FILE = ".log.db";
    public static final String SUFFIX_INDEX_FILE = ".index.db";
    public static final String SUFFIX_HASH_FILE = ".hash.db";
    public static final String SUFFIX_LOCK_FILE = ".lock.db";
    public static final String SUFFIX_TEMP_FILE = ".temp.db";
    public static final String SUFFIX_TRACE_FILE = ".trace.db";
    public static final String SUFFIX_LOB_FILE = ".lob.db";
    public static final String SUFFIX_TRACE_START_FILE = ".start";
    public static final String SUFFIX_LOBS_DIRECTORY = ".lobs.db";
    public static final String UTF8 = "UTF8";
    public static final int DEFAULT_TABLE_TYPE = 0;
    public static final int DEFAULT_MAX_LENGTH_INPLACE_LOB = 1024;
    public static final int DEFAULT_MAX_LENGTH_CLIENTSIDE_LOB = 65536;
    public static final int SALT_LEN = 8;
    public static final int DEFAULT_DATA_PAGE_SIZE = 512;
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
    public static final int DEFAULT_MAX_MEMORY_ROWS = 10000;
    public static final int DEFAULT_WRITE_DELAY = 500;
    public static final String SERVER_PROPERTIES_TITLE = "H2 Server Properties";
    public static final String SERVER_PROPERTIES_FILE = ".h2.server.properties";
    public static final long LONG_QUERY_LIMIT_MS = 100;
    public static final String PUBLIC_ROLE_NAME = "PUBLIC";
    public static final String TEMP_TABLE_PREFIX = "TEMP_TABLE_";
    public static final int BIG_DECIMAL_SCALE_MAX = 100000;
    public static final String SCHEMA_MAIN = "PUBLIC";
    public static final String SCHEMA_INFORMATION = "INFORMATION_SCHEMA";
    public static final String DBA_NAME = "DBA";
    public static final String CHARACTER_SET_NAME = "Unicode";
    public static final String CLUSTERING_DISABLED = "''";
    public static final int LOCK_MODE_OFF = 0;
    public static final int LOCK_MODE_TABLE = 1;
    public static final int LOCK_MODE_TABLE_GC = 2;
    public static final int LOCK_MODE_READ_COMMITTED = 3;
    public static final int SELECTIVITY_DISTINCT_COUNT = 10000;
    public static final int SELECTIVITY_DEFAULT = 50;
    public static final int SELECTIVITY_ANALYZE_SAMPLE_ROWS = 10000;

    // the cost is calculated on rowcount + this offset, to avoid using the wrong or no index
    // if the table contains no rows _currently_ (when preparing the statement)
    public static final int COST_ROW_OFFSET = 1000;
    public static final long FLUSH_INDEX_DELAY = 0;
    public static final int THROTTLE_DELAY = 50;
    public static final String MANAGEMENT_DB_PREFIX = "management_db_";
    public static final String MANAGEMENT_DB_USER = "sa";
    public static final boolean SERIALIZE_JAVA_OBJECTS = true;
    public static final long DEFAULT_MAX_LOG_SIZE = 32 * 1024 * 1024;
    public static final long LOG_SIZE_DIVIDER = 10;
    public static final int ALLOW_LITERALS_NONE = 0;
    public static final int ALLOW_LITERALS_NUMBERS = 1;
    public static final int ALLOW_LITERALS_ALL = 2;
    public static final int DEFAULT_ALLOW_LITERALS = ALLOW_LITERALS_ALL;
    public static final boolean AUTO_CONVERT_LOB_TO_FILES = true;
    public static final boolean ALLOW_EMPTY_BTREE_PAGES = true;
    public static final String CONN_URL_INTERNAL = "jdbc:default:connection";
    public static final String CONN_URL_COLUMNLIST = "jdbc:columnlist:connection";
    public static final int VIEW_INDEX_CACHE_SIZE = 64;
    public static final int VIEW_COST_CACHE_MAX_AGE = 10000; // 10 seconds
    public static final int MAX_PARAMETER_INDEX = 100000;
    
    public static boolean runFinalize = getBooleanSetting("h2.runFinalize", true);
    public static String scriptDirectory = getStringSetting("h2.scriptDirectory", "");
    private static String baseDir = getStringSetting("h2.baseDir", null);
    public static boolean multiThreadedKernel = getBooleanSetting("h2.multiThreadedKernel", false);
    public static boolean lobCloseBetweenReads = getBooleanSetting("h2.lobCloseBetweenReads", false);

    // TODO: also remove DataHandler.allocateObjectId, createTempFile when setting this to true and removing it
    public static final boolean LOB_FILES_IN_DIRECTORIES = getBooleanSetting("h2.lobFilesInDirectories", false);
    public static final int LOB_FILES_PER_DIRECTORY = getIntSetting("h2.lobFilesPerDirectory", 256);

    // TODO need to refactor & test the code to enable this (add more tests!)
    public static final boolean OPTIMIZE_EVALUATABLE_SUBQUERIES = false;

    // to slow down dictionary attacks
    public static final int ENCRYPTION_KEY_HASH_ITERATIONS = 1024;
    public static final String SCRIPT_SQL = "script.sql";
    public static final int CACHE_MIN_RECORDS = 16;
    public static final int MIN_WRITE_DELAY = getIntSetting("h2.minWriteDelay", 5);
    public static final boolean CHECK2 = getBooleanSetting("h2.check2", false);
    
    public static final boolean CHECK = getBooleanSetting("h2.check", true);
    public static final boolean OPTIMIZE_MIN_MAX = getBooleanSetting("h2.optimizeMinMax", true);
    public static final boolean OPTIMIZE_IN = getBooleanSetting("h2.optimizeIn", true);
    public static final int REDO_BUFFER_SIZE = getIntSetting("h2.redoBufferSize", 256 * 1024);
    public static final boolean RECOMPILE_ALWAYS = getBooleanSetting("h2.recompileAlways", false);
    public static final boolean OPTIMIZE_SUBQUERY_CACHE = getBooleanSetting("h2.optimizeSubqueryCache", true);
    public static final boolean OVERFLOW_EXCEPTIONS = getBooleanSetting("h2.overflowExceptions", true);
    public static final boolean LOG_ALL_ERRORS = getBooleanSetting("h2.logAllErrors", false);
    public static final String LOG_ALL_ERRORS_FILE = getStringSetting("h2.logAllErrorsFile", "h2errors.txt");
    public static final int SERVER_CACHED_OBJECTS = getIntSetting("h2.serverCachedObjects", 64);
    public static final int SERVER_SMALL_RESULT_SET_SIZE = getIntSetting("h2.serverSmallResultSetSize", 100);
    public static final int EMERGENCY_SPACE_INITIAL = getIntSetting("h2.emergencySpaceInitial", 1 * 1024 * 1024);
    public static final int EMERGENCY_SPACE_MIN = getIntSetting("h2.emergencySpaceMin", 128 * 1024);
    public static final boolean OBJECT_CACHE = getBooleanSetting("h2.objectCache", true);
    public static final int OBJECT_CACHE_SIZE = getIntSetting("h2.objectCacheSize", 1024);
    public static final int OBJECT_CACHE_MAX_PER_ELEMENT_SIZE = getIntSetting("h2.objectCacheMaxPerElementSize", 4096);
    public static final String CLIENT_TRACE_DIRECTORY = getStringSetting("h2.clientTraceDirectory", "trace.db/");
    public static final int MAX_FILE_RETRY = Math.max(1, getIntSetting("h2.maxFileRetry", 16));
    public static final boolean ALLOW_BIG_DECIMAL_EXTENSIONS = getBooleanSetting("h2.allowBigDecimalExtensions", false);
    public static final boolean INDEX_LOOKUP_NEW = getBooleanSetting("h2.indexLookupNew", true);
    public static final boolean TRACE_IO = getBooleanSetting("h2.traceIO", false);
    public static final int DATASOURCE_TRACE_LEVEL = getIntSetting("h2.dataSourceTraceLevel", TraceSystem.ERROR);
    public static final int CACHE_SIZE_DEFAULT = getIntSetting("h2.cacheSizeDefault", 16 * 1024);
    public static final int CACHE_SIZE_INDEX_SHIFT = getIntSetting("h2.cacheSizeIndexShift", 3);
    public static final int CACHE_SIZE_INDEX_DEFAULT = CACHE_SIZE_DEFAULT >> CACHE_SIZE_INDEX_SHIFT;
    public static final int DEFAULT_MAX_MEMORY_UNDO = getIntSetting("h2.defaultMaxMemoryUndo", 50000);
    public static final boolean OPTIMIZE_NOT = getBooleanSetting("h2.optimizeNot", true);
    public static final boolean OPTIMIZE_2_EQUALS = getBooleanSetting("h2.optimizeTwoEquals", true);
    public static final int DEFAULT_LOCK_MODE = getIntSetting("h2.defaultLockMode", LOCK_MODE_READ_COMMITTED);
    
    public static void setBaseDir(String dir) {
        if(!dir.endsWith("/")) {
            dir += "/";
        }
        baseDir = dir;
    }
    
    public static String getBaseDir() {
        return baseDir;
    }
    
    public static boolean getBooleanSetting(String name, boolean defaultValue) {
        String s = System.getProperty(name);
        if(s != null) {
            try {
                return Boolean.valueOf(s).booleanValue();
            } catch(NumberFormatException e) {
            }
        }
        return defaultValue;
    }

    public static String getStringSetting(String name, String defaultValue) {
        String s = System.getProperty(name);
        return s == null ? defaultValue : s;
    }

    public static int getIntSetting(String name, int defaultValue) {
        String s = System.getProperty(name);
        if(s != null) {
            try {
                return Integer.decode(s).intValue();
            } catch(NumberFormatException e) {
            }
        }
        return defaultValue;
    }
}
