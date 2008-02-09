/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.constant;

import org.h2.engine.Constants;
import org.h2.message.TraceSystem;

/**
 * The constants defined in this class are initialized from system properties.
 * Those properties can be set when starting the virtual machine:
 * <pre>
 * java -Dh2.baseDir=/temp
 * </pre>
 * They can be set within the application, but this must be done before loading any classes of this database
 * (before loading the JDBC driver):
 * <pre>
 * System.setProperty("h2.baseDir", "/temp");
 * </pre>
 */
public class SysProperties {

    /**
     * INTERNAL
     */
    public static final String H2_MAX_QUERY_TIMEOUT = "h2.maxQueryTimeout";

    /**
     * INTERNAL
     */
    public static final String H2_LOG_DELETE_DELAY = "h2.logDeleteDelay";

    /**
     * System property <code>file.encoding</code> (default: Cp1252).<br />
     * It is usually set by the system and is the default encoding used for the RunScript and CSV tool.
     */
    public static final String FILE_ENCODING = getStringSetting("file.encoding", "Cp1252");

    /**
     * System property <code>file.separator</code> (default: /).<br />
     * It is usually set by the system, and used to build absolute file names.
     */
    public static final String FILE_SEPARATOR = getStringSetting("file.separator", "/");

    /**
     * System property <code>line.separator</code> (default: \n).<br />
     * It is usually set by the system, and used by the script and trace tools.
     */
    public static final String LINE_SEPARATOR = getStringSetting("line.separator", "\n");

    /**
     * System property <code>user.home</code> (default: empty string).<br />
     * It is usually set by the system, and used as a replacement for ~ in file names.
     */
    public static final String USER_HOME = getStringSetting("user.home", "");

    /**
     * System property <code>h2.allowBigDecimalExtensions</code> (default: false).<br />
     * When enabled, classes that extend BigDecimal are supported in PreparedStatement.setBigDecimal.
     */
    public static final boolean ALLOW_BIG_DECIMAL_EXTENSIONS = getBooleanSetting("h2.allowBigDecimalExtensions", false);

    /**
     * System property <code>h2.allowedClasses</code> (default: *).<br />
     * Comma separated list of class names or prefixes.
     */
    public static final String ALLOWED_CLASSES = getStringSetting("h2.allowedClasses", "*");

    /**
     * System property <code>h2.bindAddress</code> (default: *).<br />
     * Comma separated list of class names or prefixes.
     */
    public static final String BIND_ADDRESS = getStringSetting("h2.bindAddress", null);

    /**
     * System property <code>h2.cacheSizeDefault</code> (default: 16384).<br />
     * The default cache size in KB.
     */
    public static final int CACHE_SIZE_DEFAULT = getIntSetting("h2.cacheSizeDefault", 16 * 1024);

    /**
     * System property <code>h2.cacheSizeIndexShift</code> (default: 3).<br />
     * How many time the cache size value is divided by two to get the index cache size.
     * The index cache size is calculated like this: cacheSize >> cacheSizeIndexShift.
     */
    public static final int CACHE_SIZE_INDEX_SHIFT = getIntSetting("h2.cacheSizeIndexShift", 3);

    /**
     * INTERNAL
     */
    public static final int CACHE_SIZE_INDEX_DEFAULT = CACHE_SIZE_DEFAULT >> CACHE_SIZE_INDEX_SHIFT;

    /**
     * System property <code>h2.check</code> (default: true).<br />
     * Assertions in the database engine.
     */
    public static final boolean CHECK = getBooleanSetting("h2.check", true);

    /**
     * System property <code>h2.check2</code> (default: true).<br />
     * Additional assertions in the database engine.
     */
    public static final boolean CHECK2 = getBooleanSetting("h2.check2", false);

    /**
     * System property <code>h2.clientTraceDirectory</code> (default: trace.db/).<br />
     * Directory where the trace files of the JDBC client are stored (only for client / server).
     */
    public static final String CLIENT_TRACE_DIRECTORY = getStringSetting("h2.clientTraceDirectory", "trace.db/");

    /**
     * System property <code>h2.defaultMaxOperationMemory</code> (default: 100000).<br />
     * The default for the setting MAX_OPERATION_MEMORY.
     */
    public static final int DEFAULT_MAX_OPERATION_MEMORY = getIntSetting("h2.defaultMaxOperationMemory", 100000);

    /**
     * System property <code>h2.dataSourceTraceLevel</code> (default: 1).<br />
     * The trace level of the data source implementation. Default is 1 for error.
     */
    public static final int DATASOURCE_TRACE_LEVEL = getIntSetting("h2.dataSourceTraceLevel", TraceSystem.ERROR);

    /**
     * System property <code>h2.defaultMaxMemoryUndo</code> (default: 100000).<br />
     * The default value for the MAX_MEMORY_UNDO setting.
     */
    public static final int DEFAULT_MAX_MEMORY_UNDO = getIntSetting("h2.defaultMaxMemoryUndo", 100000);

    /**
     * System property <code>h2.defaultLockMode</code> (default: 3).<br />
     * The default value for the LOCK_MODE setting.
     */
    public static final int DEFAULT_LOCK_MODE = getIntSetting("h2.defaultLockMode", Constants.LOCK_MODE_READ_COMMITTED);

    /**
     * System property <code>h2.emergencySpaceInitial</code> (default: 262144).<br />
     * Size of 'reserve' file to detect disk full problems early.
     */
    public static final int EMERGENCY_SPACE_INITIAL = getIntSetting("h2.emergencySpaceInitial", 256 * 1024);

    /**
     * System property <code>h2.emergencySpaceMin</code> (default: 65536).<br />
     * Minimum size of 'reserve' file.
     */
    public static final int EMERGENCY_SPACE_MIN = getIntSetting("h2.emergencySpaceMin", 64 * 1024);

    /**
     * System property <code>h2.lobCloseBetweenReads</code> (default: false).<br />
     * Close LOB files between read operations.
     */
    public static boolean lobCloseBetweenReads = getBooleanSetting("h2.lobCloseBetweenReads", false);

    /**
     * System property <code>h2.lobFilesInDirectories</code> (default: false).<br />
     * Store LOB files in subdirectories.
     */
    // TODO: also remove DataHandler.allocateObjectId, createTempFile when setting this to true and removing it
    public static final boolean LOB_FILES_IN_DIRECTORIES = getBooleanSetting("h2.lobFilesInDirectories", false);

    /**
     * System property <code>h2.lobFilesPerDirectory</code> (default: 256).<br />
     * Maximum number of LOB files per directory.
     */
    public static final int LOB_FILES_PER_DIRECTORY = getIntSetting("h2.lobFilesPerDirectory", 256);

    /**
     * System property <code>h2.logAllErrors</code> (default: false).<br />
     * Write stack traces of any kind of error to a file.
     */
    public static final boolean LOG_ALL_ERRORS = getBooleanSetting("h2.logAllErrors", false);

    /**
     * System property <code>h2.logAllErrorsFile</code> (default: h2errors.txt).<br />
     * File name to log errors.
     */
    public static final String LOG_ALL_ERRORS_FILE = getStringSetting("h2.logAllErrorsFile", "h2errors.txt");

    /**
     * System property <code>h2.maxFileRetry</code> (default: 16).<br />
     * Number of times to retry file delete and rename.
     */
    public static final int MAX_FILE_RETRY = Math.max(1, getIntSetting("h2.maxFileRetry", 16));

    /**
     * System property <code>h2.maxQueryTimeout</code> (default: 0).<br />
     * The maximum timeout of a query. The default is 0, meaning no limit.
     */
    public static final int MAX_QUERY_TIMEOUT = getIntSetting(H2_MAX_QUERY_TIMEOUT, 0);

    /**
     * System property <code>h2.minColumnNameMap</code> (default: 3).<br />
     * The minimum number of columns where a hash table is created when result set
     * methods with column name (instead of column index) parameter are called.
     */
    public static final int MIN_COLUMN_NAME_MAP = getIntSetting("h2.minColumnNameMap", 3);

    /**
     * System property <code>h2.minWriteDelay</code> (default: 5).<br />
     * The minimum write delay that causes commits to be delayed.
     */
    public static final int MIN_WRITE_DELAY = getIntSetting("h2.minWriteDelay", 5);

    /**
     * System property <code>h2.objectCache</code> (default: true).<br />
     * Cache commonly used objects (integers, strings).
     */
    public static final boolean OBJECT_CACHE = getBooleanSetting("h2.objectCache", true);

    /**
     * System property <code>h2.objectCacheMaxPerElementSize</code> (default: 4096).<br />
     * Maximum size of an object in the cache.
     */
    public static final int OBJECT_CACHE_MAX_PER_ELEMENT_SIZE = getIntSetting("h2.objectCacheMaxPerElementSize", 4096);

    /**
     * System property <code>h2.objectCacheSize</code> (default: 1024).<br />
     * Maximum size of an object in the cache.
     */
    public static final int OBJECT_CACHE_SIZE = getIntSetting("h2.objectCacheSize", 1024);

    /**
     * System property <code>h2.optimizeDropDependencies</code> (default: true).<br />
     * Improve the performance of DROP and DROP ALL OBJECTS by quicker scanning if other objects depend on this object.
     */
    public static final boolean OPTIMIZE_DROP_DEPENDENCIES = getBooleanSetting("h2.optimizeDropDependencies", true);

    /**
     * System property <code>h2.optimizeDistinct</code> (default: true).<br />
     * Improve the performance of simple DISTINCT queries if an index is available for the given column.
     */
    public static final boolean OPTIMIZE_DISTINCT = getBooleanSetting("h2.optimizeDistinct", true);

    /**
     * System property <code>h2.optimizeEvaluatableSubqueries</code> (default: true).<br />
     * Optimize subqueries that are not dependent on the outer query.
     */
    public static final boolean OPTIMIZE_EVALUATABLE_SUBQUERIES = getBooleanSetting("h2.optimizeEvaluatableSubqueries", true);

    /**
     * System property <code>h2.optimizeIn</code> (default: true).<br />
     * Optimize IN(...) comparisons.
     */
    public static final boolean OPTIMIZE_IN = getBooleanSetting("h2.optimizeIn", true);

    /**
     * System property <code>h2.optimizeInJoin</code> (default: false).<br />
     * Optimize IN(...) comparisons by converting them to inner joins.
     */
    public static final boolean OPTIMIZE_IN_JOIN = getBooleanSetting("h2.optimizeInJoin", false);

    /**
     * System property <code>h2.optimizeMinMax</code> (default: true).<br />
     * Optimize MIN and MAX aggregate functions.
     */
    public static final boolean OPTIMIZE_MIN_MAX = getBooleanSetting("h2.optimizeMinMax", true);

    /**
     * System property <code>h2.optimizeSubqueryCache</code> (default: true).<br />
     * Cache subquery results.
     */
    public static final boolean OPTIMIZE_SUBQUERY_CACHE = getBooleanSetting("h2.optimizeSubqueryCache", true);

    /**
     * System property <code>h2.optimizeNot</code> (default: true).<br />
     * Optimize NOT conditions by removing the NOT and inverting the condition.
     */
    public static final boolean OPTIMIZE_NOT = getBooleanSetting("h2.optimizeNot", true);

    /**
     * System property <code>h2.optimizeTwoEquals</code> (default: true).<br />
     * Optimize expressions of the form A=B AND B=1. In this case, AND A=1 is added so an index on A can be used.
     */
    public static final boolean OPTIMIZE_TWO_EQUALS = getBooleanSetting("h2.optimizeTwoEquals", true);

    /**
     * System property <code>h2.overflowExceptions</code> (default: true).<br />
     * Throw an exception on integer overflows.
     */
    public static final boolean OVERFLOW_EXCEPTIONS = getBooleanSetting("h2.overflowExceptions", true);

    /**
     * System property <code>h2.recompileAlways</code> (default: false).<br />
     * Always recompile prepared statements.
     */
    public static final boolean RECOMPILE_ALWAYS = getBooleanSetting("h2.recompileAlways", false);

    /**
     * System property <code>h2.redoBufferSize</code> (default: 262144).<br />
     * Size of the redo buffer (used at startup when recovering).
     */
private int test;
//    public static final int REDO_BUFFER_SIZE = getIntSetting("h2.redoBufferSize", 256 * 1024);
    public static final int REDO_BUFFER_SIZE = getIntSetting("h2.redoBufferSize", 0);

    /**
     * System property <code>h2.runFinalize</code> (default: true).<br />
     * Run finalizers to detect unclosed connections.
     */
    public static boolean runFinalize = getBooleanSetting("h2.runFinalize", true);

    /**
     * System property <code>h2.scriptDirectory</code> (default: empty string).<br />
     * Relative or absolute directory where the script files are stored to or read from.
     */
    public static String scriptDirectory = getStringSetting("h2.scriptDirectory", "");

    /**
     * System property <code>h2.serverCachedObjects</code> (default: 64).<br />
     * TCP Server: number of cached objects per session.
     */
    public static final int SERVER_CACHED_OBJECTS = getIntSetting("h2.serverCachedObjects", 64);

    /**
     * System property <code>h2.serverResultSetFetchSize</code> (default: 100).<br />
     * The default result set fetch size when using the server mode.
     */
    public static final int SERVER_RESULT_SET_FETCH_SIZE = getIntSetting("h2.serverResultSetFetchSize", 100);

    /**
     * System property <code>h2.traceIO</code> (default: false).<br />
     * Trace all I/O operations.
     */
    public static final boolean TRACE_IO = getBooleanSetting("h2.traceIO", false);

    private static String baseDir = getStringSetting("h2.baseDir", null);

    private static boolean getBooleanSetting(String name, boolean defaultValue) {
        String s = getProperty(name);
        if (s != null) {
            try {
                return Boolean.valueOf(s).booleanValue();
            } catch (NumberFormatException e) {
            }
        }
        return defaultValue;
    }

    private static String getProperty(String name) {
        try {
            return System.getProperty(name);
        } catch (SecurityException e) {
            // applets may not do that - ignore
            return null;
        }
    }

    /**
     * INTERNAL
     */
    public static String getStringSetting(String name, String defaultValue) {
        String s = getProperty(name);
        return s == null ? defaultValue : s;
    }

    /**
     * INTERNAL
     */
    public static int getIntSetting(String name, int defaultValue) {
        String s = getProperty(name);
        if (s != null) {
            try {
                return Integer.decode(s).intValue();
            } catch (NumberFormatException e) {
            }
        }
        return defaultValue;
    }

    /**
     * INTERNAL
     */
    public static void setBaseDir(String dir) {
        if (!dir.endsWith("/")) {
            dir += "/";
        }
        baseDir = dir;
    }

    /**
     * INTERNAL
     */
    public static String getBaseDir() {
        return baseDir;
    }

    /**
     * INTERNAL
     */
    public static int getMaxQueryTimeout() {
        return getIntSetting(H2_MAX_QUERY_TIMEOUT, 0);
    }

    /**
     * INTERNAL
     */
    public static int getLogFileDeleteDelay() {
        int test;
//        return getIntSetting(H2_LOG_DELETE_DELAY, 0);
        return getIntSetting(H2_LOG_DELETE_DELAY, 100);
        // return getIntSetting(H2_LOG_DELETE_DELAY, Integer.MAX_VALUE);
    }
}
