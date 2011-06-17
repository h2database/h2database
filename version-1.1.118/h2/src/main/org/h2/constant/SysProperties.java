/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.constant;

import java.sql.ResultSet;
import org.h2.engine.Constants;
import org.h2.message.TraceSystem;
import org.h2.util.MathUtils;

/**
 * The constants defined in this class are initialized from system properties.
 * Those properties can be set when starting the virtual machine:
 * <pre>
 * java -Dh2.baseDir=/temp
 * </pre>
 * They can be set within the application, but this must be done before loading
 * any classes of this database (before loading the JDBC driver):
 * <pre>
 * System.setProperty(&quot;h2.baseDir&quot;, &quot;/temp&quot;);
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
     * INTERNAL
     */
    public static final String H2_COLLATOR_CACHE_SIZE = "h2.collatorCacheSize";

    /**
     * INTERNAL
     */
    public static final String H2_PAGE_STORE = "h2.pageStore";

    /**
     * System property <code>file.encoding</code> (default: Cp1252).<br />
     * It is usually set by the system and is the default encoding used for the
     * RunScript and CSV tool.
     */
    public static final String FILE_ENCODING = getStringSetting("file.encoding", "Cp1252");

    /**
     * System property <code>file.separator</code> (default: /).<br />
     * It is usually set by the system, and used to build absolute file names.
     */
    public static final String FILE_SEPARATOR = getStringSetting("file.separator", "/");

    /**
     * System property <code>java.specification.version</code>.<br />
     * It is set by the system. Examples: 1.4, 1.5, 1.6.
     */
    public static final String JAVA_SPECIFICATION_VERSION = getStringSetting("java.specification.version", "1.4");

    /**
     * System property <code>line.separator</code> (default: \n).<br />
     * It is usually set by the system, and used by the script and trace tools.
     */
    public static final String LINE_SEPARATOR = getStringSetting("line.separator", "\n");

    /**
     * System property <code>user.home</code> (default: empty string).<br />
     * It is usually set by the system, and used as a replacement for ~ in file
     * names.
     */
    public static final String USER_HOME = getStringSetting("user.home", "");

    /**
     * System property <code>h2.aliasColumnName</code>.<br />
     * When enabled, aliased columns (as in SELECT ID AS I FROM TEST) return the
     * alias (I in this case) in ResultSetMetaData.getColumnName() and 'null' in
     * getTableName(). If disabled, the real column name (ID in this case) and
     * table name is returned. This setting only affects the default mode.
     * <p>
     * When using different modes, this feature is disabled for compatibility
     * for all databases except MySQL. For MySQL, it is always enabled.
     * </p>
     * <p>
     * In version 1.0, it is enabled by default.
     * </p>
     * <p>
     * In version 1.1, it is disabled by default.
     * </p>
     */
    public static final boolean ALIAS_COLUMN_NAME = getBooleanSetting("h2.aliasColumnName", Constants.VERSION <= 1.0);

    /**
     * System property <code>h2.allowBigDecimalExtensions</code> (default:
     * false).<br />
     * When enabled, classes that extend BigDecimal are supported in
     * PreparedStatement.setBigDecimal.
     */
    public static final boolean ALLOW_BIG_DECIMAL_EXTENSIONS = getBooleanSetting("h2.allowBigDecimalExtensions", false);

    /**
     * System property <code>h2.allowedClasses</code> (default: *).<br />
     * Comma separated list of class names or prefixes.
     */
    public static final String ALLOWED_CLASSES = getStringSetting("h2.allowedClasses", "*");

    /**
     * System property <code>h2.browser</code> (default: null).<br />
     * The preferred browser to use. If not set, the default browser is used.
     * For Windows, to use the Internet Explorer, set this property to 'explorer'.
     * For Mac OS, if the default browser is not Safari and you want to use Safari,
     * use: <code>java -Dh2.browser="open,-a,Safari,%url" ...</code>.
     */
    public static final String BROWSER = getStringSetting("h2.browser", null);

    /**
     * System property <code>h2.enableAnonymousSSL</code> (default: true).<br />
     * When using SSL connection, the anonymous cipher suite
     * SSL_DH_anon_WITH_RC4_128_MD5 should be enabled.
     */
    public static final boolean ENABLE_ANONYMOUS_SSL = getBooleanSetting("h2.enableAnonymousSSL", true);

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
     * How many time the cache size value is divided by two to get the index
     * cache size. The index cache size is calculated like this: cacheSize >>
     * cacheSizeIndexShift.
     */
    public static final int CACHE_SIZE_INDEX_SHIFT = getIntSetting("h2.cacheSizeIndexShift", 3);

    /**
     * INTERNAL
     */
    public static final int CACHE_SIZE_INDEX_DEFAULT = CACHE_SIZE_DEFAULT >> CACHE_SIZE_INDEX_SHIFT;

    /**
     * System property <code>h2.cacheTypeDefault</code> (default: LRU).<br />
     * How many time the cache size value is divided by two to get the index
     * cache size. The index cache size is calculated like this: cacheSize >>
     * cacheSizeIndexShift.
     */
    public static final String CACHE_TYPE_DEFAULT = getStringSetting("h2.cacheTypeDefault", "LRU");

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
     * System property <code>h2.clientTraceDirectory</code> (default:
     * trace.db/).<br />
     * Directory where the trace files of the JDBC client are stored (only for
     * client / server).
     */
    public static final String CLIENT_TRACE_DIRECTORY = getStringSetting("h2.clientTraceDirectory", "trace.db/");

    /**
     * System property <code>h2.defaultEscape</code> (default: \).<br />
     * The default escape character for LIKE comparisons. To select no escape
     * character, use an empty string.
     */
    public static final Character DEFAULT_ESCAPE_CHAR = getEscapeChar(getStringSetting("h2.defaultEscape", "\\"));

    /**
     * System property <code>h2.defaultMaxOperationMemory</code> (default:
     * 100000).<br />
     * The default for the setting MAX_OPERATION_MEMORY.
     */
    public static final int DEFAULT_MAX_OPERATION_MEMORY = getIntSetting("h2.defaultMaxOperationMemory", 100000);

    /**
     * System property <code>h2.defaultResultSetConcurrency</code> (default:
     * ResultSet.CONCUR_READ_ONLY).<br />
     * The default result set concurrency for statements created with
     * Connection.createStatement() or prepareStatement(String sql).
     */
    public static final int DEFAULT_RESULT_SET_CONCURRENCY = getIntSetting("h2.defaultResultSetConcurrency", ResultSet.CONCUR_READ_ONLY);

    /**
     * System property <code>h2.dataSourceTraceLevel</code> (default: 1).<br />
     * The trace level of the data source implementation. Default is 1 for
     * error.
     */
    public static final int DATASOURCE_TRACE_LEVEL = getIntSetting("h2.dataSourceTraceLevel", TraceSystem.ERROR);

    /**
     * System property <code>h2.defaultMaxMemoryUndo</code> (default: 50000).<br />
     * The default value for the MAX_MEMORY_UNDO setting.
     */
    public static final int DEFAULT_MAX_MEMORY_UNDO = getIntSetting("h2.defaultMaxMemoryUndo", 50000);

    /**
     * System property <code>h2.defaultLockMode</code> (default: 3).<br />
     * The default value for the LOCK_MODE setting.
     */
    public static final int DEFAULT_LOCK_MODE = getIntSetting("h2.defaultLockMode", Constants.LOCK_MODE_READ_COMMITTED);

    /**
     * System property <code>h2.delayWrongPasswordMin</code> (default: 250).<br />
     * The minimum delay in milliseconds before an exception is thrown for using
     * the wrong user name or password. This slows down brute force attacks. The
     * delay is reset to this value after a successful login. Unsuccessful
     * logins will double the time until DELAY_WRONG_PASSWORD_MAX.
     */
    public static final int DELAY_WRONG_PASSWORD_MIN = getIntSetting("h2.delayWrongPasswordMin", 250);

    /**
     * System property <code>h2.delayWrongPasswordMax</code> (default: 4000).<br />
     * The maximum delay in milliseconds before an exception is thrown for using
     * the wrong user name or password. This slows down brute force attacks. The
     * delay is reset after a successful login. The value 0 means there is no
     * maximum delay.
     */
    public static final int DELAY_WRONG_PASSWORD_MAX = getIntSetting("h2.delayWrongPasswordMax", 4000);

    /**
     * System property <code>h2.dollarQuoting</code> (default: true).<br />
     * Dollar quoting is used to quote text without having to use escape
     * characters. A dollar quoted string starts and ends with $$. Inside the
     * text, $$ is not allowed.
     */
    public static final boolean DOLLAR_QUOTING = getBooleanSetting("h2.dollarQuoting", true);

    /**
     * System property <code>h2.estimatedFunctionTableRows</code> (default:
     * 1000).<br />
     * The estimated number of rows in a function table (for example, CSVREAD or
     * FTL_SEARCH). This value is used by the optimizer.
     */
    public static final int ESTIMATED_FUNCTION_TABLE_ROWS = getIntSetting("h2.estimatedFunctionTableRows", 1000);

    /**
     * System property <code>h2.largeResultBufferSize</code> (default: 4096).<br />
     * Buffer size for large result sets. Set this value to 0 to disable the
     * buffer.
     */
    public static final int LARGE_RESULT_BUFFER_SIZE = getIntSetting("h2.largeResultBufferSize", 4 * 1024);

    /**
     * System property <code>h2.lobCloseBetweenReads</code> (default: false).<br />
     * Close LOB files between read operations.
     */
    public static boolean lobCloseBetweenReads = getBooleanSetting("h2.lobCloseBetweenReads", false);

    /**
     * System property <code>h2.lobFilesInDirectories</code>.<br />
     * Store LOB files in subdirectories.
     * <p>
     * In version 1.0, it is disabled by default.
     * </p><p>
     * In version 1.1, it is enabled by default.
     * </p>
     */
    // TODO: when removing this property, also remove
    // DataHandler.allocateObjectId, createTempFile it
    public static final boolean LOB_FILES_IN_DIRECTORIES = getBooleanSetting("h2.lobFilesInDirectories", Constants.VERSION > 1.0);

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
     * System property <code>h2.logAllErrorsFile</code> (default:
     * h2errors.txt).<br />
     * File name to log errors.
     */
    public static final String LOG_ALL_ERRORS_FILE = getStringSetting("h2.logAllErrorsFile", "h2errors.txt");

    /**
     * System property <code>h2.maxFileRetry</code> (default: 16).<br />
     * Number of times to retry file delete and rename. in Windows, files can't
     * be deleted if they are open. Waiting a bit can help (sometimes the
     * Windows Explorer opens the files for a short time) may help. Sometimes,
     * running garbage collection may close files if the user forgot to call
     * Connection.close() or InputStream.close().
     */
    public static final int MAX_FILE_RETRY = Math.max(1, getIntSetting("h2.maxFileRetry", 16));

    /**
     * System property <code>h2.maxMemoryRowsDistinct</code> (default:
     * Integer.MAX_VALUE).<br />
     * The maximum number of rows kept in-memory for SELECT DISTINCT queries. If
     * more than this number of rows are in a result set, a temporary table is
     * used.
     */
    public static final int MAX_MEMORY_ROWS_DISTINCT = getIntSetting("h2.maxMemoryRowsDistinct", Integer.MAX_VALUE);

    /**
     * System property <code>h2.maxQueryTimeout</code> (default: 0).<br />
     * The maximum timeout of a query. The default is 0, meaning no limit.
     */
    public static final int MAX_QUERY_TIMEOUT = getIntSetting(H2_MAX_QUERY_TIMEOUT, 0);

    /**
     * System property <code>h2.maxReconnect</code> (default: 3).<br />
     * The maximum number of tries to reconnect in a row.
     */
    public static final int MAX_RECONNECT = getIntSetting("h2.maxReconnect", 3);

    /**
     * System property <code>h2.maxTraceDataLength</code> (default: 65535).<br />
     * The maximum size of a LOB value that is written as data to the trace system.
     */
    public static final long MAX_TRACE_DATA_LENGTH = getIntSetting("h2.maxTraceDataLength", 65535);

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
     * System property <code>h2.nioLoadMapped</code> (default: false).<br />
     * If the mapped buffer should be loaded when the file is opened.
     * This can improve performance.
     */
    public static final boolean NIO_LOAD_MAPPED = getBooleanSetting("h2.nioLoadMapped", false);

    /**
     * System property <code>h2.nioCleanerHack</code> (default: true).<br />
     * If possible, use a hack to un-map the mapped file. See also
     * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038
     */
    public static final boolean NIO_CLEANER_HACK = getBooleanSetting("h2.nioCleanerHack", true);

    /**
     * System property <code>h2.objectCache</code> (default: true).<br />
     * Cache commonly used objects (integers, strings).
     */
    public static final boolean OBJECT_CACHE = getBooleanSetting("h2.objectCache", true);

    /**
     * System property <code>h2.objectCacheMaxPerElementSize</code> (default:
     * 4096).<br />
     * Maximum size of an object in the cache.
     */
    public static final int OBJECT_CACHE_MAX_PER_ELEMENT_SIZE = getIntSetting("h2.objectCacheMaxPerElementSize", 4096);

    /**
     * System property <code>h2.objectCacheSize</code> (default: 1024).<br />
     * Maximum size of an object in the cache.
     * This value must be a power of 2.
     */
    public static final int OBJECT_CACHE_SIZE = MathUtils.nextPowerOf2(getIntSetting("h2.objectCacheSize", 1024));

    /**
     * System property <code>h2.oldCommandLineOptions</code>.<br />
     * Support old command line options.
     * <p>
     * In version 1.0, it is enabled by default.
     * </p><p>
     * In version 1.1, it is disabled by default.
     * </p>
     */
    public static final boolean OLD_COMMAND_LINE_OPTIONS = getBooleanSetting("h2.oldCommandLineOptions", Constants.VERSION < 1.1);

    /**
     * System property <code>h2.optimizeDropDependencies</code> (default:
     * true).<br />
     * Improve the performance of DROP and DROP ALL OBJECTS by quicker scanning
     * if other objects depend on this object.
     */
    public static final boolean OPTIMIZE_DROP_DEPENDENCIES = getBooleanSetting("h2.optimizeDropDependencies", true);

    /**
     * System property <code>h2.optimizeDistinct</code> (default: true).<br />
     * Improve the performance of simple DISTINCT queries if an index is
     * available for the given column. The optimization is used if:
     * <ul>
     * <li>The select is a single column query without condition </li>
     * <li>The query contains only one table, and no group by </li>
     * <li>There is only one table involved </li>
     * <li>There is an ascending index on the column </li>
     * <li>The selectivity of the column is below 20 </li>
     * </ul>
     */
    public static final boolean OPTIMIZE_DISTINCT = getBooleanSetting("h2.optimizeDistinct", true);

    /**
     * System property <code>h2.optimizeEvaluatableSubqueries</code> (default:
     * true).<br />
     * Optimize subqueries that are not dependent on the outer query.
     */
    public static final boolean OPTIMIZE_EVALUATABLE_SUBQUERIES = getBooleanSetting("h2.optimizeEvaluatableSubqueries", true);

    /**
     * System property <code>h2.optimizeGroupSorted</code>.<br />
     * Optimize GROUP BY queries if an index can be used that matches the group
     * by columns.
     * <p>
     * In version 1.0, it is disabled by default.
     * </p><p>
     * In version 1.1, it is enabled by default.
     * </p>
     */
    public static final boolean OPTIMIZE_GROUP_SORTED = getBooleanSetting("h2.optimizeGroupSorted", Constants.VERSION > 1.0);

    /**
     * System property <code>h2.optimizeIn</code> (default: true).<br />
     * Optimize IN(...) comparisons.
     */
    public static final boolean OPTIMIZE_IN = getBooleanSetting("h2.optimizeIn", true);

    /**
     * System property <code>h2.optimizeInList</code> (default: false).<br />
     * Optimize IN(...) and IN(SELECT ...) comparisons. This includes
     * optimization for SELECT, DELETE, and UPDATE. Overrides h2.optimizeIn and
     * h2.optimizeInJoin if enabled.
     */
    public static final boolean OPTIMIZE_IN_LIST = getBooleanSetting("h2.optimizeInList", false);

    /**
     * System property <code>h2.optimizeInJoin</code> (default: false).<br />
     * Optimize IN(...) comparisons by converting them to inner joins.
     * <p>
     * In version 1.0, it is disabled by default.
     * </p><p>
     * In version 1.1, it is enabled by default.
     * </p>
     */
    public static boolean optimizeInJoin = getBooleanSetting("h2.optimizeInJoin", Constants.VERSION > 1.0);

    /**
     * System property <code>h2.optimizeMinMax</code> (default: true).<br />
     * Optimize MIN and MAX aggregate functions.
     */
    public static final boolean OPTIMIZE_MIN_MAX = getBooleanSetting("h2.optimizeMinMax", true);

    /**
     * System property <code>h2.optimizeNot</code> (default: true).<br />
     * Optimize NOT conditions by removing the NOT and inverting the condition.
     */
    public static final boolean OPTIMIZE_NOT = getBooleanSetting("h2.optimizeNot", true);

    /**
     * System property <code>h2.optimizeOr</code> (default: false).<br />
     * Convert (C=? OR C=?) to (C IN(?, ?)).
     */
    public static final boolean OPTIMIZE_OR = getBooleanSetting("h2.optimizeOr", false);

    /**
     * System property <code>h2.optimizeSubqueryCache</code> (default: true).<br />
     * Cache subquery results.
     */
    public static final boolean OPTIMIZE_SUBQUERY_CACHE = getBooleanSetting("h2.optimizeSubqueryCache", true);

    /**
     * System property <code>h2.optimizeTwoEquals</code> (default: true).<br />
     * Optimize expressions of the form A=B AND B=1. In this case, AND A=1 is
     * added so an index on A can be used.
     */
    public static final boolean OPTIMIZE_TWO_EQUALS = getBooleanSetting("h2.optimizeTwoEquals", true);

    /**
     * System property <code>h2.overflowExceptions</code> (default: true).<br />
     * Throw an exception on integer overflows.
     */
    public static final boolean OVERFLOW_EXCEPTIONS = getBooleanSetting("h2.overflowExceptions", true);

    /**
     * System property <code>h2.pgClientEncoding</code> (default: UTF-8).<br />
     * Default client encoding for PG server. It is used if the client does not
     * sends his encoding.
     */
    public static final String PG_DEFAULT_CLIENT_ENCODING = getStringSetting("h2.pgClientEncoding", "UTF-8");

    /**
     * System property <code>h2.recompileAlways</code> (default: false).<br />
     * Always recompile prepared statements.
     */
    public static final boolean RECOMPILE_ALWAYS = getBooleanSetting("h2.recompileAlways", false);

    /**
     * System property <code>h2.reconnectCheckDelay</code> (default: 200).<br />
     * Check the .lock.db file every this many milliseconds to detect that the
     * database was changed. The process writing to the database must first
     * notify a change in the .lock.db file, then wait twice this many
     * milliseconds before updating the database.
     */
    public static final int RECONNECT_CHECK_DELAY = getIntSetting("h2.reconnectCheckDelay", 200);

    /**
     * System property <code>h2.redoBufferSize</code> (default: 262144).<br />
     * Size of the redo buffer (used at startup when recovering).
     */
    public static final int REDO_BUFFER_SIZE = getIntSetting("h2.redoBufferSize", 256 * 1024);

    /**
     * System property <code>h2.reserveMemory</code> (default: 524288).<br />
     * This many bytes in main memory are allocated as a reserve. This reserve
     * is freed up when if no memory is available, so that rolling back a large
     * transaction is easier.
     */
    public static final int RESERVE_MEMORY = getIntSetting("h2.reserveMemory", 512 * 1024);

    /**
     * System property <code>h2.returnLobObjects</code>.<br />
     * When true, ResultSet.getObject for CLOB or BLOB will return a
     * java.sql.Clob / java.sql.Blob object. When set to false, it will return a
     * java.io.Reader / java.io.InputStream.
     * <p>
     * In version 1.0, it is disabled by default.
     * </p>
     * <p>
     * In version 1.1, it is enabled by default.
     * </p>
     */
    public static final boolean RETURN_LOB_OBJECTS = getBooleanSetting("h2.returnLobObjects", Constants.VERSION > 1.0);

    /**
     * System property <code>h2.reuseSpaceAfter</code> (default: 16).<br />
     * Reuse space in database files after this many pages are free.
     */
    public static final int REUSE_SPACE_AFTER = getIntSetting("h2.reuseSpaceAfter", 32);

    /**
     * System property <code>h2.reuseSpaceBtreeIndex</code> (default: true).<br />
     * Reuse space after deleting or updating many rows in a b-tree index.
     */
    public static final boolean REUSE_SPACE_BTREE_INDEX = getBooleanSetting("h2.reuseSpaceBtreeIndex", true);

    /**
     * System property <code>h2.reuseSpaceQuickly</code> (default: true).<br />
     * Reuse space in database files quickly.
     */
    public static final boolean REUSE_SPACE_QUICKLY = getBooleanSetting("h2.reuseSpaceQuickly", true);

    /**
     * System property <code>h2.runFinalize</code> (default: true).<br />
     * Run finalizers to detect unclosed connections.
     */
    public static boolean runFinalize = getBooleanSetting("h2.runFinalize", true);

    /**
     * System property <code>h2.serverCachedObjects</code> (default: 64).<br />
     * TCP Server: number of cached objects per session.
     */
    public static final int SERVER_CACHED_OBJECTS = getIntSetting("h2.serverCachedObjects", 64);

    /**
     * System property <code>h2.serverResultSetFetchSize</code>
     * (default: 100).<br />
     * The default result set fetch size when using the server mode.
     */
    public static final int SERVER_RESULT_SET_FETCH_SIZE = getIntSetting("h2.serverResultSetFetchSize", 100);

    /**
     * System property <code>h2.shareLinkedConnections</code>.<br />
     * Linked connections should be shared, that means connections to the same
     * database should be used for all linked tables that connect to the same
     * database.
     * <p>
     * In version 1.0, it is disabled by default.
     * </p>
     * <p>
     * In version 1.1, it is enabled by default.
     * </p>
     */
    public static final boolean SHARE_LINKED_CONNECTIONS = getBooleanSetting("h2.shareLinkedConnections", Constants.VERSION > 1.0);

    /**
     * System property <code>h2.socketConnectTimeout</code> (default: 2000).<br />
     * The timeout in milliseconds to connect to a server.
     */
    public static final int SOCKET_CONNECT_TIMEOUT = getIntSetting("h2.socketConnectTimeout", 2000);

    /**
     * System property <code>h2.sortNullsHigh</code> (default: false).<br />
     * Invert the default sorting behavior for NULL values, such that NULL
     * values are sorted to the end of a result set in an ascending sort and to
     * the beginning of a result set in a descending sort.
     */
    public static final boolean SORT_NULLS_HIGH = getBooleanSetting("h2.sortNullsHigh", false);

    /**
     * System property <code>h2.splitFileSizeShift</code> (default: 30).<br />
     * The maximum file size of a split file is 1L &lt;&lt; x.
     */
    public static final long SPLIT_FILE_SIZE_SHIFT = getIntSetting("h2.splitFileSizeShift", 30);

    /**
     * System property <code>h2.traceIO</code> (default: false).<br />
     * Trace all I/O operations.
     */
    public static final boolean TRACE_IO = getBooleanSetting("h2.traceIO", false);

    /**
     * System property <code>h2.webMaxValueLength</code> (default: 10000).<br />
     * The H2 Console will abbreviate result values larger than this size.
     */
    public static final int WEB_MAX_VALUE_LENGTH = getIntSetting("h2.webMaxValueLength", 10000);

    /**
     * System property <code>h2.collatorCacheSize</code> (default: 10000).<br />
     * The cache size for collation keys (in elements). Used when a collator has
     * been set for the database.
     */
    public static final int COLLATOR_CACHE_SIZE = getCollatorCacheSize();

    /**
     * The current time this class was loaded (in milliseconds).
     */
    public static final long TIME_START = System.currentTimeMillis();

    private static final String H2_BASE_DIR = "h2.baseDir";

    private SysProperties() {
        // utility class
    }

    private static boolean getBooleanSetting(String name, boolean defaultValue) {
        String s = getProperty(name);
        if (s != null) {
            try {
                return Boolean.valueOf(s).booleanValue();
            } catch (NumberFormatException e) {
                // ignore
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
                // ignore
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
        System.setProperty(H2_BASE_DIR, dir);
    }

    /**
     * INTERNAL
     */
    public static String getBaseDir() {
        return getStringSetting(H2_BASE_DIR, null);
    }

    /**
     * INTERNAL.
     * System property <code>h2.scriptDirectory</code> (default: empty
     * string).<br />
     * Relative or absolute directory where the script files are stored to or
     * read from.
     */
    public static String getScriptDirectory() {
        return getStringSetting("h2.scriptDirectory", "");
    }

    /**
     * INTERNAL
     */
    public static void setScriptDirectory(String dir) {
        System.setProperty("h2.scriptDirectory", dir);
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
        return getIntSetting(H2_LOG_DELETE_DELAY, 0);
    }

    /**
     * INTERNAL
     */
    public static int getCollatorCacheSize() {
        return getIntSetting(H2_COLLATOR_CACHE_SIZE, 32000);
    }

    /**
     * INTERNAL
     */
    public static boolean getPageStore() {
        return getBooleanSetting(H2_PAGE_STORE, false);
    }

    private static Character getEscapeChar(String s) {
        return s == null || s.length() == 0 ? null : s.charAt(0);
    }

}
