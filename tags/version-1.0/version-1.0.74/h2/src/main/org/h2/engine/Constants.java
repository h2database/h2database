/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

/*
 * Release checklist
 * - Test with Hibernate
 * - Run FindBugs
 * - build jarClient, check jar file size
 * - build jar, test with IKVM
 * - build javadocImpl
 * - Compile with JDK 1.6:
 *   set path=C:\Programme\Java\jdk1.6.0\bin;%PATH%
 *   set JAVA_HOME=C:\Programme\Java\jdk1.6.0
 *   set path=C:\Program Files\Java\jdk1.6.0_05\bin;%PATH%
 *   set JAVA_HOME=C:\Program Files\Java\jdk1.6.0_05
 *   build compile
 *   set classpath=
 *   build javadoc
 *   build javadocImpl (to find missing javadocs)
 * - reset to JDK 1.4
 *   build compile
 * - Change version and build number in
 *     Constants.java
 * - Maybe increase TCP_DRIVER_VERSION (old clients must be compatible!)
 * - Check code coverage
 * - No "  Message.get" (must be "throw Message.get")
 * - Check that is no TODO in the docs
 * - Run regression test with JDK 1.4 and 1.5
 * - Use latest versions of other dbs (Derby 10.4.1.3; 
 *      PostgreSQL 8.3.1; MySQL 5.0.51)
 * - Change version(s) in performance.html
 * - Run 'build benchmark' (with JDK 1.4 currently)
 * - Copy the benchmark results and update the performance page and diagram
 * - Documentation: check if all Javadoc files are in the index
 * - Update the changelog (add new version)
 * - Update the newsfeed
 * - build docs
 * - Check dataWeb/index.html, versions and links in main, downloads, build
 * - PDF
 *      - footer
 *      - front page
 *      - orphan control
 *      - check images
 *      - table of contents
 * - build all
 * - Copy the pdf file to h2/docs
 * - build zip
 * - Windows installer (nsis)
 * - Test Console
 * - Test all languages
 * - Scan for viruses
 * - build mavenDeployCentral
 * - Upload to SourceForge
 * - svn commit
 * - svn copy: /svn/trunk /svn/tags/version-1.0.x; Version 1.0.x (yyyy-mm-dd)
 * - Newsletter: prepare (always to BCC!!)
 * - Upload to h2database.com, http://code.google.com/p/h2database/downloads/list
 * - Newsletter: send (always to BCC!!)
 * - Add to freshmeat
 * - http://en.wikipedia.org/wiki/H2_%28DBMS%29 (change version)
 * - http://www.heise.de/software/
 */
/**
 * Constants are fixed values that are used in the whole database code.
 */
public class Constants {
    
    /**
     * The build id is incremented for each public release.
     */
    public static final int BUILD_ID = 74;

    /**
     * The build id of the previous release.
     */
    public static final int BUILD_ID_PREVIOUS = 73;

    /**
     * The build date is updated for each public release.
     */
    public static final String BUILD_DATE = "2008-06-21";

    /**
     * The build date is updated for each public release.
     */
    public static final String BUILD_DATE_PREVIOUS = "2008-05-31";

    /**
     * The TCP protocol version number 5. This protocol is used by the TCP
     * server and remote JDBC client.
     */
    public static final int TCP_PROTOCOL_VERSION_5 = 5;
    
    /**
     * The TCP protocol version number 6. This protocol is used by the TCP
     * server and remote JDBC client.
     */
    public static final int TCP_PROTOCOL_VERSION_6 = 6;
    
    /**
     * The major version of this product.
     */
    public static final int VERSION_MAJOR = 1;
    
    /**
     * The minor version of this product.
     */
    public static final int VERSION_MINOR = 0;
    
    /**
     * If empty b-tree pages are allowed. This is supported for backward
     * compatibility.
     */
    public static final boolean ALLOW_EMPTY_BTREE_PAGES = true;
    
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
     * Automatically convert large LOB objects to files even if they have been
     * set using setBytes.
     */
    public static final boolean AUTO_CONVERT_LOB_TO_FILES = true;
    
    /**
     * The maximum scale of a BigDecimal value.
     */
    public static final int BIG_DECIMAL_SCALE_MAX = 100000;

    /**
     * The minimum number of entries to keep in the cache.
     */
    public static final int CACHE_MIN_RECORDS = 16;
    
    /**
     * The name of the character set used in this database.
     */
    public static final String CHARACTER_SET_NAME = "Unicode";
    
    /**
     * The value of the cluster setting if clustering is disabled.
     */
    public static final String CLUSTERING_DISABLED = "''";
    
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
     * If the rounding mode should be BigDecimal.ROUND_HALF_UP when rounding
     * from a decimal value to a long value.
     */
    public static final boolean CONVERT_TO_LONG_ROUND = true;
    
    /**
     * The cost is calculated on rowcount + this offset,
     * to avoid using the wrong or no index if the table 
     * contains no rows _currently_ (when preparing the statement)
     */
    public static final int COST_ROW_OFFSET = 1000;
    
    /**
     * The default name of the system user. This name is only used as long as
     * there is no administrator user registered.
     */
    public static final String DBA_NAME = "DBA";
    
    /**
     * The number of milliseconds after which to check for a deadlock if locking
     * is not successful.
     */
    public static final int DEADLOCK_CHECK = 100;

    /**
     * The default value of the ALLOW_LITERALS setting
     */
    public static final int DEFAULT_ALLOW_LITERALS = ALLOW_LITERALS_ALL;
    
    /**
     * The default data page size.
     */
    public static final int DEFAULT_DATA_PAGE_SIZE = 512;
    
    /**
     * The default escape character for LIKE comparisons.
     */
    public static final char DEFAULT_ESCAPE_CHAR = '\\';
    
    /**
     * The default port number of the FTP server.
     */
    public static final int DEFAULT_FTP_PORT = 8021;

    /**
     * If the HTTP server should allow connections from other computers by
     * default.
     */
    public static final boolean DEFAULT_HTTP_ALLOW_OTHERS = false;
    
    /**
     * The default port number of the HTTP server (for the H2 Console). This
     * value is also in the documentation.
     */
    public static final int DEFAULT_HTTP_PORT = 8082;
    
    /**
     * The default SSL setting for the HTTP server.
     */
    public static final boolean DEFAULT_HTTP_SSL = false;
    
    /**
     * The default value for the maximum log file size.
     */
    public static final long DEFAULT_MAX_LOG_SIZE = 32 * 1024 * 1024;
    
    /**
     * The default maximum length on an in-memory LOB object.
     * Larger objects will be written to a temporary file.
     */
    public static final int DEFAULT_MAX_LENGTH_CLIENTSIDE_LOB = 65536;
    
    /**
     * The default maximum length of an LOB that is stored in the data file itself.
     */
    public static final int DEFAULT_MAX_LENGTH_INPLACE_LOB = 1024;
    
    /**
     * The default maximum number of rows to be kept in memory in a result set.
     */
    public static final int DEFAULT_MAX_MEMORY_ROWS = 10000;
    
    /**
     * The default port of the TCP server.
     * This port is also used in the documentation.
     */
    public static final int DEFAULT_SERVER_PORT = 9092;
    
    /**
     * The default table type when creating new tables.
     */
    public static final int DEFAULT_TABLE_TYPE = 0;
    
    /**
     * The default delay in milliseconds before the log file is written.
     */
    public static final int DEFAULT_WRITE_DELAY = 500;

    /**
     * The name of the JDBC driver.
     */
    public static final String DRIVER_NAME = "H2 JDBC Driver";
    
    /**
     * The password is hashed this many times
     * to slow down dictionary attacks.
     */
    public static final int ENCRYPTION_KEY_HASH_ITERATIONS = 1024;

    /**
     * The 'word size' of a file (the minimum allocation size).
     */
    public static final int FILE_BLOCK_SIZE = 16;
    
    /**
     * The maximum number of bytes a file should be expanded in one step.
     */
    public static final int FILE_MAX_INCREMENT = 32 * 1024 * 1024;
    
    /**
     * The minimum file size in bytes.
     */
    public static final int FILE_MIN_SIZE = 128 * 1024;
    
    /**
     * The page size of a file.
     */
    public static final int FILE_PAGE_SIZE = 8 * 1024;
    
    /**
     * The default delay to flush indexes. 0 means indexes are not flushed.
     */
    public static final long FLUSH_INDEX_DELAY = 0;

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
     * The lock mode that means no locking is used at all.
     */
    public static final int LOCK_MODE_OFF = 0;
    
    /**
     * The lock mode that means read locks are acquired, but they are released
     * immediately after the statement is executed.
     */
    public static final int LOCK_MODE_READ_COMMITTED = 3;
    
    /**
     * The lock mode that means row level locks are used if possible.
     * This lock mode is similar to read committed, but row level locks are
     * used instead of table level locks.
     */
    public static final int LOCK_MODE_ROW = 4;
    
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
     * The number of milliseconds to wait between checking the .lock.db file
     * still exists once a database is locked.
     */
    public static final int LOCK_SLEEP = 1000;
    
    /**
     * The divider used to calculate the minimum log file size as a function of
     * the largest file (data file or index file).
     */
    public static final long LOG_SIZE_DIVIDER = 10;

    /**
     * The file header used for binary files.
     */
    public static final String MAGIC_FILE_HEADER = "-- H2 0.5/B --      ".substring(0, FILE_BLOCK_SIZE - 1) + "\n";
    
    /**
     * The file header used for text files.
     */
    public static final String MAGIC_FILE_HEADER_TEXT = "-- H2 0.5/T --      ".substring(0, FILE_BLOCK_SIZE - 1) + "\n";
    
    /**
     * The name of the in-memory management database used by the TCP server
     * to keep the active sessions.
     */
    public static final String MANAGEMENT_DB_PREFIX = "management_db_";
    
    /**
     * The user name of the management database.
     */
    public static final String MANAGEMENT_DB_USER = "sa";

    /**
     * The highest possible parameter index.
     */
    public static final int MAX_PARAMETER_INDEX = 100000;
    
    /**
     * The number of bytes in random salt that is used to hash passwords.
     */
    public static final int SALT_LEN = 8;
    
    /**
     * The database URL prefix of this database.
     */
    public static final String START_URL = "jdbc:h2:";

    /**
     * The name prefix used for indexes that are not explicitly named.
     */
    public static final String PREFIX_INDEX = "INDEX_";
    
    /**
     * The name prefix used for primary key constraints that are not explicitly
     * named.
     */
    public static final String PREFIX_PRIMARY_KEY = "PRIMARY_KEY_";
    
    /**
     * The product name. This value must stay like that, see
     * http://opensource.atlassian.com/projects/hibernate/browse/HHH-2682
     */
    public static final String PRODUCT_NAME = "H2";
    
    /**
     * Every user belongs to this role.
     */
    public static final String PUBLIC_ROLE_NAME = "PUBLIC";
    
    /**
     * The name of the schema that contains the information schema tables.
     */
    public static final String SCHEMA_INFORMATION = "INFORMATION_SCHEMA";
    
    /**
     * The name of the default schema.
     */
    public static final String SCHEMA_MAIN = "PUBLIC";
    
    /**
     * The default name of the script file if .zip compression is used.
     */
    public static final String SCRIPT_SQL = "script.sql";
    
    /**
     * The default sample size for the ANALYZE statement.
     */
    public static final int SELECTIVITY_ANALYZE_SAMPLE_ROWS = 10000;
    
    /**
     * The default selectivity (used if the selectivity is not calculated).
     */
    public static final int SELECTIVITY_DEFAULT = 50;
    
    /**
     * The number of distinct values to keep in memory when running ANALYZE.
     */
    public static final int SELECTIVITY_DISTINCT_COUNT = 10000;
    
    /**
     * Whether Java objects should be serialized / de-serialized in the JDBC API.
     */
    public static final boolean SERIALIZE_JAVA_OBJECTS = true;
    
    /**
     * The name of the server properties file.
     */
    public static final String SERVER_PROPERTIES_FILE = ".h2.server.properties";
    
    /**
     * The title of the server properties file.
     */
    public static final String SERVER_PROPERTIES_TITLE = "H2 Server Properties";
    
    /**
     * Queries that take longer than this number of milliseconds are written to
     * the trace file with the level info.
     */
    public static final long SLOW_QUERY_LIMIT_MS = 100;

    /**
     * The file name suffix of data files.
     */
    public static final String SUFFIX_DATA_FILE = ".data.db";
    
    /**
     * The file name suffix of all database files.
     */
    public static final String SUFFIX_DB_FILE = ".db";
    
    /**
     * The file name suffix of index files.
     */
    public static final String SUFFIX_INDEX_FILE = ".index.db";
    
    /**
     * The file name suffix of file lock files that are used to make sure a
     * database is open by only one process at any time.
     */
    public static final String SUFFIX_LOCK_FILE = ".lock.db";
    
    /**
     * The file name suffix of large object files.
     */
    public static final String SUFFIX_LOB_FILE = ".lob.db";
    
    /**
     * The suffix of the directory name used if LOB objects are stored in a
     * directory.
     */
    public static final String SUFFIX_LOBS_DIRECTORY = ".lobs.db";
    
    /**
     * The file name suffix of transaction log files.
     */
    public static final String SUFFIX_LOG_FILE = ".log.db";
    
    /**
     * The file name suffix of temporary files.
     */
    public static final String SUFFIX_TEMP_FILE = ".temp.db";

    /**
     * The file name suffix of trace files.
     */
    public static final String SUFFIX_TRACE_FILE = ".trace.db";
    
    /**
     * The file name suffix of the signal file that starts trace output.
     */
    public static final String SUFFIX_TRACE_START_FILE = ".start";
    
    /**
     * The table name suffix used to create internal temporary tables.
     */
    public static final String TEMP_TABLE_PREFIX = "TEMP_TABLE_";
    
    /**
     * The delay that is to be used if throttle has been enabled.
     */
    public static final int THROTTLE_DELAY = 50;
    
    /**
     * The database URL format in simplified Backus-Naur form.
     */
    public static final String URL_FORMAT = START_URL + 
    "{ {.|mem:}[name] | [file:]fileName | {tcp|ssl}:[//]server[:port][,server2[:port]]/name }[;key=value...]";

    /**
     * Name of the character encoding format.
     */
    public static final String UTF8 = "UTF8";
    
    /**
     * The major version number of the supported JDBC API.
     */
    public static final int VERSION_JDBC_MAJOR = 3;

    /**
     * The minor version number of the supported JDBC API.
     */
    public static final int VERSION_JDBC_MINOR = 0;

    /**
     * The maximum time in milliseconds to keep the cost of a view.
     * 10000 means 10 seconds.
     */
    public static final int VIEW_COST_CACHE_MAX_AGE = 10000;
    
    /**
     * The name of the index cache that is used for temporary view (subqueries
     * used as tables).
     */
    public static final int VIEW_INDEX_CACHE_SIZE = 64;

    private Constants() {
        // utility class
    }
    
    /**
     * Get the version of this product, consisting of major version, minor version,
     * and build id.
     * 
     * @return the version number
     */
    public static String getVersion() {
        return VERSION_MAJOR + "." + VERSION_MINOR + "." + BUILD_ID;
    }
    
    /**
     * Get the version of the previous version.
     * 
     * @return the version number
     */
    public static Object getVersionPrevious() {
        return VERSION_MAJOR + "." + VERSION_MINOR + "." + BUILD_ID_PREVIOUS;
    }

    /**
     * Get the complete version number of this database, consisting of 
     * the major version, the minor version, the build id, and the build date.
     * 
     * @return the complete version
     */
    public static String getFullVersion() {
        return getVersion() + " (" + BUILD_DATE + ")";
    }

}
