/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.HashMap;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;

/**
 * This class contains various database-level settings. To override the
 * documented default value for a database, append the setting in the database
 * URL: "jdbc:h2:./test;ANALYZE_SAMPLE=1000" when opening the first connection
 * to the database. The settings can not be changed once the database is open.
 * <p>
 * Some settings are a last resort and temporary solution to work around a
 * problem in the application or database engine. Also, there are system
 * properties to enable features that are not yet fully tested or that are not
 * backward compatible.
 * </p>
 */
public class DbSettings extends SettingsBase {

    /**
     * The initial size of the hash table.
     */
    static final int TABLE_SIZE = 64;

    /**
     * INTERNAL.
     * The default settings. Those must not be modified.
     */
    public static final DbSettings DEFAULT = new DbSettings(new HashMap<>(TABLE_SIZE));

    /**
     * Database setting <code>ANALYZE_AUTO</code> (default: 2000).
     * After changing this many rows, ANALYZE is automatically run for a table.
     * Automatically running ANALYZE is disabled if set to 0. If set to 1000,
     * then ANALYZE will run against each user table after about 1000 changes to
     * that table. The time between running ANALYZE doubles each time since
     * starting the database. It is not run on local temporary tables, and
     * tables that have a trigger on SELECT.
     */
    public final int analyzeAuto = get("ANALYZE_AUTO", 2000);

    /**
     * Database setting <code>ANALYZE_SAMPLE</code> (default: 10000).
     * The default sample size when analyzing a table.
     */
    public final int analyzeSample = get("ANALYZE_SAMPLE", 10_000);

    /**
     * Database setting <code>AUTO_COMPACT_FILL_RATE</code>
     * (default: 90, which means 90%, 0 disables auto-compacting).
     * Set the auto-compact target fill rate. If the average fill rate (the
     * percentage of the storage space that contains active data) of the
     * chunks is lower, then the chunks with a low fill rate are re-written.
     * Also, if the percentage of empty space between chunks is higher than
     * this value, then chunks at the end of the file are moved. Compaction
     * stops if the target fill rate is reached.
     * This setting only affects MVStore engine.
     */
    public final int autoCompactFillRate = get("AUTO_COMPACT_FILL_RATE", 90);

    /**
     * Database setting <code>DATABASE_TO_LOWER</code> (default: false).
     * When set to true unquoted identifiers and short name of database are
     * converted to lower case. Value of this setting should not be changed
     * after creation of database. Setting this to "true" is experimental.
     */
    public final boolean databaseToLower;

    /**
     * Database setting <code>DATABASE_TO_UPPER</code> (default: true).
     * When set to true unquoted identifiers and short name of database are
     * converted to upper case.
     */
    public final boolean databaseToUpper;

    /**
     * Database setting <code>CASE_INSENSITIVE_IDENTIFIERS</code> (default:
     * false).
     * When set to true, all identifier names (table names, column names) are
     * case insensitive. Setting this to "true" is experimental.
     */
    public final boolean caseInsensitiveIdentifiers = get("CASE_INSENSITIVE_IDENTIFIERS", false);

    /**
     * Database setting <code>DEFAULT_CONNECTION</code> (default: false).
     * Whether Java functions can use
     * <code>DriverManager.getConnection("jdbc:default:connection")</code> to
     * get a database connection. This feature is disabled by default for
     * performance reasons. Please note the Oracle JDBC driver will try to
     * resolve this database URL if it is loaded before the H2 driver.
     */
    public final boolean defaultConnection = get("DEFAULT_CONNECTION", false);

    /**
     * Database setting <code>DEFAULT_ESCAPE</code> (default: \).
     * The default escape character for LIKE comparisons. To select no escape
     * character, use an empty string.
     */
    public final String defaultEscape = get("DEFAULT_ESCAPE", "\\");

    /**
     * Database setting <code>DEFRAG_ALWAYS</code> (default: false)
     * Each time the database is closed normally, it is fully defragmented (the
     * same as SHUTDOWN DEFRAG). If you execute SHUTDOWN COMPACT, then this
     * setting is ignored.
     */
    public final boolean defragAlways = get("DEFRAG_ALWAYS", false);

    /**
     * Database setting <code>DROP_RESTRICT</code> (default: true)
     * Whether the default action for DROP TABLE, DROP VIEW, DROP SCHEMA, DROP
     * DOMAIN, and DROP CONSTRAINT is RESTRICT.
     */
    public final boolean dropRestrict = get("DROP_RESTRICT", true);

    /**
     * Database setting <code>ESTIMATED_FUNCTION_TABLE_ROWS</code> (default:
     * 1000).
     * The estimated number of rows in a function table (for example, CSVREAD or
     * FTL_SEARCH). This value is used by the optimizer.
     */
    public final int estimatedFunctionTableRows = get(
            "ESTIMATED_FUNCTION_TABLE_ROWS", 1000);

    /**
     * Database setting <code>LOB_TIMEOUT</code> (default: 300000,
     * which means 5 minutes).
     * The number of milliseconds a temporary LOB reference is kept until it
     * times out. After the timeout, the LOB is no longer accessible using this
     * reference.
     */
    public final int lobTimeout = get("LOB_TIMEOUT", 300_000);

    /**
     * Database setting <code>MAX_COMPACT_TIME</code> (default: 200).
     * The maximum time in milliseconds used to compact a database when closing.
     */
    public final int maxCompactTime = get("MAX_COMPACT_TIME", 200);

    /**
     * Database setting <code>MAX_QUERY_TIMEOUT</code> (default: 0).
     * The maximum timeout of a query in milliseconds. The default is 0, meaning
     * no limit. Please note the actual query timeout may be set to a lower
     * value.
     */
    public final int maxQueryTimeout = get("MAX_QUERY_TIMEOUT", 0);

    /**
     * Database setting <code>OPTIMIZE_DISTINCT</code> (default: true).
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
    public final boolean optimizeDistinct = get("OPTIMIZE_DISTINCT", true);

    /**
     * Database setting <code>OPTIMIZE_EVALUATABLE_SUBQUERIES</code> (default:
     * true).
     * Optimize subqueries that are not dependent on the outer query.
     */
    public final boolean optimizeEvaluatableSubqueries = get(
            "OPTIMIZE_EVALUATABLE_SUBQUERIES", true);

    /**
     * Database setting <code>OPTIMIZE_INSERT_FROM_SELECT</code>
     * (default: true).
     * Insert into table from query directly bypassing temporary disk storage.
     * This also applies to create table as select.
     */
    public final boolean optimizeInsertFromSelect = get(
            "OPTIMIZE_INSERT_FROM_SELECT", true);

    /**
     * Database setting <code>OPTIMIZE_IN_LIST</code> (default: true).
     * Optimize IN(...) and IN(SELECT ...) comparisons. This includes
     * optimization for SELECT, DELETE, and UPDATE.
     */
    public final boolean optimizeInList = get("OPTIMIZE_IN_LIST", true);

    /**
     * Database setting <code>OPTIMIZE_IN_SELECT</code> (default: true).
     * Optimize IN(SELECT ...) comparisons. This includes
     * optimization for SELECT, DELETE, and UPDATE.
     */
    public final boolean optimizeInSelect = get("OPTIMIZE_IN_SELECT", true);

    /**
     * Database setting <code>OPTIMIZE_OR</code> (default: true).
     * Convert (C=? OR C=?) to (C IN(?, ?)).
     */
    public final boolean optimizeOr = get("OPTIMIZE_OR", true);

    /**
     * Database setting <code>OPTIMIZE_TWO_EQUALS</code> (default: true).
     * Optimize expressions of the form A=B AND B=1. In this case, AND A=1 is
     * added so an index on A can be used.
     */
    public final boolean optimizeTwoEquals = get("OPTIMIZE_TWO_EQUALS", true);

    /**
     * Database setting <code>OPTIMIZE_SIMPLE_SINGLE_ROW_SUBQUERIES</code> (default: true).
     * Optimize expressions of the form (SELECT A) to A.
     */
    public final boolean optimizeSimpleSingleRowSubqueries = get("OPTIMIZE_SIMPLE_SINGLE_ROW_SUBQUERIES", true);

    /**
     * Database setting <code>QUERY_CACHE_SIZE</code> (default: 8).
     * The size of the query cache, in number of cached statements. Each session
     * has it's own cache with the given size. The cache is only used if the SQL
     * statement and all parameters match. Only the last returned result per
     * query is cached. The following statement types are cached: SELECT
     * statements are cached (excluding UNION and FOR UPDATE statements), CALL
     * if it returns a single value, DELETE, INSERT, MERGE, UPDATE, and
     * transactional statements such as COMMIT. This works for both statements
     * and prepared statement.
     */
    public final int queryCacheSize = get("QUERY_CACHE_SIZE", 8);

    /**
     * Database setting <code>RECOMPILE_ALWAYS</code> (default: false).
     * Always recompile prepared statements.
     */
    public final boolean recompileAlways = get("RECOMPILE_ALWAYS", false);

    /**
     * Database setting <code>REUSE_SPACE</code> (default: true).
     * If disabled, all changes are appended to the database file, and existing
     * content is never overwritten. This setting has no effect if the database
     * is already open.
     */
    public final boolean reuseSpace = get("REUSE_SPACE", true);

    /**
     * Database setting <code>SHARE_LINKED_CONNECTIONS</code>
     * (default: true).
     * Linked connections should be shared, that means connections to the same
     * database should be used for all linked tables that connect to the same
     * database.
     */
    public final boolean shareLinkedConnections = get(
            "SHARE_LINKED_CONNECTIONS", true);

    /**
     * Database setting <code>DEFAULT_TABLE_ENGINE</code>
     * (default: null).
     * The default table engine to use for new tables.
     */
    public final String defaultTableEngine = get("DEFAULT_TABLE_ENGINE", null);

    /**
     * Database setting <code>MV_STORE</code>
     * (default: true).
     * Use the MVStore storage engine.
     */
    public final boolean mvStore = get("MV_STORE", true);

    /**
     * Database setting <code>COMPRESS</code>
     * (default: false).
     * Compress data when storing.
     */
    public final boolean compressData = get("COMPRESS", false);

    /**
     * Database setting <code>IGNORE_CATALOGS</code>
     * (default: false).
     * If set, all catalog names in identifiers are silently accepted
     * without comparing them with the short name of the database.
     */
    public final boolean ignoreCatalogs = get("IGNORE_CATALOGS", false);

    /**
     * Database setting <code>ZERO_BASED_ENUMS</code>
     * (default: false).
     * If set, ENUM ordinal values are 0-based.
     */
    public final boolean zeroBasedEnums = get("ZERO_BASED_ENUMS", false);

    private DbSettings(HashMap<String, String> s) {
        super(s);
        boolean lower = get("DATABASE_TO_LOWER", false);
        boolean upperSet = containsKey("DATABASE_TO_UPPER");
        boolean upper = get("DATABASE_TO_UPPER", true);
        if (lower && upper) {
            if (upperSet) {
                throw DbException.get(ErrorCode.UNSUPPORTED_SETTING_COMBINATION,
                        "DATABASE_TO_LOWER & DATABASE_TO_UPPER");
            }
            upper = false;
        }
        databaseToLower = lower;
        databaseToUpper = upper;
        HashMap<String, String> settings = getSettings();
        settings.put("DATABASE_TO_LOWER", Boolean.toString(lower));
        settings.put("DATABASE_TO_UPPER", Boolean.toString(upper));
    }

    /**
     * INTERNAL.
     * Get the settings for the given properties (may not be null).
     *
     * @param s the settings
     * @return the settings
     */
    static DbSettings getInstance(HashMap<String, String> s) {
        return new DbSettings(s);
    }

}
