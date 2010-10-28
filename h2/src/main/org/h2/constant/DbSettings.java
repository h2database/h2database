/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.constant;

import java.util.HashMap;
import org.h2.engine.SettingsBase;

/**
 * This class contains various database-level settings. To override the
 * documented default value for a database, append the setting in the database
 * URL: "jdbc:h2:test;ALIAS_COLUMN_NAME=TRUE" when opening the first connection to
 * the database. The settings can not be changed once the database is open.
 * <p>
 * Some settings are a last resort and temporary solution to work around a
 * problem in the application or database engine. Also, there are system
 * properties to enable features that are not yet fully tested or that are not
 * backward compatible.
 * </p>
 */
public class DbSettings extends SettingsBase {

    private static DbSettings defaultSettings;

    /**
     * Database setting <code>ALIAS_COLUMN_NAME</code> (default: false).<br />
     * When enabled, aliased columns (as in SELECT ID AS I FROM TEST) return the
     * alias (I in this case) in ResultSetMetaData.getColumnName() and 'null' in
     * getTableName(). If disabled, the real column name (ID in this case) and
     * table name is returned.
     * <br />
     * This setting only affects the default and the MySQL mode. When using
     * any other mode, this feature is enabled for compatibility, even if this
     * database setting is not enabled explicitly.
     */
    public final boolean aliasColumnName = get("ALIAS_COLUMN_NAME", false);

    /**
     * Database setting <code>ANALYZE_AUTO</code> (default: 0).<br />
     * After changing this many rows, ANALYZE is automatically run for a table.
     * Automatically running ANALYZE is disabled if set to 0. If set to 1000,
     * then ANALYZE will run against each user table after about 1000 changes to
     * that table. The time between running ANALYZE doubles each time since
     * starting the database. It is not run on local temporary tables, and
     * tables that have a trigger on SELECT.
     */
    public final int analyzeAuto = get("ANALYZE_AUTO", 0);

    /**
     * Database setting <code>ANALYZE_SAMPLE</code> (default: 10000).<br />
     * The default sample size when analyzing a table.
     */
    public final int analyzeSample = get("ANALYZE_SAMPLE", 10000);

    /**
     * Database setting <code>databaseToUpper</code> (default: true).<br />
     * Database short names are converted to uppercase for the DATABASE()
     * function, and in the CATALOG column of all database meta data methods.
     * Setting this to "false" is experimental.
     */
    public final boolean databaseToUpper = get("DATABASE_TO_UPPER", true);

    /**
     * Database setting <code>DEFAULT_ESCAPE</code> (default: \).<br />
     * The default escape character for LIKE comparisons. To select no escape
     * character, use an empty string.
     */
    public final String defaultEscape = get("DEFAULT_ESCAPE", "\\");

    /**
     * Database setting <code>DEFRAG_ALWAYS</code> (default: false).<br />
     * Each time the database is closed, it is fully defragmented (SHUTDOWN DEFRAG).
     */
    public final boolean defragAlways = get("DEFRAG_ALWAYS", false);

    /**
     * Database setting <code>DROP_RESTRICT</code> (default: false).<br />
     * Whether the default action for DROP TABLE and DROP VIEW is RESTRICT. For
     * most databases, the default action is RESTRICT, but for compatibility
     * with older versions of H2 the default action is currently CASCADE. This will
     * change in a future version of H2.
     */
    public final boolean dropRestrict = get("DROP_RESTRICT", false);

    /**
     * Database setting <code>ESTIMATED_FUNCTION_TABLE_ROWS</code> (default:
     * 1000).<br />
     * The estimated number of rows in a function table (for example, CSVREAD or
     * FTL_SEARCH). This value is used by the optimizer.
     */
    public final int estimatedFunctionTableRows = get("ESTIMATED_FUNCTION_TABLE_ROWS", 1000);

    /**
     * Database setting <code>FUNCTIONS_IN_SCHEMA</code> (default:
     * false).<br />
     * If set, all functions are stored in a schema. Specially, the SCRIPT statement
     * will always include the schema name in the CREATE ALIAS statement.
     * This is not backward compatible with H2 versions 1.2.134 and older.
     */
    public final boolean functionsInSchema = get("FUNCTIONS_IN_SCHEMA", false);

    /**
     * System property <code>h2.largeResultBufferSize</code> (default: 4096).<br />
     * Buffer size for large result sets. Set this value to 0 to disable the
     * buffer.
     */
    public final int largeResultBufferSize = get("LARGE_RESULT_BUFFER_SIZE", 4 * 1024);

    /**
     * System property <code>h2.largeTransactions</code> (default: false).<br />
     * Support very large transactions
     */
    public final boolean largeTransactions = get("LARGE_TRANSACTIONS", false);

    /**
     * Database setting <code>QUERY_CACHE_SIZE</code> (default: 0).<br />
     * The size of the query cache. Each session has it's own cache with the
     * given size. The cache is only used if the SQL statement and all
     * parameters match. Only the last returned result per query is cached. Only
     * SELECT statements are cached (excluding UNION and FOR UPDATE statements).
     * This works for both statements and prepared statement.
     */
    public final int queryCacheSize = get("QUERY_CACHE_SIZE", 0);

    /**
     * Database setting <code>MAX_QUERY_TIMEOUT</code> (default: 0).<br />
     * The maximum timeout of a query in milliseconds. The default is 0, meaning
     * no limit. Please note the actual query timeout may be set to a lower
     * value.
     */
    public int maxQueryTimeout = get("MAX_QUERY_TIMEOUT", 0);

    private DbSettings(HashMap<String, String> s) {
        super(s);
    }

    /**
     * INTERNAL.
     * Get the settings for the given properties (may be null).
     *
     * @param p the properties
     * @return the settings
     */
    public static DbSettings getInstance(HashMap<String, String> s) {
        if (s == null || s.isEmpty()) {
            if (defaultSettings == null) {
                defaultSettings = new DbSettings(new HashMap<String, String>());
            }
            return defaultSettings;
        }
        return new DbSettings(s);
    }

}
