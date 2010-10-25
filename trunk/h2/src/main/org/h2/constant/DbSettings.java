/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.constant;

import java.util.Properties;
import org.h2.engine.SettingsBase;

/**
 * This class contains various database-level settings. To override the
 * documented default value for a database, append the setting in the database
 * URL: "jdbc:h2:test;analyzeSample=100" when opening the first connection to
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
     * Database setting <code>analyzeAuto</code> (default: 0).<br />
     * After changing this many rows, ANALYZE is automatically run for a table.
     * Automatically running ANALYZE is disabled if set to 0. If set to 1000,
     * then ANALYZE will run against each user table after about 1000 changes to
     * that table. The time between running ANALYZE doubles each time since
     * starting the database. It is not run on local temporary tables, and
     * tables that have a trigger on SELECT.
     */
    public int analyzeAuto = get("analyzeAuto", 0);

    /**
     * Database setting <code>analyzeSample</code> (default: 10000).<br />
     * The default sample size when analyzing a table.
     */
    public int analyzeSample = get("analyzeSample", 10000);

    /**
     * Database setting <code>databaseToUpper</code> (default: true).<br />
     * Database short names are converted to uppercase for the DATABASE()
     * function, and in the CATALOG column of all database meta data methods.
     * Setting this to "false" is experimental.
     */
    public boolean databaseToUpper = get("databaseToUpper", true);

    /**
     * Database setting <code>defaultEscape</code> (default: \).<br />
     * The default escape character for LIKE comparisons. To select no escape
     * character, use an empty string.
     */
    public String defaultEscape = get("defaultEscape", "\\");

    /**
     * Database setting <code>defragAlways</code> (default: false).<br />
     * Each time the database is closed, it is fully defragmented (SHUTDOWN DEFRAG).
     */
    public boolean defragAlways = get("defragAlways", false);

    /**
     * Database setting <code>dropRestrict</code> (default: false).<br />
     * Whether the default action for DROP TABLE and DROP VIEW is RESTRICT. For
     * most databases, the default action is RESTRICT, but for compatibility
     * with older versions of H2 the default action is currently CASCADE. This will
     * change in a future version of H2.
     */
    public boolean dropRestrict = get("dropRestrict", false);

    /**
     * Database setting <code>estimatedFunctionTableRows</code> (default:
     * 1000).<br />
     * The estimated number of rows in a function table (for example, CSVREAD or
     * FTL_SEARCH). This value is used by the optimizer.
     */
    public int estimatedFunctionTableRows = get("estimatedFunctionTableRows", 1000);

    /**
     * Database setting <code>functionsInSchema</code> (default:
     * false).<br />
     * If set, all functions are stored in a schema. Specially, the SCRIPT statement
     * will always include the schema name in the CREATE ALIAS statement.
     * This is not backward compatible with H2 versions 1.2.134 and older.
     */
    public boolean functionsInSchema = get("functionsInSchema", false);

    /**
     * Database setting <code>queryCacheSize</code> (default: 0).<br />
     * The size of the query cache. Each session has it's own cache with the
     * given size. The cache is only used if the SQL statement and all
     * parameters match. Only the last returned result per query is cached. Only
     * SELECT statements are cached (excluding UNION and FOR UPDATE statements).
     * This works for both statements and prepared statement.
     */
    public int queryCacheSize = get("queryCacheSize", 0);

    private DbSettings(Properties p) {
        super(p);
    }

    /**
     * INTERNAL.
     * Get the settings for the given properties (may be null).
     *
     * @param p the properties
     * @return the settings
     */
    public static DbSettings getInstance(Properties p) {
        if (p == null || p.isEmpty()) {
            if (defaultSettings == null) {
                defaultSettings = new DbSettings(new Properties());
            }
            return defaultSettings;
        }
        return new DbSettings(p);
    }

}
