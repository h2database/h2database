package org.h2.engine;

import org.h2.util.StringUtils;

import java.sql.DriverPropertyInfo;

public class DriverPropertyInfoProvider {
    private static final String[] boolChoice = new String[]{"TRUE", "FALSE"};

    private static final DbSettings defaultSettings = DbSettings.DEFAULT;
    private static final DriverPropertyInfo[] DRIVER_PROPERTY_INFOS = {
            newProp("ANALYZE_AUTO", defaultSettings.analyzeAuto,
                    "Database setting ANALYZE_AUTO (default: 2000). \n" +
                            "After changing this many rows, ANALYZE is automatically run for a table. \n" +
                            "Automatically running ANALYZE is disabled if set to 0. \n" +
                            "If set to 1000, then ANALYZE will run against each user " +
                            "table after about 1000 changes to that table. \n" +
                            "The time between running ANALYZE doubles each time since starting the database. \n" +
                            "It is not run on local temporary tables, and tables that have a trigger on SELECT. \n"),
            newProp("ANALYZE_SAMPLE", defaultSettings.analyzeSample,
                    "Database setting ANALYZE_SAMPLE (default: 10000). \n" +
                            "The default sample size when analyzing a table."),
            newProp("AUTO_COMPACT_FILL_RATE", defaultSettings.autoCompactFillRate,
                    "Database setting AUTO_COMPACT_FILL_RATE " +
                            "(default: 90, which means 90%, 0 disables auto-compacting). \n" +
                            "Set the auto-compact target fill rate. \n" +
                            "If the average fill rate " +
                            "(the percentage of the storage space that contains active data) " +
                            "of the chunks is lower, then the chunks with a low fill rate are re-written. \n" +
                            "Also, if the percentage of empty space between chunks is higher than this value, " +
                            "then chunks at the end of the file are moved. \n" +
                            "Compaction stops if the target fill rate is reached. \n" +
                            "This setting only affects MVStore engine."),
            newProp("CASE_INSENSITIVE_IDENTIFIERS", defaultSettings.caseInsensitiveIdentifiers,
                    "Database setting CASE_INSENSITIVE_IDENTIFIERS (default: false). \n" +
                            "When set to true, all identifier names (table names, column names) " +
                            "are case insensitive. \n" +
                            "Setting this to \"true\" is experimental."),
            newProp("COMPRESS", defaultSettings.compressData,
                    "Database setting COMPRESS (default: false). \n" +
                            "Compress data when storing."),
            newProp("DATABASE_TO_LOWER", defaultSettings.databaseToLower,
                    "Database setting DATABASE_TO_LOWER (default: false). \n" +
                            "When set to true unquoted identifiers and short name of database " +
                            "are converted to lower case. \n" +
                            "Value of this setting should not be changed after creation of database. \n" +
                            "Setting this to \"true\" is experimental."),
            newProp("DATABASE_TO_UPPER", defaultSettings.databaseToUpper,
                    "Database setting DATABASE_TO_UPPER (default: true). \n" +
                            "When set to true unquoted identifiers and short name of database " +
                            "are converted to upper case."),
            newProp("DEFAULT_CONNECTION", defaultSettings.defaultConnection,
                    "Database setting DEFAULT_CONNECTION (default: false). \n" +
                            "Whether Java functions can use " +
                            "DriverManager.getConnection(\"jdbc:default:connection\") \n" +
                            "to get a database connection. \n" +
                            "This feature is disabled by default for performance reasons. \n" +
                            "Please note the Oracle JDBC driver will try to resolve this database URL " +
                            "if it is loaded before the H2 driver."),
            newProp("DEFAULT_ESCAPE", defaultSettings.defaultEscape,
                    "Database setting DEFAULT_ESCAPE (default: \\). \n" +
                            "The default escape character for LIKE comparisons. \n" +
                            "To select no escape character, use an empty string."),
//            newProp("DEFAULT_TABLE_ENGINE", null, "Database setting DEFAULT_TABLE_ENGINE " +
//                    "(default: null). \n" +
//                    "The default table engine to use for new tables."),
            newProp("DEFRAG_ALWAYS", defaultSettings.defragAlways,
                    "Database setting DEFRAG_ALWAYS (default: false) \n" +
                            "Each time the database is closed normally, it is fully defragmented " +
                            "(the same as SHUTDOWN DEFRAG). \n" +
                            "If you execute SHUTDOWN COMPACT, then this setting is ignored."),
            newProp("DROP_RESTRICT", defaultSettings.dropRestrict,
                    "Database setting DROP_RESTRICT (default: true) \n" +
                            "Whether the default action for DROP TABLE, DROP VIEW, DROP SCHEMA, DROP DOMAIN, " +
                            "and DROP CONSTRAINT is RESTRICT."),
            newProp("ESTIMATED_FUNCTION_TABLE_ROWS", defaultSettings.estimatedFunctionTableRows,
                    "Database setting ESTIMATED_FUNCTION_TABLE_ROWS (default: 1000). \n" +
                            "The estimated number of rows in a function table " +
                            "(for example, CSVREAD or FTL_SEARCH). \n" +
                            "This value is used by the optimizer."),
            // TODO: Fix default value.
            newProp("IGNORECASE", false,
                    "Database setting IGNORECASE (default: false). \n" +
                            "If set, case-insensitively."),
            newProp("IGNORE_CATALOGS", defaultSettings.ignoreCatalogs,
                    "Database setting IGNORE_CATALOGS (default: false). \n" +
                            "If set, all catalog names in identifiers are silently accepted without comparing them " +
                            "with the short name of the database."),
            newProp("LOB_TIMEOUT", defaultSettings.lobTimeout,
                    "Database setting LOB_TIMEOUT (default: 300000, which means 5 minutes). \n" +
                            "The number of milliseconds a temporary LOB reference is kept until it times out. \n" +
                            "After the timeout, the LOB is no longer accessible using this reference."),
            newProp("MAX_COMPACT_TIME", defaultSettings.maxCompactTime,
                    "Database setting MAX_COMPACT_TIME (default: 200). \n" +
                            "The maximum time in milliseconds used to compact a database when closing."),
            newProp("MAX_QUERY_TIMEOUT", defaultSettings.maxQueryTimeout,
                    "Database setting MAX_QUERY_TIMEOUT (default: 0). \n" +
                            "The maximum timeout of a query in milliseconds. \n" +
                            "The default is 0, meaning no limit. \n" +
                            "Please note the actual query timeout may be set to a lower value."),
            newProp("MODE", null,
                    "Compatibility modes for IBM DB2, Apache Derby, HSQLDB, MS SQL Server, MySQL, Oracle," +
                            " and PostgreSQL.",
                    null, "REGULAR", "STRICT", "LEGACY",
                    "DB2", "Derby", "MariaDB", "MSSQLServer", "HSQLDB", "MySQL", "Oracle", "PostgreSQL"),
//            newProp("MV_STORE", defaultSettings.mvStore,
//                    "Database setting MV_STORE (default: true). \n" +
//                    "Use the MVStore storage engine."),
//            newProp("OPTIMIZE_DISTINCT", true, "Database setting OPTIMIZE_DISTINCT (default: true). \nImprove the performance of simple DISTINCT queries if an index is available for the given column. \nThe optimization is used if: <ul> <li>The select is a single column query without condition </li> <li>The query contains only one table, and no group by </li> <li>There is only one table involved </li> <li>There is an ascending index on the column </li> <li>The selectivity of the column is below 20 </li> </ul>"),
//            newProp("OPTIMIZE_EVALUATABLE_SUBQUERIES", true, "Database setting OPTIMIZE_EVALUATABLE_SUBQUERIES (default: true). \nOptimize subqueries that are not dependent on the outer query."),
//            newProp("OPTIMIZE_INSERT_FROM_SELECT", true, "Database setting OPTIMIZE_INSERT_FROM_SELECT (default: true). \nInsert into table from query directly bypassing temporary disk storage. \nThis also applies to create table as select."),
//            newProp("OPTIMIZE_IN_LIST", true, "Database setting OPTIMIZE_IN_LIST (default: true). \nOptimize IN(...) and IN(SELECT ...) comparisons. \nThis includes optimization for SELECT, DELETE, and UPDATE."),
//            newProp("OPTIMIZE_IN_SELECT", true, "Database setting OPTIMIZE_IN_SELECT (default: true). \nOptimize IN(SELECT ...) comparisons. \nThis includes optimization for SELECT, DELETE, and UPDATE."),
//            newProp("OPTIMIZE_OR", true, "Database setting OPTIMIZE_OR (default: true). \nConvert (C=? OR C=?) to (C IN(?, ?))."),
//            newProp("OPTIMIZE_SIMPLE_SINGLE_ROW_SUBQUERIES", true, "Database setting OPTIMIZE_SIMPLE_SINGLE_ROW_SUBQUERIES (default: true). \nOptimize expressions of the form (SELECT A) to A."),
//            newProp("OPTIMIZE_TWO_EQUALS", true, "Database setting OPTIMIZE_TWO_EQUALS (default: true). \nOptimize expressions of the form A=B AND B=1. \nIn this case, AND A=1 is added so an index on A can be used."),
            newProp("QUERY_CACHE_SIZE", defaultSettings.queryCacheSize,
                    "Database setting QUERY_CACHE_SIZE (default: 8). \n" +
                            "The size of the query cache, in number of cached statements. \n" +
                            "Each session has it's own cache with the given size. \n" +
                            "The cache is only used if the SQL statement and all parameters match. \n" +
                            "Only the last returned result per query is cached. \n" +
                            "The following statement types are cached: SELECT statements are cached " +
                            "(excluding UNION and FOR UPDATE statements), CALL if it returns a single value, " +
                            "DELETE, INSERT, MERGE, UPDATE, and transactional statements such as COMMIT. \n" +
                            "This works for both statements and prepared statement."),
            newProp("RECOMPILE_ALWAYS", defaultSettings.recompileAlways,
                    "Database setting RECOMPILE_ALWAYS (default: false). \n" +
                            "Always recompile prepared statements."),
            newProp("REUSE_SPACE", defaultSettings.reuseSpace,
                    "Database setting REUSE_SPACE (default: true). \n" +
                            "If disabled, all changes are appended to the database file, " +
                            "and existing content is never overwritten. \n" +
                            "This setting has no effect if the database is already open."),
            newProp("SHARE_LINKED_CONNECTIONS", defaultSettings.shareLinkedConnections,
                    "Database setting SHARE_LINKED_CONNECTIONS (default: true). \n" +
                            "Linked connections should be shared, that means connections to the same database " +
                            "should be used for all linked tables that connect to the same database."),
//            newProp("ZERO_BASED_ENUMS", defaultSettings.zeroBasedEnums,
//                    "Database setting ZERO_BASED_ENUMS (default: false). \n" +
//                            "If set, ENUM ordinal values are 0-based."),
            // TODO: 2023/12/1 Add missing DriverPropertyInfo
    };

    private DriverPropertyInfoProvider() {
    }

    private static DriverPropertyInfo newProp(
            final String name,
            final String value,
            final String desc,
            final String... choice) {

        final DriverPropertyInfo info = new DriverPropertyInfo(name, value);
        info.description = desc;
        info.choices = choice;
        return info;
    }

    private static DriverPropertyInfo newProp(
            final String name,
            final boolean value,
            final String desc) {
        final DriverPropertyInfo info = newProp(name, StringUtils.toUpperEnglish(String.valueOf(value)), desc);
        info.choices = boolChoice;
        return info;
    }

    private static DriverPropertyInfo newProp(
            final String name,
            final int value,
            final String desc) {
        return newProp(name, String.valueOf(value), desc);
    }

    public static DriverPropertyInfo[] get() {
        return DRIVER_PROPERTY_INFOS;
    }
}
