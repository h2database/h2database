/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.regex.Pattern;

import org.h2.util.New;
import org.h2.util.StringUtils;

/**
 * The compatibility modes. There is a fixed set of modes (for example
 * PostgreSQL, MySQL). Each mode has different settings.
 */
public class Mode {

    /**
     * The name of the default mode.
     */
    static final String REGULAR = "REGULAR";

    private static final HashMap<String, Mode> MODES = New.hashMap();

    // Modes are also documented in the features section

    /**
     * When enabled, aliased columns (as in SELECT ID AS I FROM TEST) return the
     * alias (I in this case) in ResultSetMetaData.getColumnName() and 'null' in
     * getTableName(). If disabled, the real column name (ID in this case) and
     * table name is returned.
     */
    public boolean aliasColumnName;

    /**
     * When inserting data, if a column is defined to be NOT NULL and NULL is
     * inserted, then a 0 (or empty string, or the current timestamp for
     * timestamp columns) value is used. Usually, this operation is not allowed
     * and an exception is thrown.
     */
    public boolean convertInsertNullToZero;

    /**
     * When converting the scale of decimal data, the number is only converted
     * if the new scale is smaller than the current scale. Usually, the scale is
     * converted and 0s are added if required.
     */
    public boolean convertOnlyToSmallerScale;

    /**
     * Creating indexes in the CREATE TABLE statement is allowed using
     * <code>INDEX(..)</code> or <code>KEY(..)</code>.
     * Example: <code>create table test(id int primary key, name varchar(255),
     * key idx_name(name));</code>
     */
    public boolean indexDefinitionInCreateTable;

    /**
     * Meta data calls return identifiers in lower case.
     */
    public boolean lowerCaseIdentifiers;

    /**
     * Concatenation with NULL results in NULL. Usually, NULL is treated as an
     * empty string if only one of the operands is NULL, and NULL is only
     * returned if both operands are NULL.
     */
    public boolean nullConcatIsNull;

    /**
     * Identifiers may be quoted using square brackets as in [Test].
     */
    public boolean squareBracketQuotedNames;

    /**
     * Support for the syntax
     * [OFFSET .. ROW|ROWS] [FETCH FIRST .. ROW|ROWS ONLY]
     * as an alternative for LIMIT .. OFFSET.
     */
    public boolean supportOffsetFetch = Constants.VERSION_MINOR >= 4 ? true : false;

    /**
     * The system columns 'CTID' and 'OID' are supported.
     */
    public boolean systemColumns;

    /**
     * For unique indexes, NULL is distinct. That means only one row with NULL
     * in one of the columns is allowed.
     */
    public boolean uniqueIndexSingleNull;

    /**
     * When using unique indexes, multiple rows with NULL in all columns
     * are allowed, however it is not allowed to have multiple rows with the
     * same values otherwise.
     */
    public boolean uniqueIndexSingleNullExceptAllColumnsAreNull;

    /**
     * Empty strings are treated like NULL values. Useful for Oracle emulation.
     */
    public boolean treatEmptyStringsAsNull;

    /**
     * Support the pseudo-table SYSIBM.SYSDUMMY1.
     */
    public boolean sysDummy1;

    /**
     * Text can be concatenated using '+'.
     */
    public boolean allowPlusForStringConcat;

    /**
     * The function LOG() uses base 10 instead of E.
     */
    public boolean logIsLogBase10;

    /**
     * SERIAL and BIGSERIAL columns are not automatically primary keys.
     */
    public boolean serialColumnIsNotPK;

    /**
     * Swap the parameters of the CONVERT function.
     */
    public boolean swapConvertFunctionParameters;

    /**
     * can set the isolation level using WITH {RR|RS|CS|UR}
     */
    public boolean isolationLevelInSelectOrInsertStatement;

    /**
     * MySQL style INSERT ... ON DUPLICATE KEY UPDATE ...
     */
    public boolean onDuplicateKeyUpdate;

    /**
     * Pattern describing the keys the java.sql.Connection.setClientInfo()
     * method accepts.
     */
    public Pattern supportedClientInfoPropertiesRegEx;

    /**
     * Support the # for column names
     */
    public boolean supportPoundSymbolForColumnNames;

    /**
     * Whether an empty list as in "NAME IN()" results in a syntax error.
     */
    public boolean prohibitEmptyInPredicate;

    /**
     * Whether AFFINITY KEY keywords are supported.
     */
    public boolean allowAffinityKey;

    /**
     * Whether to right-pad fixed strings with spaces.
     */
    public boolean padFixedLengthStrings;

    /**
     * Whether DB2 TIMESTAMP formats are allowed.
     */
    public boolean allowDB2TimestampFormat;

    /**
     * An optional Set of hidden/disallowed column types.
     * Certain DBMSs don't support all column types provided by H2, such as
     * "NUMBER" when using PostgreSQL mode.
     */
    public Set<String> disallowedTypes = Collections.emptySet();

    private final String name;

    static {
        Mode mode = new Mode(REGULAR);
        mode.nullConcatIsNull = true;
        add(mode);

        mode = new Mode("DB2");
        mode.aliasColumnName = true;
        mode.supportOffsetFetch = true;
        mode.sysDummy1 = true;
        mode.isolationLevelInSelectOrInsertStatement = true;
        // See
        // https://www.ibm.com/support/knowledgecenter/SSEPEK_11.0.0/
        //     com.ibm.db2z11.doc.java/src/tpc/imjcc_r0052001.dita
        mode.supportedClientInfoPropertiesRegEx =
                Pattern.compile("ApplicationName|ClientAccountingInformation|" +
                        "ClientUser|ClientCorrelationToken");
        mode.prohibitEmptyInPredicate = true;
        mode.allowDB2TimestampFormat = true;
        add(mode);

        mode = new Mode("Derby");
        mode.aliasColumnName = true;
        mode.uniqueIndexSingleNull = true;
        mode.supportOffsetFetch = true;
        mode.sysDummy1 = true;
        mode.isolationLevelInSelectOrInsertStatement = true;
        // Derby does not support client info properties as of version 10.12.1.1
        mode.supportedClientInfoPropertiesRegEx = null;
        add(mode);

        mode = new Mode("HSQLDB");
        mode.aliasColumnName = true;
        mode.convertOnlyToSmallerScale = true;
        mode.nullConcatIsNull = true;
        mode.uniqueIndexSingleNull = true;
        mode.allowPlusForStringConcat = true;
        // HSQLDB does not support client info properties. See
        // http://hsqldb.org/doc/apidocs/
        //     org/hsqldb/jdbc/JDBCConnection.html#
        //     setClientInfo%28java.lang.String,%20java.lang.String%29
        mode.supportedClientInfoPropertiesRegEx = null;
        add(mode);

        mode = new Mode("MSSQLServer");
        mode.aliasColumnName = true;
        mode.squareBracketQuotedNames = true;
        mode.uniqueIndexSingleNull = true;
        mode.allowPlusForStringConcat = true;
        mode.swapConvertFunctionParameters = true;
        mode.supportPoundSymbolForColumnNames = true;
        // MS SQL Server does not support client info properties. See
        // https://msdn.microsoft.com/en-Us/library/dd571296%28v=sql.110%29.aspx
        mode.supportedClientInfoPropertiesRegEx = null;
        add(mode);

        mode = new Mode("MySQL");
        mode.convertInsertNullToZero = true;
        mode.indexDefinitionInCreateTable = true;
        mode.lowerCaseIdentifiers = true;
        mode.onDuplicateKeyUpdate = true;
        // MySQL allows to use any key for client info entries. See
        // http://grepcode.com/file/repo1.maven.org/maven2/mysql/
        //     mysql-connector-java/5.1.24/com/mysql/jdbc/
        //     JDBC4CommentClientInfoProvider.java
        mode.supportedClientInfoPropertiesRegEx =
                Pattern.compile(".*");
        mode.prohibitEmptyInPredicate = true;
        add(mode);

        mode = new Mode("Oracle");
        mode.aliasColumnName = true;
        mode.convertOnlyToSmallerScale = true;
        mode.uniqueIndexSingleNullExceptAllColumnsAreNull = true;
        mode.treatEmptyStringsAsNull = true;
        mode.supportPoundSymbolForColumnNames = true;
        // Oracle accepts keys of the form <namespace>.*. See
        // https://docs.oracle.com/database/121/JJDBC/jdbcvers.htm#JJDBC29006
        mode.supportedClientInfoPropertiesRegEx =
                Pattern.compile(".*\\..*");
        mode.prohibitEmptyInPredicate = true;
        add(mode);

        mode = new Mode("PostgreSQL");
        mode.aliasColumnName = true;
        mode.nullConcatIsNull = true;
        mode.supportOffsetFetch = true;
        mode.systemColumns = true;
        mode.logIsLogBase10 = true;
        mode.serialColumnIsNotPK = true;
        // PostgreSQL only supports the ApplicationName property. See
        // https://github.com/hhru/postgres-jdbc/blob/master/postgresql-jdbc-9.2-1002.src/
        //     org/postgresql/jdbc4/AbstractJdbc4Connection.java
        mode.supportedClientInfoPropertiesRegEx =
                Pattern.compile("ApplicationName");
        mode.prohibitEmptyInPredicate = true;
        mode.padFixedLengthStrings = true;
        // Enumerate all H2 types NOT supported by PostgreSQL:
        Set<String> disallowedTypes = new java.util.HashSet<String>();
        disallowedTypes.add("NUMBER");
        disallowedTypes.add("IDENTITY");
        disallowedTypes.add("TINYINT");
        mode.disallowedTypes = disallowedTypes;
        add(mode);

        mode = new Mode("Ignite");
        mode.nullConcatIsNull = true;
        mode.allowAffinityKey = true;
        mode.indexDefinitionInCreateTable = true;
        add(mode);
    }

    private Mode(String name) {
        this.name = name;
    }

    private static void add(Mode mode) {
        MODES.put(StringUtils.toUpperEnglish(mode.name), mode);
    }

    /**
     * Get the mode with the given name.
     *
     * @param name the name of the mode
     * @return the mode object
     */
    public static Mode getInstance(String name) {
        return MODES.get(StringUtils.toUpperEnglish(name));
    }

    public String getName() {
        return name;
    }

}
