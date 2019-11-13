/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.regex.Pattern;

import org.h2.util.StringUtils;
import org.h2.value.DataType;
import org.h2.value.Value;

/**
 * The compatibility modes. There is a fixed set of modes (for example
 * PostgreSQL, MySQL). Each mode has different settings.
 */
public class Mode {

    public enum ModeEnum {
        REGULAR, DB2, Derby, MSSQLServer, HSQLDB, MySQL, Oracle, PostgreSQL
    }

    /**
     * Determines how rows with {@code NULL} values in indexed columns are handled
     * in unique indexes.
     */
    public enum UniqueIndexNullsHandling {
        /**
         * Multiple rows with identical values in indexed columns with at least one
         * indexed {@code NULL} value are allowed in unique index.
         */
        ALLOW_DUPLICATES_WITH_ANY_NULL,

        /**
         * Multiple rows with identical values in indexed columns with all indexed
         * {@code NULL} values are allowed in unique index.
         */
        ALLOW_DUPLICATES_WITH_ALL_NULLS,

        /**
         * Multiple rows with identical values in indexed columns are not allowed in
         * unique index.
         */
        FORBID_ANY_DUPLICATES
    }

    private static final HashMap<String, Mode> MODES = new HashMap<>();

    // Modes are also documented in the features section

    /**
     * When enabled, aliased columns (as in SELECT ID AS I FROM TEST) return the
     * alias (I in this case) in ResultSetMetaData.getColumnName() and 'null' in
     * getTableName(). If disabled, the real column name (ID in this case) and
     * table name is returned.
     */
    public boolean aliasColumnName;

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
     * The system columns 'CTID' and 'OID' are supported.
     */
    public boolean systemColumns;

    /**
     * Determines how rows with {@code NULL} values in indexed columns are handled
     * in unique indexes.
     */
    public UniqueIndexNullsHandling uniqueIndexNullsHandling = UniqueIndexNullsHandling.ALLOW_DUPLICATES_WITH_ANY_NULL;

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
     * The single-argument function LOG() uses base 10 instead of E.
     */
    public boolean logIsLogBase10;

    /**
     * Swap the parameters of LOG() function.
     */
    public boolean swapLogFunctionParameters;

    /**
     * The function REGEXP_REPLACE() uses \ for back-references.
     */
    public boolean regexpReplaceBackslashReferences;

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
     * MySQL style INSERT ... ON DUPLICATE KEY UPDATE ... and INSERT IGNORE.
     */
    public boolean onDuplicateKeyUpdate;

    /**
     * MySQL style REPLACE INTO.
     */
    public boolean replaceInto;

    /**
     * PostgreSQL style INSERT ... ON CONFLICT DO NOTHING.
     */
    public boolean insertOnConflict;

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
     * Whether IN predicate may have an empty value list.
     */
    public boolean allowEmptyInPredicate;

    /**
     * Whether to right-pad fixed strings with spaces.
     */
    public boolean padFixedLengthStrings;

    /**
     * Whether DB2 TIMESTAMP formats are allowed.
     */
    public boolean allowDB2TimestampFormat;

    /**
     * Discard SQLServer table hints (e.g. "SELECT * FROM table WITH (NOLOCK)")
     */
    public boolean discardWithTableHints;

    /**
     * Use "IDENTITY" as an alias for "auto_increment" (SQLServer style)
     */
    public boolean useIdentityAsAutoIncrement;

    /**
     * Convert (VAR)CHAR to VAR(BINARY) and vice versa with UTF-8 encoding instead of HEX.
     */
    public boolean charToBinaryInUtf8;

    /**
     * If {@code true}, datetime value function return the same value within a
     * transaction, if {@code false} datetime value functions return the same
     * value within a command.
     */
    public boolean dateTimeValueWithinTransaction;

    /**
     * If {@code true} {@code 0x}-prefixed numbers are parsed as binary string
     * literals, if {@code false} they are parsed as hexadecimal numeric values.
     */
    public boolean zeroExLiteralsAreBinaryStrings;

    /**
     * If {@code true} unrelated ORDER BY expression are allowed in DISTINCT
     * queries, if {@code false} they are disallowed.
     */
    public boolean allowUnrelatedOrderByExpressionsInDistinctQueries;

    /**
     * If {@code true} some additional non-standard ALTER TABLE commands are allowed.
     */
    public boolean alterTableExtensionsMySQL;

    /**
     * If {@code true} non-standard ALTER TABLE MODIFY COLUMN is allowed.
     */
    public boolean alterTableModifyColumn;

    /**
     * If {@code true} TRUNCATE TABLE uses RESTART IDENTITY by default.
     */
    public boolean truncateTableRestartIdentity;

    /**
     * If {@code true} NEXT VALUE FOR SEQUENCE, CURRENT VALUE FOR SEQUENCE,
     * SEQUENCE.NEXTVAL, and SEQUENCE.CURRVAL return values with DECIMAL/NUMERIC
     * data type instead of BIGINT.
     */
    public boolean decimalSequences;

    /**
     * If {@code true} constructs like 'CREATE TABLE CATALOG..TABLE_NAME' are allowed,
     * the default schema is used.
     */
    public boolean allowEmptySchemaValuesAsDefaultSchema;

    /**
     * If {@code true} all numeric data types may have precision and 'UNSIGNED'
     * clause.
     */
    public boolean allNumericTypesHavePrecision;

    /**
     * If {@code true} 'FOR BIT DATA' clauses are allowed for character string
     * data types.
     */
    public boolean forBitData;

    /**
     * An optional Set of hidden/disallowed column types.
     * Certain DBMSs don't support all column types provided by H2, such as
     * "NUMBER" when using PostgreSQL mode.
     */
    public Set<String> disallowedTypes = Collections.emptySet();

    /**
     * Custom mappings from type names to data types.
     */
    public HashMap<String, DataType> typeByNameMap = new HashMap<>();

    private final String name;

    private final ModeEnum modeEnum;

    static {
        Mode mode = new Mode(ModeEnum.REGULAR);
        mode.nullConcatIsNull = true;
        mode.allowEmptyInPredicate = true;
        mode.dateTimeValueWithinTransaction = true;
        add(mode);

        mode = new Mode(ModeEnum.DB2);
        mode.aliasColumnName = true;
        mode.sysDummy1 = true;
        mode.isolationLevelInSelectOrInsertStatement = true;
        // See
        // https://www.ibm.com/support/knowledgecenter/SSEPEK_11.0.0/
        //     com.ibm.db2z11.doc.java/src/tpc/imjcc_r0052001.dita
        mode.supportedClientInfoPropertiesRegEx =
                Pattern.compile("ApplicationName|ClientAccountingInformation|" +
                        "ClientUser|ClientCorrelationToken");
        mode.allowDB2TimestampFormat = true;
        mode.forBitData = true;
        add(mode);

        mode = new Mode(ModeEnum.Derby);
        mode.aliasColumnName = true;
        mode.uniqueIndexNullsHandling = UniqueIndexNullsHandling.FORBID_ANY_DUPLICATES;
        mode.sysDummy1 = true;
        mode.isolationLevelInSelectOrInsertStatement = true;
        // Derby does not support client info properties as of version 10.12.1.1
        mode.supportedClientInfoPropertiesRegEx = null;
        mode.forBitData = true;
        add(mode);

        mode = new Mode(ModeEnum.HSQLDB);
        mode.nullConcatIsNull = true;
        mode.allowPlusForStringConcat = true;
        // HSQLDB does not support client info properties. See
        // http://hsqldb.org/doc/apidocs/org/hsqldb/jdbc/JDBCConnection.html#setClientInfo-java.lang.String-java.lang.String-
        mode.supportedClientInfoPropertiesRegEx = null;
        add(mode);

        mode = new Mode(ModeEnum.MSSQLServer);
        mode.aliasColumnName = true;
        mode.squareBracketQuotedNames = true;
        mode.uniqueIndexNullsHandling = UniqueIndexNullsHandling.FORBID_ANY_DUPLICATES;
        mode.allowPlusForStringConcat = true;
        mode.swapLogFunctionParameters = true;
        mode.swapConvertFunctionParameters = true;
        mode.supportPoundSymbolForColumnNames = true;
        mode.discardWithTableHints = true;
        mode.useIdentityAsAutoIncrement = true;
        // MS SQL Server does not support client info properties. See
        // https://msdn.microsoft.com/en-Us/library/dd571296%28v=sql.110%29.aspx
        mode.supportedClientInfoPropertiesRegEx = null;
        mode.zeroExLiteralsAreBinaryStrings = true;
        mode.truncateTableRestartIdentity = true;
        DataType dt = DataType.createNumeric(19, 4, false);
        dt.type = Value.DECIMAL;
        dt.sqlType = Types.NUMERIC;
        dt.name = "MONEY";
        mode.typeByNameMap.put("MONEY", dt);
        dt = DataType.createNumeric(10, 4, false);
        dt.type = Value.DECIMAL;
        dt.sqlType = Types.NUMERIC;
        dt.name = "SMALLMONEY";
        mode.typeByNameMap.put("SMALLMONEY", dt);
        mode.allowEmptySchemaValuesAsDefaultSchema = true;
        add(mode);

        mode = new Mode(ModeEnum.MySQL);
        mode.indexDefinitionInCreateTable = true;
        // Next one is for MariaDB
        mode.regexpReplaceBackslashReferences = true;
        mode.onDuplicateKeyUpdate = true;
        mode.replaceInto = true;
        // MySQL allows to use any key for client info entries. See
        // https://github.com/mysql/mysql-connector-j/blob/5.1.47/src/com/mysql/jdbc/JDBC4CommentClientInfoProvider.java
        mode.supportedClientInfoPropertiesRegEx =
                Pattern.compile(".*");
        mode.charToBinaryInUtf8 = true;
        mode.zeroExLiteralsAreBinaryStrings = true;
        mode.allowUnrelatedOrderByExpressionsInDistinctQueries = true;
        mode.alterTableExtensionsMySQL = true;
        mode.alterTableModifyColumn = true;
        mode.truncateTableRestartIdentity = true;
        mode.allNumericTypesHavePrecision = true;
        add(mode);

        mode = new Mode(ModeEnum.Oracle);
        mode.aliasColumnName = true;
        mode.convertOnlyToSmallerScale = true;
        mode.uniqueIndexNullsHandling = UniqueIndexNullsHandling.ALLOW_DUPLICATES_WITH_ALL_NULLS;
        mode.treatEmptyStringsAsNull = true;
        mode.regexpReplaceBackslashReferences = true;
        mode.supportPoundSymbolForColumnNames = true;
        // Oracle accepts keys of the form <namespace>.*. See
        // https://docs.oracle.com/database/121/JJDBC/jdbcvers.htm#JJDBC29006
        mode.supportedClientInfoPropertiesRegEx =
                Pattern.compile(".*\\..*");
        mode.alterTableModifyColumn = true;
        mode.decimalSequences = true;
        dt = DataType.createDate(/* 2001-01-01 23:59:59 */ 19, 19, "DATE", false, 0, 0);
        dt.type = Value.TIMESTAMP;
        dt.sqlType = Types.TIMESTAMP;
        dt.name = "DATE";
        mode.typeByNameMap.put("DATE", dt);
        add(mode);

        mode = new Mode(ModeEnum.PostgreSQL);
        mode.aliasColumnName = true;
        mode.nullConcatIsNull = true;
        mode.systemColumns = true;
        mode.logIsLogBase10 = true;
        mode.regexpReplaceBackslashReferences = true;
        mode.serialColumnIsNotPK = true;
        mode.insertOnConflict = true;
        // PostgreSQL only supports the ApplicationName property. See
        // https://github.com/hhru/postgres-jdbc/blob/master/postgresql-jdbc-9.2-1002.src/
        //     org/postgresql/jdbc4/AbstractJdbc4Connection.java
        mode.supportedClientInfoPropertiesRegEx =
                Pattern.compile("ApplicationName");
        mode.padFixedLengthStrings = true;
        // Enumerate all H2 types NOT supported by PostgreSQL:
        Set<String> disallowedTypes = new java.util.HashSet<>();
        disallowedTypes.add("NUMBER");
        disallowedTypes.add("IDENTITY");
        disallowedTypes.add("TINYINT");
        disallowedTypes.add("BLOB");
        mode.disallowedTypes = disallowedTypes;
        dt = DataType.createNumeric(19, 2, false);
        dt.type = Value.DECIMAL;
        dt.sqlType = Types.NUMERIC;
        dt.name = "MONEY";
        mode.typeByNameMap.put("MONEY", dt);
        mode.dateTimeValueWithinTransaction = true;
        add(mode);
    }

    private Mode(ModeEnum modeEnum) {
        this.name = modeEnum.name();
        this.modeEnum = modeEnum;
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

    public static Mode getRegular() {
        return getInstance(ModeEnum.REGULAR.name());
    }

    public String getName() {
        return name;
    }

    public ModeEnum getEnum() {
        return this.modeEnum;
    }

    @Override
    public String toString() {
        return name;
    }

}
