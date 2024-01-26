/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
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
        REGULAR, STRICT, LEGACY, DB2, Derby, MariaDB, MSSQLServer, HSQLDB, MySQL, Oracle, PostgreSQL
    }

    /**
     * Generation of column names for expressions.
     */
    public enum ExpressionNames {
        /**
         * Use optimized SQL representation of expression.
         */
        OPTIMIZED_SQL,

        /**
         * Use original SQL representation of expression.
         */
        ORIGINAL_SQL,

        /**
         * Generate empty name.
         */
        EMPTY,

        /**
         * Use ordinal number of a column.
         */
        NUMBER,

        /**
         * Use ordinal number of a column with C prefix.
         */
        C_NUMBER,

        /**
         * Use function name for functions and ?column? for other expressions
         */
        POSTGRESQL_STYLE,
    }

    /**
     * Generation of column names for expressions to be used in a view.
     */
    public enum ViewExpressionNames {
        /**
         * Use both specified and generated names as is.
         */
        AS_IS,

        /**
         * Throw exception for unspecified names.
         */
        EXCEPTION,

        /**
         * Use both specified and generated names as is, but replace too long
         * generated names with {@code Name_exp_###}.
         */
        MYSQL_STYLE,
    }

    /**
     * When CHAR values are right-padded with spaces.
     */
    public enum CharPadding {
        /**
         * CHAR values are always right-padded with spaces.
         */
        ALWAYS,

        /**
         * Spaces are trimmed from the right side of CHAR values, but CHAR
         * values in result sets are right-padded with spaces to the declared
         * length
         */
        IN_RESULT_SETS,

        /**
         * Spaces are trimmed from the right side of CHAR values.
         */
        NEVER
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
     * Identifiers may be quoted using square brackets as in [Test].
     */
    public boolean squareBracketQuotedNames;

    /**
     * The system columns 'ctid' and 'oid' are supported.
     */
    public boolean systemColumns;

    /**
     * Determines how rows with {@code NULL} values in indexed columns are handled
     * in unique indexes and constraints by default.
     */
    public NullsDistinct nullsDistinct = NullsDistinct.DISTINCT;

    /**
     * Empty strings are treated like NULL values. Useful for Oracle emulation.
     */
    public boolean treatEmptyStringsAsNull;

    /**
     * If {@code true} GREATEST and LEAST ignore nulls
     */
    public boolean greatestLeastIgnoreNulls;

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
     * How to pad or trim CHAR values.
     */
    public CharPadding charPadding = CharPadding.ALWAYS;

    /**
     * Whether DB2 TIMESTAMP formats are allowed.
     */
    public boolean allowDB2TimestampFormat;

    /**
     * Discard SQLServer table hints (e.g. "SELECT * FROM table WITH (NOLOCK)")
     */
    public boolean discardWithTableHints;

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
     * If {@code true} non-standard ALTER TABLE MODIFY COLUMN preserves nullability when changing data type.
     */
    public boolean alterTableModifyColumnPreserveNullability;

    /**
     * If {@code true} MySQL table and column options are allowed
     */
    public boolean mySqlTableOptions;

    /**
     * If {@code true} DELETE identifier FROM is allowed
     */
    public boolean deleteIdentifierFrom;

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
     * If {@code true} 'CHAR' and 'BYTE' length units are allowed.
     */
    public boolean charAndByteLengthUnits;

    /**
     * If {@code true}, sequence.NEXTVAL and sequence.CURRVAL pseudo columns are
     * supported.
     */
    public boolean nextvalAndCurrvalPseudoColumns;

    /**
     * If {@code true}, the next value expression returns different values when
     * invoked multiple times within a row. This setting does not affect
     * NEXTVAL() function.
     */
    public boolean nextValueReturnsDifferentValues;

    /**
     * If {@code true}, sequences of generated by default identity columns are
     * updated when value is provided by user.
     */
    public boolean updateSequenceOnManualIdentityInsertion;

    /**
     * If {@code true}, last identity of the session is updated on insertion of
     * a new value into identity column.
     */
    public boolean takeInsertedIdentity;

    /**
     * If {@code true}, last identity of the session is updated on generation of
     * a new sequence value.
     */
    public boolean takeGeneratedSequenceValue;

    /**
     * If {@code true}, identity columns have DEFAULT ON NULL clause.
     */
    public boolean identityColumnsHaveDefaultOnNull;

    /**
     * If {@code true}, merge when matched clause may have WHERE clause.
     */
    public boolean mergeWhere;

    /**
     * If {@code true}, allow using from clause in update statement.
     */
    public boolean allowUsingFromClauseInUpdateStatement;

    /**
     * If {@code true}, referential constraints will create a unique constraint
     * on referenced columns if it doesn't exist instead of throwing an
     * exception.
     */
    public boolean createUniqueConstraintForReferencedColumns;

    /**
     * How column names are generated for expressions.
     */
    public ExpressionNames expressionNames = ExpressionNames.OPTIMIZED_SQL;

    /**
     * How column names are generated for views.
     */
    public ViewExpressionNames viewExpressionNames = ViewExpressionNames.AS_IS;

    /**
     * Whether TOP clause in SELECT queries is supported.
     */
    public boolean topInSelect;

    /**
     * Whether TOP clause in DML commands is supported.
     */
    public boolean topInDML;

    /**
     * Whether LIMIT / OFFSET clauses are supported.
     */
    public boolean limit;

    /**
     * Whether MINUS can be used as EXCEPT.
     */
    public boolean minusIsExcept;

    /**
     * Whether IDENTITY pseudo data type is supported.
     */
    public boolean identityDataType;

    /**
     * Whether SERIAL and BIGSERIAL pseudo data types are supported.
     */
    public boolean serialDataTypes;

    /**
     * Whether SQL Server-style IDENTITY clause is supported.
     */
    public boolean identityClause;

    /**
     * Whether MySQL-style AUTO_INCREMENT clause is supported.
     */
    public boolean autoIncrementClause;

    /**
     * Whether DATE data type is parsed as TIMESTAMP(0).
     */
    public boolean dateIsTimestamp0;

    /**
     * Whether NUMERIC and DECIMAL/DEC without parameters are parsed as DECFLOAT.
     */
    public boolean numericIsDecfloat;

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

    /**
     * Allow to use GROUP BY n, where n is column index in the SELECT list, similar to ORDER BY
     */
    public boolean groupByColumnIndex;

    /**
     * Allow to compare numeric with BOOLEAN.
     */
    public boolean numericWithBooleanComparison;

    /**
     * Accepts comma ',' as key/value separator in JSON_OBJECT and JSON_OBJECTAGG functions.
     */
    public boolean acceptsCommaAsJsonKeyValueSeparator;

    private final String name;

    private final ModeEnum modeEnum;

    static {
        Mode mode = new Mode(ModeEnum.REGULAR);
        mode.allowEmptyInPredicate = true;
        mode.dateTimeValueWithinTransaction = true;
        mode.topInSelect = true;
        mode.limit = true;
        mode.minusIsExcept = true;
        mode.identityDataType = true;
        mode.serialDataTypes = true;
        mode.autoIncrementClause = true;
        add(mode);

        mode = new Mode(ModeEnum.STRICT);
        mode.dateTimeValueWithinTransaction = true;
        add(mode);

        mode = new Mode(ModeEnum.LEGACY);
        // Features of REGULAR mode
        mode.allowEmptyInPredicate = true;
        mode.dateTimeValueWithinTransaction = true;
        mode.topInSelect = true;
        mode.limit = true;
        mode.minusIsExcept = true;
        mode.identityDataType = true;
        mode.serialDataTypes = true;
        mode.autoIncrementClause = true;
        // Legacy identity and sequence features
        mode.identityClause = true;
        mode.updateSequenceOnManualIdentityInsertion = true;
        mode.takeInsertedIdentity = true;
        mode.identityColumnsHaveDefaultOnNull = true;
        mode.nextvalAndCurrvalPseudoColumns = true;
        // Legacy DML features
        mode.topInDML = true;
        mode.mergeWhere = true;
        // Legacy DDL features
        mode.createUniqueConstraintForReferencedColumns = true;
        // Legacy numeric with boolean comparison
        mode.numericWithBooleanComparison = true;
        // Legacy GREATEST and LEAST null treatment
        mode.greatestLeastIgnoreNulls = true;
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
        mode.takeInsertedIdentity = true;
        mode.nextvalAndCurrvalPseudoColumns = true;
        mode.expressionNames = ExpressionNames.NUMBER;
        mode.viewExpressionNames = ViewExpressionNames.EXCEPTION;
        mode.limit = true;
        mode.minusIsExcept = true;
        mode.numericWithBooleanComparison = true;
        add(mode);

        mode = new Mode(ModeEnum.Derby);
        mode.aliasColumnName = true;
        mode.nullsDistinct = NullsDistinct.NOT_DISTINCT;
        mode.sysDummy1 = true;
        mode.isolationLevelInSelectOrInsertStatement = true;
        // Derby does not support client info properties as of version 10.12.1.1
        mode.supportedClientInfoPropertiesRegEx = null;
        mode.forBitData = true;
        mode.takeInsertedIdentity = true;
        mode.expressionNames = ExpressionNames.NUMBER;
        mode.viewExpressionNames = ViewExpressionNames.EXCEPTION;
        add(mode);

        mode = new Mode(ModeEnum.HSQLDB);
        mode.allowPlusForStringConcat = true;
        mode.identityColumnsHaveDefaultOnNull = true;
        // HSQLDB does not support client info properties. See
        // http://hsqldb.org/doc/apidocs/org/hsqldb/jdbc/JDBCConnection.html#setClientInfo-java.lang.String-java.lang.String-
        mode.supportedClientInfoPropertiesRegEx = null;
        mode.expressionNames = ExpressionNames.C_NUMBER;
        mode.topInSelect = true;
        mode.limit = true;
        mode.minusIsExcept = true;
        mode.numericWithBooleanComparison = true;
        add(mode);

        mode = new Mode(ModeEnum.MSSQLServer);
        mode.aliasColumnName = true;
        mode.squareBracketQuotedNames = true;
        mode.nullsDistinct = NullsDistinct.NOT_DISTINCT;
        mode.greatestLeastIgnoreNulls = true;
        mode.allowPlusForStringConcat = true;
        mode.swapLogFunctionParameters = true;
        mode.swapConvertFunctionParameters = true;
        mode.supportPoundSymbolForColumnNames = true;
        mode.discardWithTableHints = true;
        // MS SQL Server does not support client info properties. See
        // https://msdn.microsoft.com/en-Us/library/dd571296%28v=sql.110%29.aspx
        mode.supportedClientInfoPropertiesRegEx = null;
        mode.zeroExLiteralsAreBinaryStrings = true;
        mode.truncateTableRestartIdentity = true;
        mode.takeInsertedIdentity = true;
        DataType dt = DataType.createNumeric(19, 4);
        dt.type = Value.NUMERIC;
        dt.sqlType = Types.NUMERIC;
        dt.specialPrecisionScale = true;
        mode.typeByNameMap.put("MONEY", dt);
        dt = DataType.createNumeric(10, 4);
        dt.type = Value.NUMERIC;
        dt.sqlType = Types.NUMERIC;
        dt.specialPrecisionScale = true;
        mode.typeByNameMap.put("SMALLMONEY", dt);
        mode.typeByNameMap.put("UNIQUEIDENTIFIER", DataType.getDataType(Value.UUID));
        mode.allowEmptySchemaValuesAsDefaultSchema = true;
        mode.expressionNames = ExpressionNames.EMPTY;
        mode.viewExpressionNames = ViewExpressionNames.EXCEPTION;
        mode.topInSelect = true;
        mode.topInDML = true;
        mode.identityClause = true;
        mode.numericWithBooleanComparison = true;
        add(mode);

        mode = new Mode(ModeEnum.MariaDB);
        mode.indexDefinitionInCreateTable = true;
        mode.regexpReplaceBackslashReferences = true;
        mode.onDuplicateKeyUpdate = true;
        mode.replaceInto = true;
        mode.charPadding = CharPadding.NEVER;
        mode.supportedClientInfoPropertiesRegEx = Pattern.compile(".*");
        mode.zeroExLiteralsAreBinaryStrings = true;
        mode.allowUnrelatedOrderByExpressionsInDistinctQueries = true;
        mode.alterTableExtensionsMySQL = true;
        mode.alterTableModifyColumn = true;
        mode.mySqlTableOptions = true;
        mode.deleteIdentifierFrom = true;
        mode.truncateTableRestartIdentity = true;
        mode.allNumericTypesHavePrecision = true;
        mode.nextValueReturnsDifferentValues = true;
        mode.updateSequenceOnManualIdentityInsertion = true;
        mode.takeInsertedIdentity = true;
        mode.identityColumnsHaveDefaultOnNull = true;
        mode.expressionNames = ExpressionNames.ORIGINAL_SQL;
        mode.viewExpressionNames = ViewExpressionNames.MYSQL_STYLE;
        mode.limit = true;
        mode.autoIncrementClause = true;
        mode.typeByNameMap.put("YEAR", DataType.getDataType(Value.SMALLINT));
        mode.groupByColumnIndex = true;
        mode.numericWithBooleanComparison = true;
        mode.acceptsCommaAsJsonKeyValueSeparator = true;
        add(mode);

        mode = new Mode(ModeEnum.MySQL);
        mode.indexDefinitionInCreateTable = true;
        mode.regexpReplaceBackslashReferences = true;
        mode.onDuplicateKeyUpdate = true;
        mode.replaceInto = true;
        mode.charPadding = CharPadding.NEVER;
        // MySQL allows to use any key for client info entries. See
        // https://github.com/mysql/mysql-connector-j/blob/5.1.47/src/com/mysql/jdbc/JDBC4CommentClientInfoProvider.java
        mode.supportedClientInfoPropertiesRegEx =
                Pattern.compile(".*");
        mode.zeroExLiteralsAreBinaryStrings = true;
        mode.allowUnrelatedOrderByExpressionsInDistinctQueries = true;
        mode.alterTableExtensionsMySQL = true;
        mode.alterTableModifyColumn = true;
        mode.mySqlTableOptions = true;
        mode.deleteIdentifierFrom = true;
        mode.truncateTableRestartIdentity = true;
        mode.allNumericTypesHavePrecision = true;
        mode.updateSequenceOnManualIdentityInsertion = true;
        mode.takeInsertedIdentity = true;
        mode.identityColumnsHaveDefaultOnNull = true;
        mode.createUniqueConstraintForReferencedColumns = true;
        mode.expressionNames = ExpressionNames.ORIGINAL_SQL;
        mode.viewExpressionNames = ViewExpressionNames.MYSQL_STYLE;
        mode.limit = true;
        mode.autoIncrementClause = true;
        mode.typeByNameMap.put("YEAR", DataType.getDataType(Value.SMALLINT));
        mode.groupByColumnIndex = true;
        mode.numericWithBooleanComparison = true;
        mode.acceptsCommaAsJsonKeyValueSeparator = true;
        add(mode);

        mode = new Mode(ModeEnum.Oracle);
        mode.aliasColumnName = true;
        mode.convertOnlyToSmallerScale = true;
        mode.nullsDistinct = NullsDistinct.ALL_DISTINCT;
        mode.treatEmptyStringsAsNull = true;
        mode.regexpReplaceBackslashReferences = true;
        mode.supportPoundSymbolForColumnNames = true;
        // Oracle accepts keys of the form <namespace>.*. See
        // https://docs.oracle.com/database/121/JJDBC/jdbcvers.htm#JJDBC29006
        mode.supportedClientInfoPropertiesRegEx =
                Pattern.compile(".*\\..*");
        mode.alterTableModifyColumn = true;
        mode.alterTableModifyColumnPreserveNullability = true;
        mode.decimalSequences = true;
        mode.charAndByteLengthUnits = true;
        mode.nextvalAndCurrvalPseudoColumns = true;
        mode.mergeWhere = true;
        mode.minusIsExcept = true;
        mode.expressionNames = ExpressionNames.ORIGINAL_SQL;
        mode.viewExpressionNames = ViewExpressionNames.EXCEPTION;
        mode.dateIsTimestamp0 = true;
        mode.typeByNameMap.put("BINARY_FLOAT", DataType.getDataType(Value.REAL));
        mode.typeByNameMap.put("BINARY_DOUBLE", DataType.getDataType(Value.DOUBLE));
        add(mode);

        mode = new Mode(ModeEnum.PostgreSQL);
        mode.aliasColumnName = true;
        mode.systemColumns = true;
        mode.greatestLeastIgnoreNulls = true;
        mode.logIsLogBase10 = true;
        mode.regexpReplaceBackslashReferences = true;
        mode.insertOnConflict = true;
        // PostgreSQL only supports the ApplicationName property. See
        // https://github.com/hhru/postgres-jdbc/blob/master/postgresql-jdbc-9.2-1002.src/
        //     org/postgresql/jdbc4/AbstractJdbc4Connection.java
        mode.supportedClientInfoPropertiesRegEx =
                Pattern.compile("ApplicationName");
        mode.charPadding = CharPadding.IN_RESULT_SETS;
        mode.nextValueReturnsDifferentValues = true;
        mode.takeGeneratedSequenceValue = true;
        mode.expressionNames = ExpressionNames.POSTGRESQL_STYLE;
        mode.allowUsingFromClauseInUpdateStatement = true;
        mode.limit = true;
        mode.serialDataTypes = true;
        mode.numericIsDecfloat = true;
        // Enumerate all H2 types NOT supported by PostgreSQL:
        Set<String> disallowedTypes = new java.util.HashSet<>();
        disallowedTypes.add("NUMBER");
        disallowedTypes.add("TINYINT");
        disallowedTypes.add("BLOB");
        disallowedTypes.add("VARCHAR_IGNORECASE");
        mode.disallowedTypes = disallowedTypes;
        dt = DataType.getDataType(Value.JSON);
        mode.typeByNameMap.put("JSONB", dt);
        dt = DataType.createNumeric(19, 2);
        dt.type = Value.NUMERIC;
        dt.sqlType = Types.NUMERIC;
        dt.specialPrecisionScale = true;
        mode.typeByNameMap.put("MONEY", dt);
        dt = DataType.getDataType(Value.INTEGER);
        mode.typeByNameMap.put("OID", dt);
        mode.dateTimeValueWithinTransaction = true;
        mode.groupByColumnIndex = true;
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
