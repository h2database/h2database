/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc.meta;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.ParameterInterface;
import org.h2.expression.condition.CompareLike;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.SimpleResult;
import org.h2.schema.Schema;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;
import org.h2.value.ValueVarchar;

/**
 * Local implementation of database meta information.
 */
public final class DatabaseMetaLocal extends DatabaseMetaLocalBase {

    private static final Value PERCENT = ValueVarchar.get("%");

    private static final Value BACKSLASH = ValueVarchar.get("\\");

    private static final Value YES = ValueVarchar.get("YES");

    private static final Value NO = ValueVarchar.get("NO");

    private static final Value SCHEMA_MAIN = ValueVarchar.get(Constants.SCHEMA_MAIN);

    private final Session session;

    private Comparator<String> comparator;

    public DatabaseMetaLocal(Session session) {
        this.session = session;
    }

    @Override
    public String getSQLKeywords() {
        return "CURRENT_CATALOG," //
                + "CURRENT_SCHEMA," //
                + "GROUPS," //
                + "IF,ILIKE,INTERSECTS," //
                + "LIMIT," //
                + "MINUS," //
                + "OFFSET," //
                + "QUALIFY," //
                + "REGEXP,ROWNUM," //
                + "SYSDATE,SYSTIME,SYSTIMESTAMP," //
                + "TODAY,TOP,"//
                + "_ROWID_";
    }

    @Override
    public String getNumericFunctions() {
        return getFunctions("Functions (Numeric)");
    }

    @Override
    public String getStringFunctions() {
        return getFunctions("Functions (String)");
    }

    @Override
    public String getSystemFunctions() {
        return getFunctions("Functions (System)");
    }

    @Override
    public String getTimeDateFunctions() {
        return getFunctions("Functions (Time and Date)");
    }

    private String getFunctions(String section) {
        String sql = "SELECT TOPIC FROM INFORMATION_SCHEMA.HELP WHERE SECTION = ?";
        Value[] args = new Value[] { getString(section) };
        ResultInterface result = executeQuery(sql, args);
        StringBuilder builder = new StringBuilder();
        while (result.next()) {
            String s = result.currentRow()[0].getString().trim();
            String[] array = StringUtils.arraySplit(s, ',', true);
            for (String a : array) {
                if (builder.length() != 0) {
                    builder.append(',');
                }
                String f = a.trim();
                int spaceIndex = f.indexOf(' ');
                if (spaceIndex >= 0) {
                    // remove 'Function' from 'INSERT Function'
                    StringUtils.trimSubstring(builder, f, 0, spaceIndex);
                } else {
                    builder.append(f);
                }
            }
        }
        return builder.toString();
    }

    @Override
    public String getSearchStringEscape() {
        return session.getDatabase().getSettings().defaultEscape;
    }

    @Override
    public ResultInterface getProcedures(String catalogPattern, String schemaPattern, String procedureNamePattern) {
        return executeQuery("SELECT " //
                + "ALIAS_CATALOG PROCEDURE_CAT, " //
                + "ALIAS_SCHEMA PROCEDURE_SCHEM, " //
                + "ALIAS_NAME PROCEDURE_NAME, " //
                + "COLUMN_COUNT NUM_INPUT_PARAMS, " //
                + "ZERO() NUM_OUTPUT_PARAMS, " //
                + "ZERO() NUM_RESULT_SETS, " //
                + "REMARKS, " //
                + "RETURNS_RESULT PROCEDURE_TYPE, " //
                + "ALIAS_NAME SPECIFIC_NAME " //
                + "FROM INFORMATION_SCHEMA.FUNCTION_ALIASES " //
                + "WHERE ALIAS_CATALOG LIKE ?1 ESCAPE ?4 " //
                + "AND ALIAS_SCHEMA LIKE ?2 ESCAPE ?4 " //
                + "AND ALIAS_NAME LIKE ?3 ESCAPE ?4 " //
                + "ORDER BY PROCEDURE_SCHEM, PROCEDURE_NAME, NUM_INPUT_PARAMS", //
                getCatalogPattern(catalogPattern), //
                getSchemaPattern(schemaPattern), //
                getPattern(procedureNamePattern), //
                BACKSLASH);
    }

    @Override
    public ResultInterface getProcedureColumns(String catalogPattern, String schemaPattern, //
            String procedureNamePattern, String columnNamePattern) {
        return executeQuery("SELECT " //
                + "ALIAS_CATALOG PROCEDURE_CAT, " //
                + "ALIAS_SCHEMA PROCEDURE_SCHEM, " //
                + "ALIAS_NAME PROCEDURE_NAME, " //
                + "COLUMN_NAME, " //
                + "COLUMN_TYPE, " //
                + "DATA_TYPE, " //
                + "TYPE_NAME, " //
                + "PRECISION, " //
                + "PRECISION LENGTH, " //
                + "SCALE, " //
                + "RADIX, " //
                + "NULLABLE, " //
                + "REMARKS, " //
                + "COLUMN_DEFAULT COLUMN_DEF, " //
                + "ZERO() SQL_DATA_TYPE, " //
                + "ZERO() SQL_DATETIME_SUB, " //
                + "ZERO() CHAR_OCTET_LENGTH, " //
                + "POS ORDINAL_POSITION, " //
                + "?1 IS_NULLABLE, " //
                + "ALIAS_NAME SPECIFIC_NAME " //
                + "FROM INFORMATION_SCHEMA.FUNCTION_COLUMNS " //
                + "WHERE ALIAS_CATALOG LIKE ?2 ESCAPE ?6 " //
                + "AND ALIAS_SCHEMA LIKE ?3 ESCAPE ?6 " //
                + "AND ALIAS_NAME LIKE ?4 ESCAPE ?6 " //
                + "AND COLUMN_NAME LIKE ?5 ESCAPE ?6 " //
                + "ORDER BY PROCEDURE_SCHEM, PROCEDURE_NAME, ORDINAL_POSITION", //
                YES, //
                getCatalogPattern(catalogPattern), //
                getSchemaPattern(schemaPattern), //
                getPattern(procedureNamePattern), //
                getPattern(columnNamePattern), //
                BACKSLASH);
    }

    @Override
    public ResultInterface getTables(String catalogPattern, String schemaPattern, String tableNamePattern,
            String[] types) {
        int typesLength = types != null ? types.length : 0;
        boolean includeSynonyms = types == null || Arrays.asList(types).contains("SYNONYM");
        // (1024 - 16) is enough for the most cases
        StringBuilder select = new StringBuilder(1008);
        if (includeSynonyms) {
            select.append("SELECT " //
                    + "TABLE_CAT, " //
                    + "TABLE_SCHEM, " //
                    + "TABLE_NAME, " //
                    + "TABLE_TYPE, " //
                    + "REMARKS, " //
                    + "TYPE_CAT, " //
                    + "TYPE_SCHEM, " //
                    + "TYPE_NAME, " //
                    + "SELF_REFERENCING_COL_NAME, " //
                    + "REF_GENERATION, " //
                    + "SQL " //
                    + "FROM (" //
                    + "SELECT " //
                    + "SYNONYM_CATALOG TABLE_CAT, " //
                    + "SYNONYM_SCHEMA TABLE_SCHEM, " //
                    + "SYNONYM_NAME as TABLE_NAME, " //
                    + "TYPE_NAME AS TABLE_TYPE, " //
                    + "REMARKS, " //
                    + "TYPE_NAME TYPE_CAT, " //
                    + "TYPE_NAME TYPE_SCHEM, " //
                    + "TYPE_NAME AS TYPE_NAME, " //
                    + "TYPE_NAME SELF_REFERENCING_COL_NAME, " //
                    + "TYPE_NAME REF_GENERATION, " //
                    + "NULL AS SQL " //
                    + "FROM INFORMATION_SCHEMA.SYNONYMS " //
                    + "WHERE SYNONYM_CATALOG LIKE ?1 ESCAPE ?4 " //
                    + "AND SYNONYM_SCHEMA LIKE ?2 ESCAPE ?4 " //
                    + "AND SYNONYM_NAME LIKE ?3 ESCAPE ?4 " //
                    + "UNION ");
        }
        select.append("SELECT " //
                + "TABLE_CATALOG TABLE_CAT, " //
                + "TABLE_SCHEMA TABLE_SCHEM, " //
                + "TABLE_NAME, " //
                + "TABLE_TYPE, " //
                + "REMARKS, " //
                + "TYPE_NAME TYPE_CAT, " //
                + "TYPE_NAME TYPE_SCHEM, " //
                + "TYPE_NAME, " //
                + "TYPE_NAME SELF_REFERENCING_COL_NAME, " //
                + "TYPE_NAME REF_GENERATION, " //
                + "SQL " //
                + "FROM INFORMATION_SCHEMA.TABLES " //
                + "WHERE TABLE_CATALOG LIKE ?1 ESCAPE ?4 " //
                + "AND TABLE_SCHEMA LIKE ?2 ESCAPE ?4 " //
                + "AND TABLE_NAME LIKE ?3 ESCAPE ?4");
        if (typesLength > 0) {
            select.append(" AND TABLE_TYPE IN(");
            for (int i = 0; i < typesLength; i++) {
                if (i > 0) {
                    select.append(", ");
                }
                select.append('?').append(i + 5);
            }
            select.append(')');
        }
        if (includeSynonyms) {
            select.append(')');
        }
        Value[] args = new Value[typesLength + 4];
        args[0] = getCatalogPattern(catalogPattern);
        args[1] = getSchemaPattern(schemaPattern);
        args[2] = getPattern(tableNamePattern);
        args[3] = BACKSLASH;
        for (int i = 0; i < typesLength; i++) {
            args[i + 4] = getString(types[i]);
        }
        return executeQuery(select.append(" ORDER BY TABLE_TYPE, TABLE_SCHEM, TABLE_NAME").toString(), args);
    }

    @Override
    public ResultInterface getSchemas() {
        return getSchemas(null, null);
    }

    @Override
    public ResultInterface getCatalogs() {
        checkClosed();
        SimpleResult result = new SimpleResult();
        result.addColumn("TABLE_CAT", TypeInfo.TYPE_VARCHAR);
        result.addRow(getString(session.getDatabase().getShortName()));
        return result;
    }

    @Override
    public ResultInterface getTableTypes() {
        return executeQuery("SELECT " //
                + "TYPE TABLE_TYPE " //
                + "FROM INFORMATION_SCHEMA.TABLE_TYPES " //
                + "ORDER BY TABLE_TYPE");
    }

    @Override
    public ResultInterface getColumns(String catalogPattern, String schemaPattern, String tableNamePattern,
            String columnNamePattern) {
        return executeQuery("SELECT " //
                + "TABLE_CAT, " //
                + "TABLE_SCHEM, " //
                + "TABLE_NAME, " //
                + "COLUMN_NAME, " //
                + "DATA_TYPE, " //
                + "TYPE_NAME, " //
                + "COLUMN_SIZE, " //
                + "BUFFER_LENGTH, " //
                + "DECIMAL_DIGITS, " //
                + "NUM_PREC_RADIX, " //
                + "NULLABLE, " //
                + "REMARKS, " //
                + "COLUMN_DEF, " //
                + "SQL_DATA_TYPE, " //
                + "SQL_DATETIME_SUB, " //
                + "CHAR_OCTET_LENGTH, " //
                + "ORDINAL_POSITION, " //
                + "IS_NULLABLE, " //
                + "SCOPE_CATALOG, " //
                + "SCOPE_SCHEMA, " //
                + "SCOPE_TABLE, " //
                + "SOURCE_DATA_TYPE, " //
                + "IS_AUTOINCREMENT, " //
                + "IS_GENERATEDCOLUMN " //
                + "FROM (" //
                + "SELECT " //
                + "s.SYNONYM_CATALOG TABLE_CAT, " //
                + "s.SYNONYM_SCHEMA TABLE_SCHEM, " //
                + "s.SYNONYM_NAME TABLE_NAME, " //
                + "c.COLUMN_NAME, " //
                + "c.DATA_TYPE, " //
                + "c.TYPE_NAME, " //
                + "c.CHARACTER_MAXIMUM_LENGTH COLUMN_SIZE, " //
                + "c.CHARACTER_MAXIMUM_LENGTH BUFFER_LENGTH, " //
                + "c.NUMERIC_SCALE DECIMAL_DIGITS, " //
                + "c.NUMERIC_PRECISION_RADIX NUM_PREC_RADIX, " //
                + "c.NULLABLE, " //
                + "c.REMARKS, " //
                + "c.COLUMN_DEFAULT COLUMN_DEF, " //
                + "c.DATA_TYPE SQL_DATA_TYPE, " //
                + "ZERO() SQL_DATETIME_SUB, " //
                + "c.CHARACTER_OCTET_LENGTH CHAR_OCTET_LENGTH, " //
                + "c.ORDINAL_POSITION, " //
                + "c.IS_NULLABLE IS_NULLABLE, " //
                + "CAST(c.SOURCE_DATA_TYPE AS VARCHAR) SCOPE_CATALOG, " //
                + "CAST(c.SOURCE_DATA_TYPE AS VARCHAR) SCOPE_SCHEMA, " //
                + "CAST(c.SOURCE_DATA_TYPE AS VARCHAR) SCOPE_TABLE, " //
                + "c.SOURCE_DATA_TYPE, " //
                + "CASE WHEN c.SEQUENCE_NAME IS NULL THEN " //
                + "CAST(?1 AS VARCHAR) ELSE CAST(?2 AS VARCHAR) END IS_AUTOINCREMENT, " //
                + "CASE WHEN c.IS_COMPUTED THEN " //
                + "CAST(?2 AS VARCHAR) ELSE CAST(?1 AS VARCHAR) END IS_GENERATEDCOLUMN " //
                + "FROM INFORMATION_SCHEMA.COLUMNS c JOIN INFORMATION_SCHEMA.SYNONYMS s ON " //
                + "s.SYNONYM_FOR = c.TABLE_NAME " //
                + "AND s.SYNONYM_FOR_SCHEMA = c.TABLE_SCHEMA " //
                + "WHERE s.SYNONYM_CATALOG LIKE ?3 ESCAPE ?7 " //
                + "AND s.SYNONYM_SCHEMA LIKE ?4 ESCAPE ?7 " //
                + "AND s.SYNONYM_NAME LIKE ?5 ESCAPE ?7 " //
                + "AND c.COLUMN_NAME LIKE ?6 ESCAPE ?7 " //
                + "UNION SELECT " //
                + "TABLE_CATALOG TABLE_CAT, " //
                + "TABLE_SCHEMA TABLE_SCHEM, " //
                + "TABLE_NAME, " //
                + "COLUMN_NAME, " //
                + "DATA_TYPE, " //
                + "TYPE_NAME, " //
                + "CHARACTER_MAXIMUM_LENGTH COLUMN_SIZE, " //
                + "CHARACTER_MAXIMUM_LENGTH BUFFER_LENGTH, " //
                + "NUMERIC_SCALE DECIMAL_DIGITS, " //
                + "NUMERIC_PRECISION_RADIX NUM_PREC_RADIX, " //
                + "NULLABLE, " //
                + "REMARKS, " //
                + "COLUMN_DEFAULT COLUMN_DEF, " //
                + "DATA_TYPE SQL_DATA_TYPE, " //
                + "ZERO() SQL_DATETIME_SUB, " //
                + "CHARACTER_OCTET_LENGTH CHAR_OCTET_LENGTH, " //
                + "ORDINAL_POSITION, " //
                + "IS_NULLABLE IS_NULLABLE, " //
                + "CAST(SOURCE_DATA_TYPE AS VARCHAR) SCOPE_CATALOG, " //
                + "CAST(SOURCE_DATA_TYPE AS VARCHAR) SCOPE_SCHEMA, " //
                + "CAST(SOURCE_DATA_TYPE AS VARCHAR) SCOPE_TABLE, " //
                + "SOURCE_DATA_TYPE, " //
                + "CASE WHEN SEQUENCE_NAME IS NULL THEN " //
                + "CAST(?1 AS VARCHAR) ELSE CAST(?2 AS VARCHAR) END IS_AUTOINCREMENT, " //
                + "CASE WHEN IS_COMPUTED THEN " //
                + "CAST(?2 AS VARCHAR) ELSE CAST(?1 AS VARCHAR) END IS_GENERATEDCOLUMN " //
                + "FROM INFORMATION_SCHEMA.COLUMNS " //
                + "WHERE TABLE_CATALOG LIKE ?3 ESCAPE ?7 " //
                + "AND TABLE_SCHEMA LIKE ?4 ESCAPE ?7 " //
                + "AND TABLE_NAME LIKE ?5 ESCAPE ?7 " //
                + "AND COLUMN_NAME LIKE ?6 ESCAPE ?7 " //
                + "ORDER BY TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION)", NO, //
                YES, //
                getCatalogPattern(catalogPattern), //
                getSchemaPattern(schemaPattern), //
                getPattern(tableNamePattern), //
                getPattern(columnNamePattern), //
                BACKSLASH);
    }

    @Override
    public ResultInterface getColumnPrivileges(String catalogPattern, String schemaPattern, String table,
            String columnNamePattern) {
        return executeQuery("SELECT " //
                + "TABLE_CATALOG TABLE_CAT, " //
                + "TABLE_SCHEMA TABLE_SCHEM, " //
                + "TABLE_NAME, " //
                + "COLUMN_NAME, " //
                + "GRANTOR, " //
                + "GRANTEE, " //
                + "PRIVILEGE_TYPE PRIVILEGE, " //
                + "IS_GRANTABLE " //
                + "FROM INFORMATION_SCHEMA.COLUMN_PRIVILEGES " //
                + "WHERE TABLE_CATALOG LIKE ?1 ESCAPE ?5 " //
                + "AND TABLE_SCHEMA LIKE ?2 ESCAPE ?5 " //
                + "AND TABLE_NAME = ?3 " //
                + "AND COLUMN_NAME LIKE ?4 ESCAPE ?5 " //
                + "ORDER BY COLUMN_NAME, PRIVILEGE", //
                getCatalogPattern(catalogPattern), //
                getSchemaPattern(schemaPattern), //
                getString(table), //
                getPattern(columnNamePattern), //
                BACKSLASH);
    }

    @Override
    public ResultInterface getTablePrivileges(String catalogPattern, String schemaPattern, String tableNamePattern) {
        return executeQuery("SELECT " //
                + "TABLE_CATALOG TABLE_CAT, " //
                + "TABLE_SCHEMA TABLE_SCHEM, " //
                + "TABLE_NAME, " //
                + "GRANTOR, " //
                + "GRANTEE, " //
                + "PRIVILEGE_TYPE PRIVILEGE, " //
                + "IS_GRANTABLE " //
                + "FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES " //
                + "WHERE TABLE_CATALOG LIKE ?1 ESCAPE ?4 " //
                + "AND TABLE_SCHEMA LIKE ?2 ESCAPE ?4 " //
                + "AND TABLE_NAME LIKE ?3 ESCAPE ?4 " //
                + "ORDER BY TABLE_SCHEM, TABLE_NAME, PRIVILEGE", //
                getCatalogPattern(catalogPattern), //
                getSchemaPattern(schemaPattern), //
                getPattern(tableNamePattern), //
                BACKSLASH);
    }

    @Override
    public ResultInterface getBestRowIdentifier(String catalogPattern, String schemaPattern, String tableName,
            int scope, boolean nullable) {
        return executeQuery("SELECT " //
                + "CAST(?1 AS SMALLINT) SCOPE, " //
                + "C.COLUMN_NAME, " //
                + "C.DATA_TYPE, " //
                + "C.TYPE_NAME, " //
                + "C.CHARACTER_MAXIMUM_LENGTH COLUMN_SIZE, " //
                + "C.CHARACTER_MAXIMUM_LENGTH BUFFER_LENGTH, " //
                + "CAST(C.NUMERIC_SCALE AS SMALLINT) DECIMAL_DIGITS, " //
                + "CAST(?2 AS SMALLINT) PSEUDO_COLUMN " //
                + "FROM INFORMATION_SCHEMA.INDEXES I, " //
                + "INFORMATION_SCHEMA.COLUMNS C " //
                + "WHERE C.TABLE_NAME = I.TABLE_NAME " //
                + "AND C.COLUMN_NAME = I.COLUMN_NAME " //
                + "AND C.TABLE_CATALOG LIKE ?3 ESCAPE ?6 " //
                + "AND C.TABLE_SCHEMA LIKE ?4 ESCAPE ?6 " //
                + "AND C.TABLE_NAME = ?5 " //
                + "AND I.PRIMARY_KEY = TRUE " //
                + "ORDER BY SCOPE", //
                // SCOPE
                ValueInteger.get(DatabaseMetaData.bestRowSession), //
                // PSEUDO_COLUMN
                ValueInteger.get(DatabaseMetaData.bestRowNotPseudo), //
                getCatalogPattern(catalogPattern), //
                getSchemaPattern(schemaPattern), //
                getString(tableName), //
                BACKSLASH);
    }

    @Override
    public ResultInterface getPrimaryKeys(String catalogPattern, String schemaPattern, String tableName) {
        return executeQuery("SELECT " //
                + "TABLE_CATALOG TABLE_CAT, " //
                + "TABLE_SCHEMA TABLE_SCHEM, " //
                + "TABLE_NAME, " //
                + "COLUMN_NAME, " //
                + "ORDINAL_POSITION KEY_SEQ, " //
                + "COALESCE(CONSTRAINT_NAME, INDEX_NAME) PK_NAME " //
                + "FROM INFORMATION_SCHEMA.INDEXES " //
                + "WHERE TABLE_CATALOG LIKE ?1 ESCAPE ?4 " //
                + "AND TABLE_SCHEMA LIKE ?2 ESCAPE ?4 " //
                + "AND TABLE_NAME = ?3 " //
                + "AND PRIMARY_KEY = TRUE " //
                + "ORDER BY COLUMN_NAME", //
                getCatalogPattern(catalogPattern), //
                getSchemaPattern(schemaPattern), //
                getString(tableName), //
                BACKSLASH);
    }

    @Override
    public ResultInterface getImportedKeys(String catalogPattern, String schemaPattern, String tableName) {
        return executeQuery("SELECT " //
                + "PKTABLE_CATALOG PKTABLE_CAT, " //
                + "PKTABLE_SCHEMA PKTABLE_SCHEM, " //
                + "PKTABLE_NAME PKTABLE_NAME, " //
                + "PKCOLUMN_NAME, " //
                + "FKTABLE_CATALOG FKTABLE_CAT, " //
                + "FKTABLE_SCHEMA FKTABLE_SCHEM, " //
                + "FKTABLE_NAME, " //
                + "FKCOLUMN_NAME, " //
                + "ORDINAL_POSITION KEY_SEQ, " //
                + "UPDATE_RULE, " //
                + "DELETE_RULE, " //
                + "FK_NAME, " //
                + "PK_NAME, " //
                + "DEFERRABILITY " //
                + "FROM INFORMATION_SCHEMA.CROSS_REFERENCES " //
                + "WHERE FKTABLE_CATALOG LIKE ?1 ESCAPE ?4 " //
                + "AND FKTABLE_SCHEMA LIKE ?2 ESCAPE ?4 " //
                + "AND FKTABLE_NAME = ?3 " //
                + "ORDER BY PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, FK_NAME, KEY_SEQ", //
                getCatalogPattern(catalogPattern), //
                getSchemaPattern(schemaPattern), //
                getString(tableName), //
                BACKSLASH);
    }

    @Override
    public ResultInterface getExportedKeys(String catalogPattern, String schemaPattern, String tableName) {
        return executeQuery("SELECT " //
                + "PKTABLE_CATALOG PKTABLE_CAT, " //
                + "PKTABLE_SCHEMA PKTABLE_SCHEM, " //
                + "PKTABLE_NAME PKTABLE_NAME, " //
                + "PKCOLUMN_NAME, " //
                + "FKTABLE_CATALOG FKTABLE_CAT, " //
                + "FKTABLE_SCHEMA FKTABLE_SCHEM, " //
                + "FKTABLE_NAME, " //
                + "FKCOLUMN_NAME, " //
                + "ORDINAL_POSITION KEY_SEQ, " //
                + "UPDATE_RULE, " //
                + "DELETE_RULE, " //
                + "FK_NAME, " //
                + "PK_NAME, " //
                + "DEFERRABILITY " //
                + "FROM INFORMATION_SCHEMA.CROSS_REFERENCES " //
                + "WHERE PKTABLE_CATALOG LIKE ?1 ESCAPE ?4 " //
                + "AND PKTABLE_SCHEMA LIKE ?2 ESCAPE ?4 " //
                + "AND PKTABLE_NAME = ?3 " //
                + "ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, FK_NAME, KEY_SEQ", //
                getCatalogPattern(catalogPattern), //
                getSchemaPattern(schemaPattern), //
                getString(tableName), //
                BACKSLASH);
    }

    @Override
    public ResultInterface getCrossReference(String primaryCatalogPattern, String primarySchemaPattern,
            String primaryTable, String foreignCatalogPattern, String foreignSchemaPattern, String foreignTable) {
        return executeQuery("SELECT " //
                + "PKTABLE_CATALOG PKTABLE_CAT, " //
                + "PKTABLE_SCHEMA PKTABLE_SCHEM, " //
                + "PKTABLE_NAME PKTABLE_NAME, " //
                + "PKCOLUMN_NAME, " //
                + "FKTABLE_CATALOG FKTABLE_CAT, " //
                + "FKTABLE_SCHEMA FKTABLE_SCHEM, " //
                + "FKTABLE_NAME, " //
                + "FKCOLUMN_NAME, " //
                + "ORDINAL_POSITION KEY_SEQ, " //
                + "UPDATE_RULE, " //
                + "DELETE_RULE, " //
                + "FK_NAME, " //
                + "PK_NAME, " //
                + "DEFERRABILITY " //
                + "FROM INFORMATION_SCHEMA.CROSS_REFERENCES " //
                + "WHERE PKTABLE_CATALOG LIKE ?1 ESCAPE ?7 " //
                + "AND PKTABLE_SCHEMA LIKE ?2 ESCAPE ?7 " //
                + "AND PKTABLE_NAME = ?3 " //
                + "AND FKTABLE_CATALOG LIKE ?4 ESCAPE ?7 " //
                + "AND FKTABLE_SCHEMA LIKE ?5 ESCAPE ?7 " //
                + "AND FKTABLE_NAME = ?6 " //
                + "ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, FK_NAME, KEY_SEQ", //
                getCatalogPattern(primaryCatalogPattern), //
                getSchemaPattern(primarySchemaPattern), //
                getString(primaryTable), //
                getCatalogPattern(foreignCatalogPattern), //
                getSchemaPattern(foreignSchemaPattern), //
                getString(foreignTable), //
                BACKSLASH);
    }

    @Override
    public ResultInterface getTypeInfo() {
        return executeQuery("SELECT " //
                + "TYPE_NAME, " //
                + "DATA_TYPE, " //
                + "PRECISION, " //
                + "PREFIX LITERAL_PREFIX, " //
                + "SUFFIX LITERAL_SUFFIX, " //
                + "PARAMS CREATE_PARAMS, " //
                + "NULLABLE, " //
                + "CASE_SENSITIVE, " //
                + "SEARCHABLE, " //
                + "FALSE UNSIGNED_ATTRIBUTE, " //
                + "FALSE FIXED_PREC_SCALE, " //
                + "AUTO_INCREMENT, " //
                + "TYPE_NAME LOCAL_TYPE_NAME, " //
                + "MINIMUM_SCALE, " //
                + "MAXIMUM_SCALE, " //
                + "DATA_TYPE SQL_DATA_TYPE, " //
                + "ZERO() SQL_DATETIME_SUB, " //
                + "RADIX NUM_PREC_RADIX " //
                + "FROM INFORMATION_SCHEMA.TYPE_INFO " //
                + "ORDER BY DATA_TYPE, POS");
    }

    @Override
    public ResultInterface getIndexInfo(String catalogPattern, String schemaPattern, String tableName, boolean unique,
            boolean approximate) {
        String uniqueCondition = unique ? "NON_UNIQUE=FALSE" : "TRUE";
        return executeQuery("SELECT " //
                + "TABLE_CATALOG TABLE_CAT, " //
                + "TABLE_SCHEMA TABLE_SCHEM, " //
                + "TABLE_NAME, " //
                + "NON_UNIQUE, " //
                + "TABLE_CATALOG INDEX_QUALIFIER, " //
                + "INDEX_NAME, " //
                + "INDEX_TYPE TYPE, " //
                + "ORDINAL_POSITION, " //
                + "COLUMN_NAME, " //
                + "ASC_OR_DESC, " //
                // TODO meta data for number of unique values in an index
                + "CARDINALITY, " //
                + "PAGES, " //
                + "FILTER_CONDITION, " //
                + "SORT_TYPE " //
                + "FROM INFORMATION_SCHEMA.INDEXES " //
                + "WHERE TABLE_CATALOG LIKE ?1 ESCAPE ?4 " //
                + "AND TABLE_SCHEMA LIKE ?2 ESCAPE ?4 " //
                + "AND (" + uniqueCondition + ") " //
                + "AND TABLE_NAME = ?3 " //
                + "ORDER BY NON_UNIQUE, TYPE, TABLE_SCHEM, INDEX_NAME, ORDINAL_POSITION", //
                getCatalogPattern(catalogPattern), //
                getSchemaPattern(schemaPattern), //
                getString(tableName), //
                BACKSLASH);
    }

    @Override
    public ResultInterface getSchemas(String catalogPattern, String schemaPattern) {
        checkClosed();
        SimpleResult result = new SimpleResult();
        result.addColumn("TABLE_SCHEM", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TABLE_CATALOG", TypeInfo.TYPE_VARCHAR);
        if (!checkCatalog(catalogPattern)) {
            return result;
        }
        CompareLike schemaLike = getLike(schemaPattern);
        Collection<Schema> allSchemas = session.getDatabase().getAllSchemas();
        ArrayList<String> list;
        if (schemaLike == null) {
            list = new ArrayList<>(allSchemas.size());
            for (Schema s : allSchemas) {
                list.add(s.getName());
            }
        } else {
            list = Utils.newSmallArrayList();
            for (Schema s : allSchemas) {
                String name = s.getName();
                if (schemaLike.test(name)) {
                    list.add(name);
                }
            }
        }
        list.sort(getComparator());
        Value c = getString(session.getDatabase().getShortName());
        for (String s : list) {
            result.addRow(getString(s), c);
        }
        return result;
    }

    private ResultInterface executeQuery(String sql, Value... args) {
        checkClosed();
        synchronized (session) {
            CommandInterface command = session.prepareCommand(sql, Integer.MAX_VALUE);
            int l = args.length;
            if (l > 0) {
                ArrayList<? extends ParameterInterface> parameters = command.getParameters();
                for (int i = 0; i < l; i++) {
                    parameters.get(i).setValue(args[i], true);
                }
            }
            boolean lazy = session.isLazyQueryExecution();
            ResultInterface result;
            try {
                session.setLazyQueryExecution(false);
                result = command.executeQuery(0, false);
                command.close();
            } finally {
                session.setLazyQueryExecution(lazy);
            }
            return result;
        }
    }

    @Override
    void checkClosed() {
        if (session.isClosed()) {
            throw DbException.get(ErrorCode.DATABASE_CALLED_AT_SHUTDOWN);
        }
    }

    private Comparator<String> getComparator() {
        Comparator<String> comparator = this.comparator;
        if (comparator == null) {
            Database db = session.getDatabase();
            comparator = new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    return db.getCompareMode().compareString(o1, o2, false);
                }
            };
            this.comparator = comparator;
        }
        return comparator;
    }

    private Value getString(String string) {
        return string != null ? ValueVarchar.get(string, session) : ValueNull.INSTANCE;
    }

    private Value getPattern(String pattern) {
        return pattern == null ? PERCENT : getString(pattern);
    }

    private Value getSchemaPattern(String pattern) {
        return pattern == null ? PERCENT : pattern.isEmpty() ? SCHEMA_MAIN : getString(pattern);
    }

    private boolean checkCatalog(String catalogPattern) {
        if (catalogPattern != null && !catalogPattern.isEmpty()) {
            return getLike().test(catalogPattern, session.getDatabase().getShortName(), '\\');
        }
        return true;
    }

    private CompareLike getLike(String pattern) {
        if (pattern == null) {
            return null;
        }
        CompareLike like = getLike();
        like.initPattern(pattern, '\\');
        return like;
    }

    private CompareLike getLike() {
        return new CompareLike(session.getDatabase().getCompareMode(), "\\", null, false, false, null, null,
                CompareLike.LikeType.LIKE);
    }

    private Value getCatalogPattern(String catalogPattern) {
        return catalogPattern == null || catalogPattern.isEmpty() ? PERCENT : getString(catalogPattern);
    }

}
