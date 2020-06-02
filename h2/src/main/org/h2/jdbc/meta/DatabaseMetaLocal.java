/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc.meta;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.command.dml.Help;
import org.h2.constraint.Constraint;
import org.h2.constraint.ConstraintActionType;
import org.h2.constraint.ConstraintReferential;
import org.h2.constraint.ConstraintUnique;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.ParameterInterface;
import org.h2.expression.condition.CompareLike;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.SimpleResult;
import org.h2.result.SortOrder;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;
import org.h2.value.ValueSmallint;
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

    private static final ValueSmallint BEST_ROW_SESSION = ValueSmallint.get((short) DatabaseMetaData.bestRowSession);

    private static final ValueSmallint BEST_ROW_NOT_PSEUDO = ValueSmallint
            .get((short) DatabaseMetaData.bestRowNotPseudo);

    private static final ValueSmallint IMPORTED_KEY_CASCADE = ValueSmallint
            .get((short) DatabaseMetaData.importedKeyCascade);

    private static final ValueSmallint IMPORTED_KEY_RESTRICT = ValueSmallint
            .get((short) DatabaseMetaData.importedKeyRestrict);

    private static final ValueSmallint IMPORTED_KEY_DEFAULT = ValueSmallint
            .get((short) DatabaseMetaData.importedKeySetDefault);

    private static final ValueSmallint IMPORTED_KEY_SET_NULL = ValueSmallint
            .get((short) DatabaseMetaData.importedKeySetNull);

    private static final ValueSmallint IMPORTED_KEY_NOT_DEFERRABLE = ValueSmallint
            .get((short) DatabaseMetaData.importedKeyNotDeferrable);

    private static final ValueSmallint TABLE_INDEX_STATISTIC = ValueSmallint.get(DatabaseMetaData.tableIndexStatistic);

    private static final ValueSmallint TABLE_INDEX_HASHED = ValueSmallint.get(DatabaseMetaData.tableIndexHashed);

    private static final ValueSmallint TABLE_INDEX_OTHER = ValueSmallint.get(DatabaseMetaData.tableIndexOther);

    private static final ValueSmallint TYPE_NULLABLE = ValueSmallint.get((short) DatabaseMetaData.typeNullable);

    private static final ValueSmallint TYPE_SEARCHABLE = ValueSmallint.get((short) DatabaseMetaData.typeSearchable);

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
        checkClosed();
        StringBuilder builder = new StringBuilder();
        try {
            ResultSet rs = Help.getTable();
            while (rs.next()) {
                if (rs.getString(1).trim().equals(section)) {
                    if (builder.length() != 0) {
                        builder.append(',');
                    }
                    String f = rs.getString(2).trim();
                    int spaceIndex = f.indexOf(' ');
                    if (spaceIndex >= 0) {
                        // remove 'Function' from 'INSERT Function'
                        StringUtils.trimSubstring(builder, f, 0, spaceIndex);
                    } else {
                        builder.append(f);
                    }
                }
            }
        } catch (Exception e) {
            throw DbException.convert(e);
        }
        return builder.toString();
    }

    @Override
    public String getSearchStringEscape() {
        return session.getDatabase().getSettings().defaultEscape;
    }

    @Override
    public ResultInterface getProcedures(String catalog, String schemaPattern, String procedureNamePattern) {
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
                getCatalogPattern(catalog), //
                getSchemaPattern(schemaPattern), //
                getPattern(procedureNamePattern), //
                BACKSLASH);
    }

    @Override
    public ResultInterface getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
            String columnNamePattern) {
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
                getCatalogPattern(catalog), //
                getSchemaPattern(schemaPattern), //
                getPattern(procedureNamePattern), //
                getPattern(columnNamePattern), //
                BACKSLASH);
    }

    @Override
    public ResultInterface getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) {
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
        args[0] = getCatalogPattern(catalog);
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
    public ResultInterface getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) {
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
                getCatalogPattern(catalog), //
                getSchemaPattern(schema), //
                getString(table), //
                getPattern(columnNamePattern), //
                BACKSLASH);
    }

    @Override
    public ResultInterface getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) {
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
                getCatalogPattern(catalog), //
                getSchemaPattern(schemaPattern), //
                getPattern(tableNamePattern), //
                BACKSLASH);
    }

    @Override
    public ResultInterface getBestRowIdentifier(String catalog, String schema, String table, int scope,
            boolean nullable) {
        if (table == null) {
            throw DbException.getInvalidValueException("table", null);
        }
        checkClosed();
        SimpleResult result = new SimpleResult();
        result.addColumn("SCOPE", TypeInfo.TYPE_SMALLINT);
        result.addColumn("COLUMN_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("DATA_TYPE", TypeInfo.TYPE_INTEGER);
        result.addColumn("TYPE_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("COLUMN_SIZE", TypeInfo.TYPE_INTEGER);
        result.addColumn("BUFFER_LENGTH", TypeInfo.TYPE_INTEGER);
        result.addColumn("DECIMAL_DIGITS", TypeInfo.TYPE_SMALLINT);
        result.addColumn("PSEUDO_COLUMN", TypeInfo.TYPE_SMALLINT);
        if (!checkCatalogName(catalog)) {
            return result;
        }
        for (Schema s : getSchemas(schema)) {
            Table t = s.findTableOrView(session, table);
            if (t == null || t.isHidden()) {
                continue;
            }
            ArrayList<Constraint> constraints = t.getConstraints();
            if (constraints == null) {
                continue;
            }
            for (Constraint constraint : constraints) {
                if (constraint.getConstraintType() != Constraint.Type.PRIMARY_KEY) {
                    continue;
                }
                IndexColumn[] columns = ((ConstraintUnique) constraint).getColumns();
                for (int i = 0, l = columns.length; i < l; i++) {
                    IndexColumn ic = columns[i];
                    Column c = ic.column;
                    TypeInfo type = c.getType();
                    DataType dt = DataType.getDataType(type.getValueType());
                    ValueInteger precision = ValueInteger.get(MathUtils.convertLongToInt(type.getPrecision()));
                    result.addRow(
                            // SCOPE
                            BEST_ROW_SESSION,
                            // COLUMN_NAME
                            getString(c.getName()),
                            // DATA_TYPE
                            ValueInteger.get(dt.sqlType),
                            // TYPE_NAME
                            getString(dt.name),
                            // COLUMN_SIZE
                            precision,
                            // BUFFER_LENGTH
                            precision,
                            // DECIMAL_DIGITS
                            dt.supportsScale ? ValueSmallint.get(MathUtils.convertIntToShort(type.getScale()))
                                    : ValueNull.INSTANCE,
                            // PSEUDO_COLUMN
                            BEST_ROW_NOT_PSEUDO);
                }
            }
        }
        return result;
    }

    @Override
    public ResultInterface getPrimaryKeys(String catalog, String schema, String table) {
        if (table == null) {
            throw DbException.getInvalidValueException("table", null);
        }
        checkClosed();
        SimpleResult result = new SimpleResult();
        result.addColumn("TABLE_CAT", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TABLE_SCHEM", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TABLE_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("COLUMN_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("KEY_SEQ", TypeInfo.TYPE_SMALLINT);
        result.addColumn("PK_NAME", TypeInfo.TYPE_VARCHAR);
        if (!checkCatalogName(catalog)) {
            return result;
        }
        Database db = session.getDatabase();
        Value catalogValue = getString(db.getShortName());
        for (Schema s : getSchemas(schema)) {
            Table t = s.findTableOrView(session, table);
            if (t == null || t.isHidden()) {
                continue;
            }
            ArrayList<Constraint> constraints = t.getConstraints();
            if (constraints == null) {
                continue;
            }
            for (Constraint constraint : constraints) {
                if (constraint.getConstraintType() != Constraint.Type.PRIMARY_KEY) {
                    continue;
                }
                Value schemaValue = getString(s.getName());
                Value tableValue = getString(t.getName());
                Value pkValue = getString(constraint.getName());
                IndexColumn[] columns = ((ConstraintUnique) constraint).getColumns();
                for (int i = 0, l = columns.length; i < l;) {
                    result.addRow(
                            // TABLE_CAT
                            catalogValue,
                            // TABLE_SCHEM
                            schemaValue,
                            // TABLE_NAME
                            tableValue,
                            // COLUMN_NAME
                            getString(columns[i].column.getName()),
                            // KEY_SEQ
                            ValueSmallint.get((short) ++i),
                            // PK_NAME
                            pkValue);
                }
            }
        }
        result.sortRows(new SortOrder(session, new int[] { 3 }, new int[1], null));
        return result;
    }

    @Override
    public ResultInterface getImportedKeys(String catalog, String schema, String table) {
        if (table == null) {
            throw DbException.getInvalidValueException("table", null);
        }
        SimpleResult result = initCrossReferenceResult();
        if (!checkCatalogName(catalog)) {
            return result;
        }
        Database db = session.getDatabase();
        Value catalogValue = getString(db.getShortName());
        for (Schema s : getSchemas(schema)) {
            Table t = s.findTableOrView(session, table);
            if (t == null || t.isHidden()) {
                continue;
            }
            ArrayList<Constraint> constraints = t.getConstraints();
            if (constraints == null) {
                continue;
            }
            for (Constraint constraint : constraints) {
                if (constraint.getConstraintType() != Constraint.Type.REFERENTIAL) {
                    continue;
                }
                ConstraintReferential fk = (ConstraintReferential) constraint;
                Table fkTable = fk.getTable();
                if (fkTable != t) {
                    continue;
                }
                Table pkTable = fk.getRefTable();
                addCrossReferenceResult(result, catalogValue, pkTable.getSchema().getName(), pkTable,
                        fkTable.getSchema().getName(), fkTable, fk);
            }
        }
        return sortCrossReferenceResult(result);
    }

    @Override
    public ResultInterface getExportedKeys(String catalog, String schema, String table) {
        if (table == null) {
            throw DbException.getInvalidValueException("table", null);
        }
        SimpleResult result = initCrossReferenceResult();
        if (!checkCatalogName(catalog)) {
            return result;
        }
        Database db = session.getDatabase();
        Value catalogValue = getString(db.getShortName());
        for (Schema s : getSchemas(schema)) {
            Table t = s.findTableOrView(session, table);
            if (t == null || t.isHidden()) {
                continue;
            }
            ArrayList<Constraint> constraints = t.getConstraints();
            if (constraints == null) {
                continue;
            }
            for (Constraint constraint : constraints) {
                if (constraint.getConstraintType() != Constraint.Type.REFERENTIAL) {
                    continue;
                }
                ConstraintReferential fk = (ConstraintReferential) constraint;
                Table pkTable = fk.getRefTable();
                if (pkTable != t) {
                    continue;
                }
                Table fkTable = fk.getTable();
                addCrossReferenceResult(result, catalogValue, pkTable.getSchema().getName(), pkTable,
                        fkTable.getSchema().getName(), fkTable, fk);
            }
        }
        return sortCrossReferenceResult(result);
    }

    @Override
    public ResultInterface getCrossReference(String primaryCatalog, String primarySchema, String primaryTable,
            String foreignCatalog, String foreignSchema, String foreignTable) {
        if (primaryTable == null) {
            throw DbException.getInvalidValueException("primaryTable", null);
        }
        if (foreignTable == null) {
            throw DbException.getInvalidValueException("foreignTable", null);
        }
        SimpleResult result = initCrossReferenceResult();
        if (!checkCatalogName(primaryCatalog) || !checkCatalogName(foreignCatalog)) {
            return result;
        }
        Database db = session.getDatabase();
        Value catalogValue = getString(db.getShortName());
        for (Schema s : getSchemas(foreignSchema)) {
            Table t = s.findTableOrView(session, foreignTable);
            if (t == null || t.isHidden()) {
                continue;
            }
            ArrayList<Constraint> constraints = t.getConstraints();
            if (constraints == null) {
                continue;
            }
            for (Constraint constraint : constraints) {
                if (constraint.getConstraintType() != Constraint.Type.REFERENTIAL) {
                    continue;
                }
                ConstraintReferential fk = (ConstraintReferential) constraint;
                Table fkTable = fk.getTable();
                if (fkTable != t) {
                    continue;
                }
                Table pkTable = fk.getRefTable();
                if (!db.equalsIdentifiers(pkTable.getName(), primaryTable)) {
                    continue;
                }
                Schema pkSchema = pkTable.getSchema();
                if (!checkSchema(primarySchema, pkSchema)) {
                    continue;
                }
                addCrossReferenceResult(result, catalogValue, pkSchema.getName(), pkTable,
                        fkTable.getSchema().getName(), fkTable, fk);
            }
        }
        return sortCrossReferenceResult(result);
    }

    private SimpleResult initCrossReferenceResult() {
        checkClosed();
        SimpleResult result = new SimpleResult();
        result.addColumn("PKTABLE_CAT", TypeInfo.TYPE_VARCHAR);
        result.addColumn("PKTABLE_SCHEM", TypeInfo.TYPE_VARCHAR);
        result.addColumn("PKTABLE_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("PKCOLUMN_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("FKTABLE_CAT", TypeInfo.TYPE_VARCHAR);
        result.addColumn("FKTABLE_SCHEM", TypeInfo.TYPE_VARCHAR);
        result.addColumn("FKTABLE_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("FKCOLUMN_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("KEY_SEQ", TypeInfo.TYPE_SMALLINT);
        result.addColumn("UPDATE_RULE", TypeInfo.TYPE_SMALLINT);
        result.addColumn("DELETE_RULE", TypeInfo.TYPE_SMALLINT);
        result.addColumn("FK_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("PK_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("DEFERRABILITY", TypeInfo.TYPE_SMALLINT);
        return result;
    }

    private void addCrossReferenceResult(SimpleResult result, Value catalog, String pkSchema, Table pkTable,
            String fkSchema, Table fkTable, ConstraintReferential fk) {
        Value pkSchemaValue = getString(pkSchema);
        Value pkTableValue = getString(pkTable.getName());
        Value fkSchemaValue = getString(fkSchema);
        Value fkTableValue = getString(fkTable.getName());
        IndexColumn[] pkCols = fk.getRefColumns();
        IndexColumn[] fkCols = fk.getColumns();
        Value update = getRefAction(fk.getUpdateAction());
        Value delete = getRefAction(fk.getDeleteAction());
        Value fkNameValue = getString(fk.getName());
        Value pkNameValue = getString(fk.getReferencedConstraint().getName());
        for (int j = 0, len = fkCols.length; j < len; j++) {
            result.addRow(
                    // PKTABLE_CAT
                    catalog,
                    // PKTABLE_SCHEM
                    pkSchemaValue,
                    // PKTABLE_NAME
                    pkTableValue,
                    // PKCOLUMN_NAME
                    getString(pkCols[j].column.getName()),
                    // FKTABLE_CAT
                    catalog,
                    // FKTABLE_SCHEM
                    fkSchemaValue,
                    // FKTABLE_NAME
                    fkTableValue,
                    // FKCOLUMN_NAME
                    getString(fkCols[j].column.getName()),
                    // KEY_SEQ
                    ValueSmallint.get((short) (j + 1)),
                    // UPDATE_RULE
                    update,
                    // DELETE_RULE
                    delete,
                    // FK_NAME
                    fkNameValue,
                    // PK_NAME
                    pkNameValue,
                    // DEFERRABILITY
                    IMPORTED_KEY_NOT_DEFERRABLE);
        }
    }

    private static ValueSmallint getRefAction(ConstraintActionType action) {
        switch (action) {
        case CASCADE:
            return IMPORTED_KEY_CASCADE;
        case RESTRICT:
            return IMPORTED_KEY_RESTRICT;
        case SET_DEFAULT:
            return IMPORTED_KEY_DEFAULT;
        case SET_NULL:
            return IMPORTED_KEY_SET_NULL;
        default:
            throw DbException.throwInternalError("action=" + action);
        }
    }

    private ResultInterface sortCrossReferenceResult(SimpleResult result) {
        result.sortRows(new SortOrder(session, new int[] { 4, 5, 6, 8 }, new int[4], null));
        return result;
    }

    @Override
    public ResultInterface getTypeInfo() {
        checkClosed();
        SimpleResult result = new SimpleResult();
        result.addColumn("TYPE_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("DATA_TYPE", TypeInfo.TYPE_INTEGER);
        result.addColumn("PRECISION", TypeInfo.TYPE_INTEGER);
        result.addColumn("LITERAL_PREFIX", TypeInfo.TYPE_VARCHAR);
        result.addColumn("LITERAL_SUFFIX", TypeInfo.TYPE_VARCHAR);
        result.addColumn("CREATE_PARAMS", TypeInfo.TYPE_VARCHAR);
        result.addColumn("NULLABLE", TypeInfo.TYPE_SMALLINT);
        result.addColumn("CASE_SENSITIVE", TypeInfo.TYPE_BOOLEAN);
        result.addColumn("SEARCHABLE", TypeInfo.TYPE_SMALLINT);
        result.addColumn("UNSIGNED_ATTRIBUTE", TypeInfo.TYPE_BOOLEAN);
        result.addColumn("FIXED_PREC_SCALE", TypeInfo.TYPE_BOOLEAN);
        result.addColumn("AUTO_INCREMENT", TypeInfo.TYPE_BOOLEAN);
        result.addColumn("LOCAL_TYPE_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("MINIMUM_SCALE", TypeInfo.TYPE_SMALLINT);
        result.addColumn("MAXIMUM_SCALE", TypeInfo.TYPE_SMALLINT);
        result.addColumn("SQL_DATA_TYPE", TypeInfo.TYPE_INTEGER);
        result.addColumn("SQL_DATETIME_SUB", TypeInfo.TYPE_INTEGER);
        result.addColumn("NUM_PREC_RADIX", TypeInfo.TYPE_INTEGER);
        for (DataType t : DataType.getTypes()) {
            if (t.hidden) {
                continue;
            }
            Value name = getString(t.name);
            ValueInteger sqlType = ValueInteger.get(t.sqlType);
            result.addRow(
                    // TYPE_NAME
                    name,
                    // DATA_TYPE
                    sqlType,
                    // PRECISION
                    ValueInteger.get(MathUtils.convertLongToInt(t.maxPrecision)),
                    // LITERAL_PREFIX
                    getString(t.prefix),
                    // LITERAL_SUFFIX
                    getString(t.suffix),
                    // CREATE_PARAMS
                    getString(t.params),
                    // NULLABLE
                    TYPE_NULLABLE,
                    // CASE_SENSITIVE
                    ValueBoolean.get(t.caseSensitive),
                    // SEARCHABLE
                    TYPE_SEARCHABLE,
                    // UNSIGNED_ATTRIBUTE
                    ValueBoolean.FALSE,
                    // FIXED_PREC_SCALE
                    ValueBoolean.get(t.type == Value.NUMERIC),
                    // AUTO_INCREMENT
                    ValueBoolean.get(t.autoIncrement),
                    // LOCAL_TYPE_NAME
                    name,
                    // MINIMUM_SCALE
                    ValueSmallint.get(MathUtils.convertIntToShort(t.minScale)),
                    // MAXIMUM_SCALE
                    ValueSmallint.get(MathUtils.convertIntToShort(t.maxScale)),
                    // SQL_DATA_TYPE
                    sqlType,
                    // SQL_DATETIME_SUB
                    ValueInteger.get(0),
                    // NUM_PREC_RADIX
                    t.decimal ? ValueInteger.get(10) : ValueNull.INSTANCE);
        }
        return result;
    }

    @Override
    public ResultInterface getIndexInfo(String catalog, String schema, String table, boolean unique,
            boolean approximate) {
        if (table == null) {
            throw DbException.getInvalidValueException("table", null);
        }
        checkClosed();
        SimpleResult result = new SimpleResult();
        result.addColumn("TABLE_CAT", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TABLE_SCHEM", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TABLE_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("NON_UNIQUE", TypeInfo.TYPE_BOOLEAN);
        result.addColumn("INDEX_QUALIFIER", TypeInfo.TYPE_VARCHAR);
        result.addColumn("INDEX_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TYPE", TypeInfo.TYPE_SMALLINT);
        result.addColumn("ORDINAL_POSITION", TypeInfo.TYPE_SMALLINT);
        result.addColumn("COLUMN_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("ASC_OR_DESC", TypeInfo.TYPE_VARCHAR);
        result.addColumn("CARDINALITY", TypeInfo.TYPE_BIGINT);
        result.addColumn("PAGES", TypeInfo.TYPE_BIGINT);
        result.addColumn("FILTER_CONDITION", TypeInfo.TYPE_VARCHAR);
        if (!checkCatalogName(catalog)) {
            return result;
        }
        Database db = session.getDatabase();
        Value catalogValue = getString(db.getShortName());
        for (Schema s : getSchemas(schema)) {
            Table t = s.findTableOrView(session, table);
            if (t == null || t.isHidden()) {
                continue;
            }
            getIndexInfo(catalogValue, getString(s.getName()), t, unique, approximate, result, db);
        }
        result.sortRows(new SortOrder(session, new int[] { 3, 6, 5, 7 }, new int[4], null));
        return result;
    }

    private void getIndexInfo(Value catalogValue, Value schemaValue, Table table, boolean unique, boolean approximate,
            SimpleResult result, Database db) {
        for (Index index : table.getIndexes()) {
            if (index.getCreateSQL() == null) {
                continue;
            }
            IndexType indexType = index.getIndexType();
            boolean isUnique = indexType.isUnique();
            if (unique && !isUnique) {
                continue;
            }
            Value tableValue = getString(table.getName());
            Value indexValue = getString(index.getName());
            ValueBoolean nonUnique = ValueBoolean.get(!isUnique);
            IndexColumn[] cols = index.getIndexColumns();
            ValueSmallint type = TABLE_INDEX_STATISTIC;
            type: if (isUnique) {
                for (IndexColumn c : cols) {
                    if (c.column.isNullable()) {
                        break type;
                    }
                }
                type = indexType.isHash() ? TABLE_INDEX_HASHED : TABLE_INDEX_OTHER;
            }
            for (int i = 0, l = cols.length; i < l; i++) {
                IndexColumn c = cols[i];
                result.addRow(
                        // TABLE_CAT
                        catalogValue,
                        // TABLE_SCHEM
                        schemaValue,
                        // TABLE_NAME
                        tableValue,
                        // NON_UNIQUE
                        nonUnique,
                        // INDEX_QUALIFIER
                        catalogValue,
                        // INDEX_NAME
                        indexValue,
                        // TYPE
                        type,
                        // ORDINAL_POSITION
                        ValueSmallint.get((short) (i + 1)),
                        // COLUMN_NAME
                        getString(c.column.getName()),
                        // ASC_OR_DESC
                        getString((c.sortType & SortOrder.DESCENDING) != 0 ? "D" : "A"),
                        // CARDINALITY
                        ValueBigint.get(approximate ? index.getRowCountApproximation() : index.getRowCount(session)),
                        // PAGES
                        ValueBigint.get(index.getDiskSpaceUsed() / db.getPageSize()),
                        // FILTER_CONDITION
                        ValueNull.INSTANCE);
            }
        }
    }

    @Override
    public ResultInterface getSchemas(String catalog, String schemaPattern) {
        checkClosed();
        SimpleResult result = new SimpleResult();
        result.addColumn("TABLE_SCHEM", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TABLE_CATALOG", TypeInfo.TYPE_VARCHAR);
        if (!checkCatalogName(catalog)) {
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
            this.comparator = comparator = (o1, o2) -> db.getCompareMode().compareString(o1, o2, false);
        }
        return comparator;
    }

    Value getString(String string) {
        return string != null ? ValueVarchar.get(string, session) : ValueNull.INSTANCE;
    }

    private Value getPattern(String pattern) {
        return pattern == null ? PERCENT : getString(pattern);
    }

    private Value getSchemaPattern(String pattern) {
        return pattern == null ? PERCENT : pattern.isEmpty() ? SCHEMA_MAIN : getString(pattern);
    }

    private boolean checkCatalogName(String catalog) {
        if (catalog != null && !catalog.isEmpty()) {
            Database db = session.getDatabase();
            return db.equalsIdentifiers(catalog, db.getShortName());
        }
        return true;
    }

    private Collection<Schema> getSchemas(String schema) {
        Database db = session.getDatabase();
        if (schema == null) {
            return db.getAllSchemas();
        } else if (schema.isEmpty()) {
            return Collections.singleton(db.getMainSchema());
        } else {
            Schema s = db.findSchema(schema);
            if (s != null) {
                return Collections.singleton(s);
            }
            return Collections.emptySet();
        }
    }

    private boolean checkSchema(String schemaName, Schema schema) {
        if (schemaName == null) {
            return true;
        } else if (schemaName.isEmpty()) {
            return schema == session.getDatabase().getMainSchema();
        } else {
            return session.getDatabase().equalsIdentifiers(schemaName, schema.getName());
        }
    }

    private CompareLike getLike(String pattern) {
        if (pattern == null) {
            return null;
        }
        CompareLike like = new CompareLike(session.getDatabase().getCompareMode(), "\\", null, false, false, null, //
                null, CompareLike.LikeType.LIKE);
        like.initPattern(pattern, '\\');
        return like;
    }

    private Value getCatalogPattern(String catalogPattern) {
        return catalogPattern == null || catalogPattern.isEmpty() ? PERCENT : getString(catalogPattern);
    }

}
