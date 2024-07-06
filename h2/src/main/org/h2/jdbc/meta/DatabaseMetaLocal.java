/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
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
import java.util.HashSet;

import org.h2.api.ErrorCode;
import org.h2.command.dml.Help;
import org.h2.constraint.Constraint;
import org.h2.constraint.ConstraintActionType;
import org.h2.constraint.ConstraintReferential;
import org.h2.constraint.ConstraintUnique;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Mode;
import org.h2.engine.Right;
import org.h2.engine.SessionLocal;
import org.h2.engine.User;
import org.h2.expression.condition.CompareLike;
import org.h2.index.Index;
import org.h2.message.DbException;
import org.h2.mode.DefaultNullOrdering;
import org.h2.result.ResultInterface;
import org.h2.result.SimpleResult;
import org.h2.result.SortOrder;
import org.h2.schema.FunctionAlias;
import org.h2.schema.FunctionAlias.JavaMethod;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObject;
import org.h2.schema.UserDefinedFunction;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableSynonym;
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
import org.h2.value.ValueToObjectConverter2;
import org.h2.value.ValueVarchar;

/**
 * Local implementation of database meta information.
 */
public final class DatabaseMetaLocal extends DatabaseMetaLocalBase {

    private static final Value YES = ValueVarchar.get("YES");

    private static final Value NO = ValueVarchar.get("NO");

    private static final ValueSmallint BEST_ROW_SESSION = ValueSmallint.get((short) DatabaseMetaData.bestRowSession);

    private static final ValueSmallint BEST_ROW_NOT_PSEUDO = ValueSmallint
            .get((short) DatabaseMetaData.bestRowNotPseudo);

    private static final ValueInteger COLUMN_NO_NULLS = ValueInteger.get(DatabaseMetaData.columnNoNulls);

    private static final ValueSmallint COLUMN_NO_NULLS_SMALL = ValueSmallint
            .get((short) DatabaseMetaData.columnNoNulls);

    private static final ValueInteger COLUMN_NULLABLE = ValueInteger.get(DatabaseMetaData.columnNullable);

    private static final ValueSmallint COLUMN_NULLABLE_UNKNOWN_SMALL = ValueSmallint
            .get((short) DatabaseMetaData.columnNullableUnknown);

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

    private static final ValueSmallint PROCEDURE_COLUMN_IN = ValueSmallint
            .get((short) DatabaseMetaData.procedureColumnIn);

    private static final ValueSmallint PROCEDURE_COLUMN_RETURN = ValueSmallint
            .get((short) DatabaseMetaData.procedureColumnReturn);

    private static final ValueSmallint PROCEDURE_NO_RESULT = ValueSmallint
            .get((short) DatabaseMetaData.procedureNoResult);

    private static final ValueSmallint PROCEDURE_RETURNS_RESULT = ValueSmallint
            .get((short) DatabaseMetaData.procedureReturnsResult);

    private static final ValueSmallint TABLE_INDEX_HASHED = ValueSmallint.get(DatabaseMetaData.tableIndexHashed);

    private static final ValueSmallint TABLE_INDEX_OTHER = ValueSmallint.get(DatabaseMetaData.tableIndexOther);

    // This list must be ordered
    private static final String[] TABLE_TYPES = { "BASE TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "SYNONYM",
            "VIEW" };

    private static final ValueSmallint TYPE_NULLABLE = ValueSmallint.get((short) DatabaseMetaData.typeNullable);

    private static final ValueSmallint TYPE_SEARCHABLE = ValueSmallint.get((short) DatabaseMetaData.typeSearchable);

    private static final Value NO_USAGE_RESTRICTIONS = ValueVarchar.get("NO_USAGE_RESTRICTIONS");

    private final SessionLocal session;

    public DatabaseMetaLocal(SessionLocal session) {
        this.session = session;
    }

    @Override
    public final DefaultNullOrdering defaultNullOrdering() {
        return session.getDatabase().getDefaultNullOrdering();
    }

    @Override
    public String getSQLKeywords() {
        StringBuilder builder = new StringBuilder(103).append( //
                "CURRENT_CATALOG," //
                        + "CURRENT_SCHEMA," //
                        + "GROUPS," //
                        + "IF,ILIKE," //
                        + "KEY,");
        Mode mode = session.getMode();
        if (mode.limit) {
            builder.append("LIMIT,");
        }
        if (mode.minusIsExcept) {
            builder.append("MINUS,");
        }
        builder.append( //
                "OFFSET," //
                        + "QUALIFY," //
                        + "REGEXP,ROWNUM,");
        if (mode.topInSelect || mode.topInDML) {
            builder.append("TOP,");
        }
        return builder.append("_ROWID_") //
                .toString();
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
                    String topic = rs.getString(2).trim();
                    int spaceIndex = topic.indexOf(' ');
                    if (spaceIndex >= 0) {
                        // remove 'Function' from 'INSERT Function'
                        StringUtils.trimSubstring(builder, topic, 0, spaceIndex);
                    } else {
                        builder.append(topic);
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
        checkClosed();
        SimpleResult result = new SimpleResult();
        result.addColumn("PROCEDURE_CAT", TypeInfo.TYPE_VARCHAR);
        result.addColumn("PROCEDURE_SCHEM", TypeInfo.TYPE_VARCHAR);
        result.addColumn("PROCEDURE_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("RESERVED1", TypeInfo.TYPE_NULL);
        result.addColumn("RESERVED2", TypeInfo.TYPE_NULL);
        result.addColumn("RESERVED3", TypeInfo.TYPE_NULL);
        result.addColumn("REMARKS", TypeInfo.TYPE_VARCHAR);
        result.addColumn("PROCEDURE_TYPE", TypeInfo.TYPE_SMALLINT);
        result.addColumn("SPECIFIC_NAME", TypeInfo.TYPE_VARCHAR);
        if (!checkCatalogName(catalog)) {
            return result;
        }
        Database db = session.getDatabase();
        Value catalogValue = getString(db.getShortName());
        CompareLike procedureLike = getLike(procedureNamePattern);
        for (Schema s : getSchemasForPattern(schemaPattern)) {
            Value schemaValue = getString(s.getName());
            for (UserDefinedFunction userDefinedFunction : s.getAllFunctionsAndAggregates()) {
                String procedureName = userDefinedFunction.getName();
                if (procedureLike != null && !procedureLike.test(procedureName)) {
                    continue;
                }
                Value procedureNameValue = getString(procedureName);
                if (userDefinedFunction instanceof FunctionAlias) {
                    JavaMethod[] methods;
                    try {
                        methods = ((FunctionAlias) userDefinedFunction).getJavaMethods();
                    } catch (DbException e) {
                        continue;
                    }
                    for (int i = 0; i < methods.length; i++) {
                        JavaMethod method = methods[i];
                        TypeInfo typeInfo = method.getDataType();
                        getProceduresAdd(result, catalogValue, schemaValue, procedureNameValue,
                                userDefinedFunction.getComment(),
                                typeInfo == null || typeInfo.getValueType() != Value.NULL ? PROCEDURE_RETURNS_RESULT
                                        : PROCEDURE_NO_RESULT,
                                getString(procedureName + '_' + (i + 1)));
                    }
                } else {
                    getProceduresAdd(result, catalogValue, schemaValue, procedureNameValue,
                            userDefinedFunction.getComment(), PROCEDURE_RETURNS_RESULT, procedureNameValue);
                }
            }
        }
        // PROCEDURE_CAT, PROCEDURE_SCHEM, PROCEDURE_NAME, SPECIFIC_ NAME
        result.sortRows(new SortOrder(session, new int[] { 1, 2, 8 }));
        return result;
    }

    private void getProceduresAdd(SimpleResult result, Value catalogValue, Value schemaValue, Value procedureNameValue,
            String comment, ValueSmallint procedureType, Value specificNameValue) {
        result.addRow(
                // PROCEDURE_CAT
                catalogValue,
                // PROCEDURE_SCHEM
                schemaValue,
                // PROCEDURE_NAME
                procedureNameValue,
                // RESERVED1
                ValueNull.INSTANCE,
                // RESERVED2
                ValueNull.INSTANCE,
                // RESERVED3
                ValueNull.INSTANCE,
                // REMARKS
                getString(comment),
                // PROCEDURE_TYPE
                procedureType,
                // SPECIFIC_NAME
                specificNameValue);
    }

    @Override
    public ResultInterface getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
            String columnNamePattern) {
        checkClosed();
        SimpleResult result = new SimpleResult();
        result.addColumn("PROCEDURE_CAT", TypeInfo.TYPE_VARCHAR);
        result.addColumn("PROCEDURE_SCHEM", TypeInfo.TYPE_VARCHAR);
        result.addColumn("PROCEDURE_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("COLUMN_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("COLUMN_TYPE", TypeInfo.TYPE_SMALLINT);
        result.addColumn("DATA_TYPE", TypeInfo.TYPE_INTEGER);
        result.addColumn("TYPE_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("PRECISION", TypeInfo.TYPE_INTEGER);
        result.addColumn("LENGTH", TypeInfo.TYPE_INTEGER);
        result.addColumn("SCALE", TypeInfo.TYPE_SMALLINT);
        result.addColumn("RADIX", TypeInfo.TYPE_SMALLINT);
        result.addColumn("NULLABLE", TypeInfo.TYPE_SMALLINT);
        result.addColumn("REMARKS", TypeInfo.TYPE_VARCHAR);
        result.addColumn("COLUMN_DEF", TypeInfo.TYPE_VARCHAR);
        result.addColumn("SQL_DATA_TYPE", TypeInfo.TYPE_INTEGER);
        result.addColumn("SQL_DATETIME_SUB", TypeInfo.TYPE_INTEGER);
        result.addColumn("CHAR_OCTET_LENGTH", TypeInfo.TYPE_INTEGER);
        result.addColumn("ORDINAL_POSITION", TypeInfo.TYPE_INTEGER);
        result.addColumn("IS_NULLABLE", TypeInfo.TYPE_VARCHAR);
        result.addColumn("SPECIFIC_NAME", TypeInfo.TYPE_VARCHAR);
        if (!checkCatalogName(catalog)) {
            return result;
        }
        Database db = session.getDatabase();
        Value catalogValue = getString(db.getShortName());
        CompareLike procedureLike = getLike(procedureNamePattern);
        for (Schema s : getSchemasForPattern(schemaPattern)) {
            Value schemaValue = getString(s.getName());
            for (UserDefinedFunction userDefinedFunction : s.getAllFunctionsAndAggregates()) {
                if (!(userDefinedFunction instanceof FunctionAlias)) {
                    continue;
                }
                String procedureName = userDefinedFunction.getName();
                if (procedureLike != null && !procedureLike.test(procedureName)) {
                    continue;
                }
                Value procedureNameValue = getString(procedureName);
                JavaMethod[] methods;
                try {
                    methods = ((FunctionAlias) userDefinedFunction).getJavaMethods();
                } catch (DbException e) {
                    continue;
                }
                for (int i = 0, l = methods.length; i < l; i++) {
                    JavaMethod method = methods[i];
                    Value specificNameValue = getString(procedureName + '_' + (i + 1));
                    TypeInfo typeInfo = method.getDataType();
                    if (typeInfo != null && typeInfo.getValueType() != Value.NULL) {
                        getProcedureColumnAdd(result, catalogValue, schemaValue, procedureNameValue, specificNameValue,
                                typeInfo, method.getClass().isPrimitive(), 0);
                    }
                    Class<?>[] columnList = method.getColumnClasses();
                    for (int o = 1, p = method.hasConnectionParam() ? 1 : 0, n = columnList.length; p < n; o++, p++) {
                        Class<?> clazz = columnList[p];
                        getProcedureColumnAdd(result, catalogValue, schemaValue, procedureNameValue, specificNameValue,
                                ValueToObjectConverter2.classToType(clazz), clazz.isPrimitive(), o);
                    }
                }
            }
        }
        // PROCEDURE_CAT, PROCEDURE_SCHEM, PROCEDURE_NAME, SPECIFIC_NAME, return
        // value first
        result.sortRows(new SortOrder(session, new int[] { 1, 2, 19 }));
        return result;
    }

    private void getProcedureColumnAdd(SimpleResult result, Value catalogValue, Value schemaValue,
            Value procedureNameValue, Value specificNameValue, TypeInfo type, boolean notNull, int ordinal) {
        int valueType = type.getValueType();
        DataType dt = DataType.getDataType(valueType);
        ValueInteger precisionValue = ValueInteger.get(MathUtils.convertLongToInt(type.getPrecision()));
        result.addRow(
                // PROCEDURE_CAT
                catalogValue,
                // PROCEDURE_SCHEM
                schemaValue,
                // PROCEDURE_NAME
                procedureNameValue,
                // COLUMN_NAME
                getString(ordinal == 0 ? "RESULT" : "P" + ordinal),
                // COLUMN_TYPE
                ordinal == 0 ? PROCEDURE_COLUMN_RETURN : PROCEDURE_COLUMN_IN,
                // DATA_TYPE
                ValueInteger.get(DataType.convertTypeToSQLType(type)),
                // TYPE_NAME
                getDataTypeName(type),
                // PRECISION
                precisionValue,
                // LENGTH
                precisionValue,
                // SCALE
                dt.supportsScale //
                        ? ValueSmallint.get(MathUtils.convertIntToShort(dt.defaultScale))
                        : ValueNull.INSTANCE,
                // RADIX
                getRadix(valueType, true),
                // NULLABLE
                notNull ? COLUMN_NO_NULLS_SMALL : COLUMN_NULLABLE_UNKNOWN_SMALL,
                // REMARKS
                ValueNull.INSTANCE,
                // COLUMN_DEF
                ValueNull.INSTANCE,
                // SQL_DATA_TYPE
                ValueNull.INSTANCE,
                // SQL_DATETIME_SUB
                ValueNull.INSTANCE,
                // CHAR_OCTET_LENGTH
                DataType.isBinaryStringType(valueType) || DataType.isCharacterStringType(valueType) ? precisionValue
                        : ValueNull.INSTANCE,
                // ORDINAL_POSITION
                ValueInteger.get(ordinal),
                // IS_NULLABLE
                ValueVarchar.EMPTY,
                // SPECIFIC_NAME
                specificNameValue);
    }

    @Override
    public ResultInterface getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) {
        SimpleResult result = new SimpleResult();
        result.addColumn("TABLE_CAT", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TABLE_SCHEM", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TABLE_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TABLE_TYPE", TypeInfo.TYPE_VARCHAR);
        result.addColumn("REMARKS", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TYPE_CAT", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TYPE_SCHEM", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TYPE_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("SELF_REFERENCING_COL_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("REF_GENERATION", TypeInfo.TYPE_VARCHAR);
        if (!checkCatalogName(catalog)) {
            return result;
        }
        Database db = session.getDatabase();
        Value catalogValue = getString(db.getShortName());
        HashSet<String> typesSet;
        if (types != null) {
            typesSet = new HashSet<>(8);
            for (String type : types) {
                int idx = Arrays.binarySearch(TABLE_TYPES, type);
                if (idx >= 0) {
                    typesSet.add(TABLE_TYPES[idx]);
                } else if (type.equals("TABLE")) {
                    typesSet.add("BASE TABLE");
                }
            }
            if (typesSet.isEmpty()) {
                return result;
            }
        } else {
            typesSet = null;
        }
        for (Schema schema : getSchemasForPattern(schemaPattern)) {
            Value schemaValue = getString(schema.getName());
            for (SchemaObject object : getTablesForPattern(schema, tableNamePattern)) {
                Value tableName = getString(object.getName());
                if (object instanceof Table) {
                    getTablesAdd(result, catalogValue, schemaValue, tableName, (Table) object, false, typesSet);
                } else {
                    getTablesAdd(result, catalogValue, schemaValue, tableName, ((TableSynonym) object).getSynonymFor(),
                            true, typesSet);
                }
            }
        }
        // TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME
        result.sortRows(new SortOrder(session, new int[] { 3, 1, 2 }));
        return result;
    }

    private void getTablesAdd(SimpleResult result, Value catalogValue, Value schemaValue, Value tableName, Table t,
            boolean synonym, HashSet<String> typesSet) {
        String type = synonym ? "SYNONYM" : t.getSQLTableType();
        if (typesSet != null && !typesSet.contains(type)) {
            return;
        }
        result.addRow(
                // TABLE_CAT
                catalogValue,
                // TABLE_SCHEM
                schemaValue,
                // TABLE_NAME
                tableName,
                // TABLE_TYPE
                getString(type),
                // REMARKS
                getString(t.getComment()),
                // TYPE_CAT
                ValueNull.INSTANCE,
                // TYPE_SCHEM
                ValueNull.INSTANCE,
                // TYPE_NAME
                ValueNull.INSTANCE,
                // SELF_REFERENCING_COL_NAME
                ValueNull.INSTANCE,
                // REF_GENERATION
                ValueNull.INSTANCE);
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
        SimpleResult result = new SimpleResult();
        result.addColumn("TABLE_TYPE", TypeInfo.TYPE_VARCHAR);
        // Order by TABLE_TYPE
        result.addRow(getString("BASE TABLE"));
        result.addRow(getString("GLOBAL TEMPORARY"));
        result.addRow(getString("LOCAL TEMPORARY"));
        result.addRow(getString("SYNONYM"));
        result.addRow(getString("VIEW"));
        return result;
    }

    @Override
    public ResultInterface getColumns(String catalog, String schemaPattern, String tableNamePattern,
            String columnNamePattern) {
        SimpleResult result = new SimpleResult();
        result.addColumn("TABLE_CAT", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TABLE_SCHEM", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TABLE_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("COLUMN_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("DATA_TYPE", TypeInfo.TYPE_INTEGER);
        result.addColumn("TYPE_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("COLUMN_SIZE", TypeInfo.TYPE_INTEGER);
        result.addColumn("BUFFER_LENGTH", TypeInfo.TYPE_INTEGER);
        result.addColumn("DECIMAL_DIGITS", TypeInfo.TYPE_INTEGER);
        result.addColumn("NUM_PREC_RADIX", TypeInfo.TYPE_INTEGER);
        result.addColumn("NULLABLE", TypeInfo.TYPE_INTEGER);
        result.addColumn("REMARKS", TypeInfo.TYPE_VARCHAR);
        result.addColumn("COLUMN_DEF", TypeInfo.TYPE_VARCHAR);
        result.addColumn("SQL_DATA_TYPE", TypeInfo.TYPE_INTEGER);
        result.addColumn("SQL_DATETIME_SUB", TypeInfo.TYPE_INTEGER);
        result.addColumn("CHAR_OCTET_LENGTH", TypeInfo.TYPE_INTEGER);
        result.addColumn("ORDINAL_POSITION", TypeInfo.TYPE_INTEGER);
        result.addColumn("IS_NULLABLE", TypeInfo.TYPE_VARCHAR);
        result.addColumn("SCOPE_CATALOG", TypeInfo.TYPE_VARCHAR);
        result.addColumn("SCOPE_SCHEMA", TypeInfo.TYPE_VARCHAR);
        result.addColumn("SCOPE_TABLE", TypeInfo.TYPE_VARCHAR);
        result.addColumn("SOURCE_DATA_TYPE", TypeInfo.TYPE_SMALLINT);
        result.addColumn("IS_AUTOINCREMENT", TypeInfo.TYPE_VARCHAR);
        result.addColumn("IS_GENERATEDCOLUMN", TypeInfo.TYPE_VARCHAR);
        if (!checkCatalogName(catalog)) {
            return result;
        }
        Database db = session.getDatabase();
        Value catalogValue = getString(db.getShortName());
        CompareLike columnLike = getLike(columnNamePattern);
        for (Schema schema : getSchemasForPattern(schemaPattern)) {
            Value schemaValue = getString(schema.getName());
            for (SchemaObject object : getTablesForPattern(schema, tableNamePattern)) {
                Value tableName = getString(object.getName());
                if (object instanceof Table) {
                    getColumnsAdd(result, catalogValue, schemaValue, tableName, (Table) object, columnLike);
                } else {
                    TableSynonym s = (TableSynonym) object;
                    Table t = s.getSynonymFor();
                    getColumnsAdd(result, catalogValue, schemaValue, tableName, t, columnLike);
                }
            }
        }
        // TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION
        result.sortRows(new SortOrder(session, new int[] { 1, 2, 16 }));
        return result;
    }

    private void getColumnsAdd(SimpleResult result, Value catalogValue, Value schemaValue, Value tableName, Table t,
            CompareLike columnLike) {
        int ordinal = 0;
        for (Column c : t.getColumns()) {
            if (!c.getVisible()) {
                continue;
            }
            ordinal++;
            String name = c.getName();
            if (columnLike != null && !columnLike.test(name)) {
                continue;
            }
            TypeInfo type = c.getType();
            ValueInteger precision = ValueInteger.get(MathUtils.convertLongToInt(type.getPrecision()));
            boolean nullable = c.isNullable(), isGenerated = c.isGenerated();
            result.addRow(
                    // TABLE_CAT
                    catalogValue,
                    // TABLE_SCHEM
                    schemaValue,
                    // TABLE_NAME
                    tableName,
                    // COLUMN_NAME
                    getString(name),
                    // DATA_TYPE
                    ValueInteger.get(DataType.convertTypeToSQLType(type)),
                    // TYPE_NAME
                    getDataTypeName(type),
                    // COLUMN_SIZE
                    precision,
                    // BUFFER_LENGTH
                    ValueNull.INSTANCE,
                    // DECIMAL_DIGITS
                    ValueInteger.get(type.getScale()),
                    // NUM_PREC_RADIX
                    getRadix(type.getValueType(), false),
                    // NULLABLE
                    nullable ? COLUMN_NULLABLE : COLUMN_NO_NULLS,
                    // REMARKS
                    getString(c.getComment()),
                    // COLUMN_DEF
                    isGenerated ? ValueNull.INSTANCE : getString(c.getDefaultSQL()),
                    // SQL_DATA_TYPE (unused)
                    ValueNull.INSTANCE,
                    // SQL_DATETIME_SUB (unused)
                    ValueNull.INSTANCE,
                    // CHAR_OCTET_LENGTH
                    precision,
                    // ORDINAL_POSITION
                    ValueInteger.get(ordinal),
                    // IS_NULLABLE
                    nullable ? YES : NO,
                    // SCOPE_CATALOG
                    ValueNull.INSTANCE,
                    // SCOPE_SCHEMA
                    ValueNull.INSTANCE,
                    // SCOPE_TABLE
                    ValueNull.INSTANCE,
                    // SOURCE_DATA_TYPE
                    ValueNull.INSTANCE,
                    // IS_AUTOINCREMENT
                    c.isIdentity() ? YES : NO,
                    // IS_GENERATEDCOLUMN
                    isGenerated ? YES : NO);
        }
    }

    @Override
    public ResultInterface getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) {
        if (table == null) {
            throw DbException.getInvalidValueException("table", null);
        }
        checkClosed();
        SimpleResult result = new SimpleResult();
        result.addColumn("TABLE_CAT", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TABLE_SCHEM", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TABLE_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("COLUMN_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("GRANTOR", TypeInfo.TYPE_VARCHAR);
        result.addColumn("GRANTEE", TypeInfo.TYPE_VARCHAR);
        result.addColumn("PRIVILEGE", TypeInfo.TYPE_VARCHAR);
        result.addColumn("IS_GRANTABLE", TypeInfo.TYPE_VARCHAR);
        if (!checkCatalogName(catalog)) {
            return result;
        }
        Database db = session.getDatabase();
        Value catalogValue = getString(db.getShortName());
        CompareLike columnLike = getLike(columnNamePattern);
        for (Right r : db.getAllRights()) {
            DbObject object = r.getGrantedObject();
            if (!(object instanceof Table)) {
                continue;
            }
            Table t = (Table) object;
            String tableName = t.getName();
            if (!db.equalsIdentifiers(table, tableName)) {
                continue;
            }
            Schema s = t.getSchema();
            if (!checkSchema(schema, s)) {
                continue;
            }
            addPrivileges(result, catalogValue, s.getName(), tableName, r.getGrantee(), r.getRightMask(), columnLike,
                    t.getColumns());
        }
        // COLUMN_NAME, PRIVILEGE
        result.sortRows(new SortOrder(session, new int[] { 3, 6 }));
        return result;
    }

    @Override
    public ResultInterface getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) {
        checkClosed();
        SimpleResult result = new SimpleResult();
        result.addColumn("TABLE_CAT", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TABLE_SCHEM", TypeInfo.TYPE_VARCHAR);
        result.addColumn("TABLE_NAME", TypeInfo.TYPE_VARCHAR);
        result.addColumn("GRANTOR", TypeInfo.TYPE_VARCHAR);
        result.addColumn("GRANTEE", TypeInfo.TYPE_VARCHAR);
        result.addColumn("PRIVILEGE", TypeInfo.TYPE_VARCHAR);
        result.addColumn("IS_GRANTABLE", TypeInfo.TYPE_VARCHAR);
        if (!checkCatalogName(catalog)) {
            return result;
        }
        Database db = session.getDatabase();
        Value catalogValue = getString(db.getShortName());
        CompareLike schemaLike = getLike(schemaPattern);
        CompareLike tableLike = getLike(tableNamePattern);
        for (Right r : db.getAllRights()) {
            DbObject object = r.getGrantedObject();
            if (!(object instanceof Table)) {
                continue;
            }
            Table table = (Table) object;
            String tableName = table.getName();
            if (tableLike != null && !tableLike.test(tableName)) {
                continue;
            }
            Schema schema = table.getSchema();
            String schemaName = schema.getName();
            if (schemaPattern != null) {
                if (schemaPattern.isEmpty()) {
                    if (schema != db.getMainSchema()) {
                        continue;
                    }
                } else {
                    if (!schemaLike.test(schemaName)) {
                        continue;
                    }
                }
            }
            addPrivileges(result, catalogValue, schemaName, tableName, r.getGrantee(), r.getRightMask(), null, null);
        }
        // TABLE_CAT, TABLE_SCHEM, TABLE_NAME, PRIVILEGE
        result.sortRows(new SortOrder(session, new int[] { 1, 2, 5 }));
        return result;
    }

    private void addPrivileges(SimpleResult result, Value catalogValue, String schemaName, String tableName,
            DbObject grantee, int rightMask, CompareLike columnLike, Column[] columns) {
        Value schemaValue = getString(schemaName);
        Value tableValue = getString(tableName);
        Value granteeValue = getString(grantee.getName());
        boolean isAdmin = grantee.getType() == DbObject.USER && ((User) grantee).isAdmin();
        if ((rightMask & Right.SELECT) != 0) {
            addPrivilege(result, catalogValue, schemaValue, tableValue, granteeValue, "SELECT", isAdmin, columnLike,
                    columns);
        }
        if ((rightMask & Right.INSERT) != 0) {
            addPrivilege(result, catalogValue, schemaValue, tableValue, granteeValue, "INSERT", isAdmin, columnLike,
                    columns);
        }
        if ((rightMask & Right.UPDATE) != 0) {
            addPrivilege(result, catalogValue, schemaValue, tableValue, granteeValue, "UPDATE", isAdmin, columnLike,
                    columns);
        }
        if ((rightMask & Right.DELETE) != 0) {
            addPrivilege(result, catalogValue, schemaValue, tableValue, granteeValue, "DELETE", isAdmin, columnLike,
                    columns);
        }
    }

    private void addPrivilege(SimpleResult result, Value catalogValue, Value schemaValue, Value tableValue,
            Value granteeValue, String right, boolean isAdmin, CompareLike columnLike, Column[] columns) {
        if (columns == null) {
            result.addRow(
                    // TABLE_CAT
                    catalogValue,
                    // TABLE_SCHEM
                    schemaValue,
                    // TABLE_NAME
                    tableValue,
                    // GRANTOR
                    ValueNull.INSTANCE,
                    // GRANTEE
                    granteeValue,
                    // PRIVILEGE
                    getString(right),
                    // IS_GRANTABLE
                    isAdmin ? YES : NO);
        } else {
            for (Column column : columns) {
                String columnName = column.getName();
                if (columnLike != null && !columnLike.test(columnName)) {
                    continue;
                }
                result.addRow(
                        // TABLE_CAT
                        catalogValue,
                        // TABLE_SCHEM
                        schemaValue,
                        // TABLE_NAME
                        tableValue,
                        // COLUMN_NAME
                        getString(columnName),
                        // GRANTOR
                        ValueNull.INSTANCE,
                        // GRANTEE
                        granteeValue,
                        // PRIVILEGE
                        getString(right),
                        // IS_GRANTABLE
                        isAdmin ? YES : NO);
            }
        }
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
            if (t == null) {
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
                    result.addRow(
                            // SCOPE
                            BEST_ROW_SESSION,
                            // COLUMN_NAME
                            getString(c.getName()),
                            // DATA_TYPE
                            ValueInteger.get(DataType.convertTypeToSQLType(type)),
                            // TYPE_NAME
                            getDataTypeName(type),
                            // COLUMN_SIZE
                            ValueInteger.get(MathUtils.convertLongToInt(type.getPrecision())),
                            // BUFFER_LENGTH
                            ValueNull.INSTANCE,
                            // DECIMAL_DIGITS
                            dt.supportsScale ? ValueSmallint.get(MathUtils.convertIntToShort(type.getScale()))
                                    : ValueNull.INSTANCE,
                            // PSEUDO_COLUMN
                            BEST_ROW_NOT_PSEUDO);
                }
            }
        }
        // Order by SCOPE (always the same)
        return result;
    }

    private Value getDataTypeName(TypeInfo typeInfo) {
        return getString(typeInfo.getDeclaredTypeName());
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
            if (t == null) {
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
        // COLUMN_NAME
        result.sortRows(new SortOrder(session, new int[] { 3 }));
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
            if (t == null) {
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
        // PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, KEY_SEQ
        result.sortRows(new SortOrder(session, new int[] { 1, 2, 8 }));
        return result;
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
            if (t == null) {
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
        // FKTABLE_CAT FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ
        result.sortRows(new SortOrder(session, new int[] { 5, 6, 8 }));
        return result;
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
            if (t == null) {
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
        // FKTABLE_CAT FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ
        result.sortRows(new SortOrder(session, new int[] { 5, 6, 8 }));
        return result;
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
            throw DbException.getInternalError("action=" + action);
        }
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
        for (int i = 1, l = Value.TYPE_COUNT; i < l; i++) {
            DataType t = DataType.getDataType(i);
            Value name = getString(Value.getTypeName(t.type));
            result.addRow(
                    // TYPE_NAME
                    name,
                    // DATA_TYPE
                    ValueInteger.get(t.sqlType),
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
                    ValueBoolean.get(DataType.isNumericType(i)),
                    // LOCAL_TYPE_NAME
                    name,
                    // MINIMUM_SCALE
                    ValueSmallint.get(MathUtils.convertIntToShort(t.minScale)),
                    // MAXIMUM_SCALE
                    ValueSmallint.get(MathUtils.convertIntToShort(t.maxScale)),
                    // SQL_DATA_TYPE (unused)
                    ValueNull.INSTANCE,
                    // SQL_DATETIME_SUB (unused)
                    ValueNull.INSTANCE,
                    // NUM_PREC_RADIX
                    getRadix(t.type, false));
        }
        // DATA_TYPE, better types first
        result.sortRows(new SortOrder(session, new int[] { 1 }));
        return result;
    }

    private static Value getRadix(int valueType, boolean small) {
        if (DataType.isNumericType(valueType)) {
            int radix = valueType == Value.NUMERIC || valueType == Value.DECFLOAT ? 10 : 2;
            return small ? ValueSmallint.get((short) radix) : ValueInteger.get(radix);
        }
        return ValueNull.INSTANCE;
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
            if (t == null) {
                continue;
            }
            getIndexInfo(catalogValue, getString(s.getName()), t, unique, approximate, result, db);
        }
        // NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION
        result.sortRows(new SortOrder(session, new int[] { 3, 6, 5, 7 }));
        return result;
    }

    private void getIndexInfo(Value catalogValue, Value schemaValue, Table table, boolean unique, boolean approximate,
            SimpleResult result, Database db) {
        ArrayList<Index> indexes = table.getIndexes();
        if (indexes != null) {
            for (Index index : indexes) {
                if (index.getCreateSQL() == null) {
                    continue;
                }
                int uniqueColumnCount = index.getUniqueColumnCount();
                if (unique && uniqueColumnCount == 0) {
                    continue;
                }
                Value tableValue = getString(table.getName());
                Value indexValue = getString(index.getName());
                IndexColumn[] cols = index.getIndexColumns();
                ValueSmallint type = index.getIndexType().isHash() ? TABLE_INDEX_HASHED : TABLE_INDEX_OTHER;
                for (int i = 0, l = cols.length; i < l; i++) {
                    IndexColumn c = cols[i];
                    boolean nonUnique = i >= uniqueColumnCount;
                    if (unique && nonUnique) {
                        break;
                    }
                    result.addRow(
                            // TABLE_CAT
                            catalogValue,
                            // TABLE_SCHEM
                            schemaValue,
                            // TABLE_NAME
                            tableValue,
                            // NON_UNIQUE
                            ValueBoolean.get(nonUnique),
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
                            ValueBigint.get(approximate //
                                    ? index.getRowCountApproximation(session)
                                    : index.getRowCount(session)),
                            // PAGES
                            ValueBigint.get(index.getDiskSpaceUsed(approximate) / db.getPageSize()),
                            // FILTER_CONDITION
                            ValueNull.INSTANCE);
                }
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
        Value catalogValue = getString(session.getDatabase().getShortName());
        if (schemaLike == null) {
            for (Schema s : allSchemas) {
                result.addRow(getString(s.getName()), catalogValue);
            }
        } else {
            for (Schema s : allSchemas) {
                String name = s.getName();
                if (schemaLike.test(name)) {
                    result.addRow(getString(s.getName()), catalogValue);
                }
            }
        }
        // TABLE_CATALOG, TABLE_SCHEM
        result.sortRows(new SortOrder(session, new int[] { 0 }));
        return result;
    }

    @Override
    public ResultInterface getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
            String columnNamePattern) {
        SimpleResult result = getPseudoColumnsResult();
        if (!checkCatalogName(catalog)) {
            return result;
        }
        Database db = session.getDatabase();
        Value catalogValue = getString(db.getShortName());
        CompareLike columnLike = getLike(columnNamePattern);
        for (Schema schema : getSchemasForPattern(schemaPattern)) {
            Value schemaValue = getString(schema.getName());
            for (SchemaObject object : getTablesForPattern(schema, tableNamePattern)) {
                Value tableName = getString(object.getName());
                if (object instanceof Table) {
                    getPseudoColumnsAdd(result, catalogValue, schemaValue, tableName, (Table) object, columnLike);
                } else {
                    TableSynonym s = (TableSynonym) object;
                    Table t = s.getSynonymFor();
                    getPseudoColumnsAdd(result, catalogValue, schemaValue, tableName, t, columnLike);
                }
            }
        }
        // TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME
        result.sortRows(new SortOrder(session, new int[] { 1, 2, 3 }));
        return result;
    }

    private void getPseudoColumnsAdd(SimpleResult result, Value catalogValue, Value schemaValue, Value tableName,
            Table t, CompareLike columnLike) {
        Column rowId = t.getRowIdColumn();
        if (rowId != null) {
            getPseudoColumnsAdd(result, catalogValue, schemaValue, tableName, columnLike, rowId);
        }
        for (Column c : t.getColumns()) {
            if (!c.getVisible()) {
                getPseudoColumnsAdd(result, catalogValue, schemaValue, tableName, columnLike, c);
            }
        }
    }

    private void getPseudoColumnsAdd(SimpleResult result, Value catalogValue, Value schemaValue, Value tableName,
            CompareLike columnLike, Column c) {
        String name = c.getName();
        if (columnLike != null && !columnLike.test(name)) {
            return;
        }
        TypeInfo type = c.getType();
        ValueInteger precision = ValueInteger.get(MathUtils.convertLongToInt(type.getPrecision()));
        result.addRow(
                // TABLE_CAT
                catalogValue,
                // TABLE_SCHEM
                schemaValue,
                // TABLE_NAME
                tableName,
                // COLUMN_NAME
                getString(name),
                // DATA_TYPE
                ValueInteger.get(DataType.convertTypeToSQLType(type)),
                // COLUMN_SIZE
                precision,
                // DECIMAL_DIGITS
                ValueInteger.get(type.getScale()),
                // NUM_PREC_RADIX
                getRadix(type.getValueType(), false),
                // COLUMN_USAGE
                NO_USAGE_RESTRICTIONS,
                // REMARKS
                getString(c.getComment()),
                // CHAR_OCTET_LENGTH
                precision,
                // IS_NULLABLE
                c.isNullable() ? YES : NO);
    }

    @Override
    void checkClosed() {
        if (session.isClosed()) {
            throw DbException.get(ErrorCode.DATABASE_CALLED_AT_SHUTDOWN);
        }
    }

    Value getString(String string) {
        return string != null ? ValueVarchar.get(string, session) : ValueNull.INSTANCE;
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

    private Collection<Schema> getSchemasForPattern(String schemaPattern) {
        Database db = session.getDatabase();
        if (schemaPattern == null) {
            return db.getAllSchemas();
        } else if (schemaPattern.isEmpty()) {
            return Collections.singleton(db.getMainSchema());
        } else {
            ArrayList<Schema> list = Utils.newSmallArrayList();
            CompareLike like = getLike(schemaPattern);
            for (Schema s : db.getAllSchemas()) {
                if (like.test(s.getName())) {
                    list.add(s);
                }
            }
            return list;
        }
    }

    private Collection<? extends SchemaObject> getTablesForPattern(Schema schema, String tablePattern) {
        Collection<Table> tables = schema.getAllTablesAndViews(session);
        Collection<TableSynonym> synonyms = schema.getAllSynonyms();
        if (tablePattern == null) {
            if (tables.isEmpty()) {
                return synonyms;
            } else if (synonyms.isEmpty()) {
                return tables;
            }
            ArrayList<SchemaObject> list = new ArrayList<>(tables.size() + synonyms.size());
            list.addAll(tables);
            list.addAll(synonyms);
            return list;
        } else if (tables.isEmpty() && synonyms.isEmpty()) {
            return Collections.emptySet();
        } else {
            ArrayList<SchemaObject> list = Utils.newSmallArrayList();
            CompareLike like = getLike(tablePattern);
            for (Table t : tables) {
                if (like.test(t.getName())) {
                    list.add(t);
                }
            }
            for (TableSynonym t : synonyms) {
                if (like.test(t.getName())) {
                    list.add(t);
                }
            }
            return list;
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

}
