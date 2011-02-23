/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

//## Java 1.5 begin ##
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.h2.jaqu.Table.IndexType;
import org.h2.jaqu.Table.JQColumn;
import org.h2.jaqu.Table.JQIndex;
import org.h2.jaqu.Table.JQSchema;
import org.h2.jaqu.Table.JQTable;
import org.h2.jaqu.util.StatementLogger;
import org.h2.jaqu.util.StatementBuilder;
import org.h2.jaqu.util.StringUtils;
import org.h2.jaqu.util.Utils;
//## Java 1.5 end ##

/**
 * A table definition contains the index definitions of a table, the field
 * definitions, the table name, and other meta data.
 *
 * @param <T> the table type
 */
//## Java 1.5 begin ##
class TableDefinition<T> {
//## Java 1.5 end ##

    /**
     * The meta data of an index.
     */
//## Java 1.5 begin ##
    static class IndexDefinition {
        IndexType type;
        String indexName;

        List<String> columnNames;
    }
//## Java 1.5 end ##

    /**
     * The meta data of a field.
     */
//## Java 1.5 begin ##
    static class FieldDefinition {
        String columnName;
        Field field;
        String dataType;
        int maxLength;
        boolean isPrimaryKey;
        boolean isAutoIncrement;
        boolean trimString;
        boolean allowNull;
        String defaultValue;

        Object getValue(Object obj) {
            try {
                return field.get(obj);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        void initWithNewObject(Object obj) {
            Object o = Utils.newObject(field.getType());
            setValue(obj, o);
        }

        void setValue(Object obj, Object o) {
            try {
                int setAccessibleShouldNotBeRequiredHere;
                if (!field.isAccessible()) {
                    field.setAccessible(true);
                }
                o = Utils.convert(o, field.getType());
                field.set(obj, o);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        Object read(ResultSet rs, int columnIndex) {
            try {
                return rs.getObject(columnIndex);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    String schemaName;
    String tableName;
    private boolean createTableIfRequired = true;
    private Class<T> clazz;
    private ArrayList<FieldDefinition> fields = Utils.newArrayList();
    private IdentityHashMap<Object, FieldDefinition> fieldMap =
            Utils.newIdentityHashMap();

    private List<String> primaryKeyColumnNames;
    private ArrayList<IndexDefinition> indexes = Utils.newArrayList();
    private boolean memoryTable;

    int tableVersion;

    TableDefinition(Class<T> clazz) {
        this.clazz = clazz;
        schemaName = null;
        tableName = clazz.getSimpleName();
    }

    Class<T> getModelClass() {
        return clazz;
    }

    List<FieldDefinition> getFields() {
        return fields;
    }

    void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Define a primary key by the specified model fields.
     * 
     * @param modelFields the ordered list of model fields
     */
    void setPrimaryKey(Object[] modelFields) {
        List<String> columnNames = mapColumnNames(modelFields);
        setPrimaryKey(columnNames);
    }

    /**
     * Define a primary key by the specified column names.
     * 
     * @param columnNames the ordered list of column names
     */
    void setPrimaryKey(List<String> columnNames) {
        primaryKeyColumnNames = Utils.newArrayList(columnNames);
        // set isPrimaryKey flag for all field definitions
        for (FieldDefinition fieldDefinition : fieldMap.values()) {
            fieldDefinition.isPrimaryKey = this.primaryKeyColumnNames
                .contains(fieldDefinition.columnName);
        }
    }

    <A> String getColumnName(A fieldObject) {
        FieldDefinition def = fieldMap.get(fieldObject);
        return def == null ? null : def.columnName;
    }

    private ArrayList<String> mapColumnNames(Object[] columns) {
        ArrayList<String> columnNames = Utils.newArrayList();
        for (Object column : columns) {
            columnNames.add(getColumnName(column));
        }
        return columnNames;
    }

    /**
     * Defines an index with the specified model fields.
     * 
     * @param type the index type (STANDARD, HASH, UNIQUE, UNIQUE_HASH)
     * @param modelFields the ordered list of model fields
     */
    void addIndex(IndexType type, Object[] modelFields) {
        List<String> columnNames = mapColumnNames(modelFields);
        addIndex(type, columnNames);
    }

    /**
     * Defines an index with the specified column names.
     * 
     * @param type the index type (STANDARD, HASH, UNIQUE, UNIQUE_HASH) 
     * @param columnNames the ordered list of column names
     */
    void addIndex(IndexType type, List<String> columnNames) {
        IndexDefinition index = new IndexDefinition();
        index.indexName = tableName + "_" + indexes.size();
        index.columnNames = Utils.newArrayList(columnNames);
        index.type = type;
        indexes.add(index);
    }

    public void setMaxLength(Object column, int maxLength) {
        String columnName = getColumnName(column);
        for (FieldDefinition f: fields) {
            if (f.columnName.equals(columnName)) {
                f.maxLength = maxLength;
                break;
            }
        }
    }

    void mapFields() {
        boolean byAnnotationsOnly = false;
        boolean inheritColumns = false;
        boolean strictTypeMapping = false;
        if (clazz.isAnnotationPresent(JQTable.class)) {
            JQTable tableAnnotation = clazz.getAnnotation(JQTable.class);
            byAnnotationsOnly = tableAnnotation.annotationsOnly();
            inheritColumns = tableAnnotation.inheritColumns();
            strictTypeMapping = tableAnnotation.strictTypeMapping();
        }

        List<Field> classFields = Utils.newArrayList();
        classFields.addAll(Arrays.asList(clazz.getDeclaredFields()));
        if (inheritColumns) {
            Class<?> superClazz = clazz.getSuperclass();
            classFields.addAll(Arrays.asList(superClazz.getDeclaredFields()));
        }

        for (Field f : classFields) {
            // default to field name
            String columnName = f.getName();
            boolean isAutoIncrement = false;
            boolean isPrimaryKey = false;
            int maxLength = 0;
            boolean trimString = false;
            boolean allowNull = true;
            String defaultValue = "";
            boolean hasAnnotation = f.isAnnotationPresent(JQColumn.class);
            if (hasAnnotation) {
                JQColumn col = f.getAnnotation(JQColumn.class);
                if (!StringUtils.isNullOrEmpty(col.name())) {
                    columnName = col.name();
                }
                isAutoIncrement = col.autoIncrement();
                isPrimaryKey = col.primaryKey();
                maxLength = col.maxLength();
                trimString = col.trimString();
                allowNull = col.allowNull();
                defaultValue = col.defaultValue();
            }
            boolean isPublic = Modifier.isPublic(f.getModifiers());
            boolean reflectiveMatch = isPublic && !byAnnotationsOnly;
            if (reflectiveMatch || hasAnnotation) {
                FieldDefinition fieldDef = new FieldDefinition();
                fieldDef.field = f;
                fieldDef.columnName = columnName;
                fieldDef.isAutoIncrement = isAutoIncrement;
                fieldDef.isPrimaryKey = isPrimaryKey;
                fieldDef.maxLength = maxLength;
                fieldDef.trimString = trimString;
                fieldDef.allowNull = allowNull;
                fieldDef.defaultValue = defaultValue;
                fieldDef.dataType = ModelUtils.getDataType(fieldDef, strictTypeMapping);
                fields.add(fieldDef);
            }
        }
        List<String> primaryKey = Utils.newArrayList();
        for (FieldDefinition fieldDef : fields) {
            if (fieldDef.isPrimaryKey) {
                primaryKey.add(fieldDef.columnName);
            }
        }
        if (primaryKey.size() > 0) {
            setPrimaryKey(primaryKey);
        }
    }

    // Optionally truncates strings to maxLength
    private Object getValue(Object obj, FieldDefinition field) {
        Object value = field.getValue(obj);
        if (field.trimString && field.maxLength > 0) {
            if (value instanceof String) {
                // Clip Strings
                String s = (String) value;
                if (s.length() > field.maxLength)
                    return s.substring(0, field.maxLength);
                return s;
            }
            return value;
        } else
            // Standard JaQu behavior
            return value;
    }

    long insert(Db db, Object obj, boolean returnKey) {
        SQLStatement stat = new SQLStatement(db);
        StatementBuilder buff = new StatementBuilder("INSERT INTO ");
        buff.append(db.getDialect().tableName(schemaName, tableName)).append('(');
        for (FieldDefinition field : fields) {
            buff.appendExceptFirst(", ");
            buff.append(field.columnName);
        }
        buff.append(") VALUES(");
        buff.resetCount();
        for (FieldDefinition field : fields) {
            buff.appendExceptFirst(", ");
            buff.append('?');
            Object value = getValue(obj, field);
            stat.addParameter(value);
        }
        buff.append(')');
        stat.setSQL(buff.toString());
        StatementLogger.insert(stat.getSQL());
        if (returnKey)
            return stat.executeInsert();
        return stat.executeUpdate();
    }

    void merge(Db db, Object obj) {
        if (primaryKeyColumnNames == null || primaryKeyColumnNames.size() == 0) {
            throw new IllegalStateException("No primary key columns defined "
                + "for table " + obj.getClass() + " - no update possible");
        }
        SQLStatement stat = new SQLStatement(db);
        StatementBuilder buff = new StatementBuilder("MERGE INTO ");
        buff.append(db.getDialect().tableName(schemaName, tableName)).append(" (");
        buff.resetCount();
        for (FieldDefinition field : fields) {
            buff.appendExceptFirst(", ");
            buff.append(field.columnName);
        }
        buff.append(") KEY(");
        buff.resetCount();
        for (FieldDefinition field : fields) {
            if (field.isPrimaryKey) {
                buff.appendExceptFirst(", ");
                buff.append(field.columnName);
            }
        }
        buff.append(") ");
        buff.resetCount();
        buff.append("VALUES (");
        for (FieldDefinition field : fields) {
            buff.appendExceptFirst(", ");
            buff.append('?');
            Object value = getValue(obj, field);
            stat.addParameter(value);
        }
        buff.append(')');
        stat.setSQL(buff.toString());
        StatementLogger.merge(stat.getSQL());
        stat.executeUpdate();
    }

    void update(Db db, Object obj) {
        if (primaryKeyColumnNames == null || primaryKeyColumnNames.size() == 0) {
            throw new IllegalStateException("No primary key columns defined "
                + "for table " + obj.getClass() + " - no update possible");
        }
        SQLStatement stat = new SQLStatement(db);
        StatementBuilder buff = new StatementBuilder("UPDATE ");
        buff.append(db.getDialect().tableName(schemaName, tableName)).append(" SET ");
        buff.resetCount();

        for (FieldDefinition field : fields) {
            if (!field.isPrimaryKey) {
                buff.appendExceptFirst(", ");
                buff.append(field.columnName);
                buff.append(" = ?");
                Object value = getValue(obj, field);
                stat.addParameter(value);
            }
        }
        Object alias = Utils.newObject(obj.getClass());
        Query<Object> query = Query.from(db, alias);
        boolean firstCondition = true;
        for (FieldDefinition field : fields) {
            if (field.isPrimaryKey) {
                Object aliasValue = field.getValue(alias);
                Object value = field.getValue(obj);
                if (!firstCondition) {
                    query.addConditionToken(ConditionAndOr.AND);
                }
                firstCondition = false;
                query.addConditionToken(
                        new Condition<Object>(
                                aliasValue, value, CompareType.EQUAL));
            }
        }
        stat.setSQL(buff.toString());
        query.appendWhere(stat);
        StatementLogger.update(stat.getSQL());
        stat.executeUpdate();
    }

    void delete(Db db, Object obj) {
        if (primaryKeyColumnNames == null || primaryKeyColumnNames.size() == 0) {
            throw new IllegalStateException("No primary key columns defined "
                + "for table " + obj.getClass() + " - no update possible");
        }
        SQLStatement stat = new SQLStatement(db);
        StatementBuilder buff = new StatementBuilder("DELETE FROM ");
        buff.append(db.getDialect().tableName(schemaName, tableName));
        buff.resetCount();
        Object alias = Utils.newObject(obj.getClass());
        Query<Object> query = Query.from(db, alias);
        boolean firstCondition = true;
        for (FieldDefinition field : fields) {
            if (field.isPrimaryKey) {
                Object aliasValue = field.getValue(alias);
                Object value = field.getValue(obj);
                if (!firstCondition) {
                    query.addConditionToken(ConditionAndOr.AND);
                }
                firstCondition = false;
                query.addConditionToken(
                    new Condition<Object>(
                        aliasValue, value, CompareType.EQUAL));
            }
        }
        stat.setSQL(buff.toString());
        query.appendWhere(stat);
        StatementLogger.delete(stat.getSQL());
        stat.executeUpdate();
    }

    TableDefinition<T> createTableIfRequired(Db db) {
        if (!createTableIfRequired) {
            // Skip table and index creation
            // But still check for upgrades
            db.upgradeTable(this);
            return this;
        }
        SQLStatement stat = new SQLStatement(db);
        StatementBuilder buff;
        if (memoryTable &&
                db.getConnection().getClass()
                .getCanonicalName().equals("org.h2.jdbc.JdbcConnection"))
            buff = new StatementBuilder("CREATE MEMORY TABLE IF NOT EXISTS ");
        else
            buff = new StatementBuilder("CREATE TABLE IF NOT EXISTS ");

        int todoChangeToGetTableNameChangeAllMethodsInDialectInterface;
        buff.append(db.getDialect().tableName(schemaName, tableName)).append('(');

        for (FieldDefinition field : fields) {
            buff.appendExceptFirst(", ");
            buff.append(field.columnName).append(' ').append(field.dataType);

            // FIELD LENGTH
            if (field.maxLength > 0) {
                buff.append('(').append(field.maxLength).append(')');
            }

            // AUTO_INCREMENT
            if (field.isAutoIncrement) {
                buff.append(" AUTO_INCREMENT");
            }

            // NOT NULL
            if (!field.allowNull) {
                buff.append(" NOT NULL");
            }

            // DEFAULT...
            if (!field.isAutoIncrement && !field.isPrimaryKey) {
                String dv = field.defaultValue;
                if (!StringUtils.isNullOrEmpty(dv)) {
                    if (ModelUtils.isProperlyFormattedDefaultValue(dv)
                            && ModelUtils.isValidDefaultValue(field.field.getType(), dv)) {
                        buff.append(" DEFAULT " + dv);
                    }
                }
            }
        }

        int reviewJavadoc;
        // PRIMARY KEY...
        if (primaryKeyColumnNames != null && primaryKeyColumnNames.size() > 0) {
            buff.append(", PRIMARY KEY(");
            buff.resetCount();
            for (String n : primaryKeyColumnNames) {
                buff.appendExceptFirst(", ");
                buff.append(n);
            }
            buff.append(')');
        }
        buff.append(')');
        stat.setSQL(buff.toString());
        StatementLogger.create(stat.getSQL());
        stat.executeUpdate();

        // Create Indexes
        for (IndexDefinition index:indexes) {
            String sql = db.getDialect().createIndex(schemaName, tableName, index);
            stat.setSQL(sql);
            StatementLogger.create(stat.getSQL());
            stat.executeUpdate();
        }

        // Table is created IF NOT EXISTS, otherwise statement is ignored
        // But we still need to process potential Upgrade
        db.upgradeTable(this);
        return this;
    }

    // Retrieve list of columns from CSV whitespace notated index
    private List<String> getColumns(String index) {
        List<String> cols = Utils.newArrayList();
        if (index == null || index.length() == 0)
            return null;
        String[] cs = index.split("(,|\\s)");
        for (String c : cs)
            if (c != null && c.trim().length() > 0)
                cols.add(c.trim());
        if (cols.size() == 0)
            return null;
        return cols;
    }

    void mapObject(Object obj) {
        fieldMap.clear();
        initObject(obj, fieldMap);

        if (clazz.isAnnotationPresent(JQSchema.class)) {
            JQSchema schemaAnnotation = clazz.getAnnotation(JQSchema.class);
            // Setup Schema name mapping, if properly annotated
            if (!StringUtils.isNullOrEmpty(schemaAnnotation.name()))
                schemaName = schemaAnnotation.name();
        }

        if (clazz.isAnnotationPresent(JQTable.class)) {
            JQTable tableAnnotation = clazz.getAnnotation(JQTable.class);

            // Setup Table name mapping, if properly annotated
            if (!StringUtils.isNullOrEmpty(tableAnnotation.name()))
                tableName = tableAnnotation.name();

            // Allow control over createTableIfRequired()
            createTableIfRequired = tableAnnotation.createIfRequired();

            // Model Version
            if (tableAnnotation.version() > 0)
                tableVersion = tableAnnotation.version();

            // Setup the Primary Index, if properly annotated
            List<String> primaryKey = getColumns(tableAnnotation.primaryKey());
            if (primaryKey != null)
                setPrimaryKey(primaryKey);
        }

        if (clazz.isAnnotationPresent(JQIndex.class)) {
            JQIndex indexAnnotation = clazz.getAnnotation(JQIndex.class);

            // Setup the indexes, if properly annotated
            addIndexes(IndexType.STANDARD, indexAnnotation.standard());
            addIndexes(IndexType.UNIQUE, indexAnnotation.unique());
            addIndexes(IndexType.HASH, indexAnnotation.hash());
            addIndexes(IndexType.UNIQUE_HASH, indexAnnotation.uniqueHash());
        }
    }

    void addIndexes(IndexType type, String [] indexes) {
        for (String index:indexes) {
            List<String> validatedColumns = getColumns(index);
            if (validatedColumns == null)
                return;
            addIndex(type, validatedColumns);
        }
    }

    List<IndexDefinition> getIndexes(IndexType type) {
        List<IndexDefinition> list = Utils.newArrayList();
        for (IndexDefinition def:indexes)
            if (def.type.equals(type))
                list.add(def);
        return list;
    }

    void initObject(Object obj, Map<Object, FieldDefinition> map) {
        for (FieldDefinition def : fields) {
            def.initWithNewObject(obj);
            map.put(def.getValue(obj), def);
        }
    }

    void initSelectObject(SelectTable<T> table, Object obj,
            Map<Object, SelectColumn<T>> map) {
        for (FieldDefinition def : fields) {
            def.initWithNewObject(obj);
            SelectColumn<T> column = new SelectColumn<T>(table, def);
            map.put(def.getValue(obj), column);
        }
    }

    void readRow(Object item, ResultSet rs) {
        for (int i = 0; i < fields.size(); i++) {
            FieldDefinition def = fields.get(i);
            Object o = def.read(rs, i + 1);
            def.setValue(item, o);
        }
    }

    SQLStatement getSelectList(Db db) {
        SQLStatement selectList = new SQLStatement(db);
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                selectList.appendSQL(", ");
            }
            FieldDefinition def = fields.get(i);
            selectList.appendSQL(def.columnName);
        }
        return selectList;
    }

    <Y, X> SQLStatement getSelectList(Query<Y> query, X x) {
        SQLStatement selectList = new SQLStatement(query.getDb());
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                selectList.appendSQL(", ");
            }
            FieldDefinition def = fields.get(i);
            Object obj = def.getValue(x);
            query.appendSQL(selectList, obj);
        }
        return selectList;
    }

    <Y, X> void copyAttributeValues(Query<Y> query, X to, X map) {
        for (FieldDefinition def : fields) {
            Object obj = def.getValue(map);
            SelectColumn<Y> col = query.getSelectColumn(obj);
            Object value = col.getCurrentValue();
            def.setValue(to, value);
        }
    }

}
//## Java 1.5 end ##
