/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

//## Java 1.5 begin ##
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.h2.jaqu.util.Utils;
import org.h2.util.StatementBuilder;

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
        boolean unique;
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

    String tableName;
    private Class<T> clazz;
    private ArrayList<FieldDefinition> fields = Utils.newArrayList();
    private IdentityHashMap<Object, FieldDefinition> fieldMap =
            Utils.newIdentityHashMap();
    private List<String> primaryKeyColumnNames;
    private ArrayList<IndexDefinition> indexes = Utils.newArrayList();

    TableDefinition(Class<T> clazz) {
        this.clazz = clazz;
        tableName = clazz.getSimpleName();
    }

    List<FieldDefinition> getFields() {
        return fields;
    }

    void setTableName(String tableName) {
        this.tableName = tableName;
    }

    void setPrimaryKey(Object[] primaryKeyColumns) {
        this.primaryKeyColumnNames = mapColumnNames(primaryKeyColumns);
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

    private List<String> mapColumnNames(Object[] columns) {
        List<String> columnNames = Utils.newArrayList();
        for (Object column : columns) {
            columnNames.add(getColumnName(column));
        }
        return columnNames;
    }

    void addIndex(Object[] columns) {
        IndexDefinition index = new IndexDefinition();
        index.indexName = tableName + "_" + indexes.size();
        index.columnNames = mapColumnNames(columns);
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
        Field[] classFields = clazz.getFields();
        for (Field f : classFields) {
            FieldDefinition fieldDef = new FieldDefinition();
            fieldDef.field = f;
            fieldDef.columnName = f.getName();
            fieldDef.dataType = getDataType(f);
            fields.add(fieldDef);
        }
    }

    private String getDataType(Field field) {
        Class< ? > fieldClass = field.getType();
        if (fieldClass == Integer.class) {
            return "INT";
        } else if (fieldClass == String.class) {
            return "VARCHAR";
        } else if (fieldClass == Double.class) {
            return "DOUBLE";
        } else if (fieldClass == java.math.BigDecimal.class) {
            return "DECIMAL";
        } else if (fieldClass == java.util.Date.class) {
            return "DATE";
        } else if (fieldClass == java.sql.Date.class) {
            return "DATE";
        } else if (fieldClass == java.sql.Time.class) {
            return "TIME";
        } else if (fieldClass == java.sql.Timestamp.class) {
            return "TIMESTAMP";
        }
        return "VARCHAR";
        // TODO add more data types
    }

    void insert(Db db, Object obj) {
        SQLStatement stat = new SQLStatement(db);
        StatementBuilder buff = new StatementBuilder("INSERT INTO ");
        buff.append(tableName).append('(');
        for (FieldDefinition field : fields) {
            buff.appendExceptFirst(", ");
            buff.append(field.columnName);
        }
        buff.append(") VALUES(");
        buff.resetCount();
        for (FieldDefinition field : fields) {
            buff.appendExceptFirst(", ");
            buff.append('?');
            Object value = field.getValue(obj);
            stat.addParameter(value);
        }
        buff.append(')');
        stat.setSQL(buff.toString());
        stat.executeUpdate();
    }

    void merge(Db db, Object obj) {
        if (primaryKeyColumnNames == null || primaryKeyColumnNames.size() == 0) {
            throw new IllegalStateException("No primary key columns defined "
                + "for table " + obj.getClass() + " - no update possible");
        }
        SQLStatement stat = new SQLStatement(db);
        StatementBuilder buff = new StatementBuilder("MERGE INTO ");
        buff.append(tableName).append(" (");
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
            Object value = field.getValue(obj);
            stat.addParameter(value);
        }
        buff.append(')');
        stat.setSQL(buff.toString());
        stat.executeUpdate();
    }

    void update(Db db, Object obj) {
        if (primaryKeyColumnNames == null || primaryKeyColumnNames.size() == 0) {
            throw new IllegalStateException("No primary key columns defined "
                + "for table " + obj.getClass() + " - no update possible");
        }
        SQLStatement stat = new SQLStatement(db);
        StatementBuilder buff = new StatementBuilder("UPDATE ");
        buff.append(tableName).append(" SET ");
        buff.resetCount();
        for (FieldDefinition field : fields) {
            if (!field.isPrimaryKey) {
                buff.appendExceptFirst(", ");
                buff.append(field.columnName);
                buff.append(" = ?");
                Object value = field.getValue(obj);
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
        stat.executeUpdate();
    }

    TableDefinition<T> createTableIfRequired(Db db) {
        SQLStatement stat = new SQLStatement(db);
        StatementBuilder buff = new StatementBuilder("CREATE TABLE IF NOT EXISTS ");
        buff.append(tableName).append('(');
        for (FieldDefinition field : fields) {
            buff.appendExceptFirst(", ");
            buff.append(field.columnName).append(' ').append(field.dataType);
            if (field.maxLength != 0) {
                buff.append('(').append(field.maxLength).append(')');
            }
        }
        if (primaryKeyColumnNames != null) {
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
        stat.executeUpdate();
        // TODO create indexes
        return this;
    }

    void mapObject(Object obj) {
        fieldMap.clear();
        initObject(obj, fieldMap);
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
