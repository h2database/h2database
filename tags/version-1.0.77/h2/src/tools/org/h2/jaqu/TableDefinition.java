/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
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
import java.util.Map;

import org.h2.jaqu.util.Utils;

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
        String[] columnNames;
    }
//## Java 1.5 end ##
    
    /**
     * The meta data of a field.
     */
//## Java 1.5 begin ##
    static class FieldDefinition<X> {
        String columnName;
        Field field;
        String dataType;
        
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
        
        @SuppressWarnings("unchecked")
        X read(ResultSet rs, int columnIndex) {
            try {
                return (X) rs.getObject(columnIndex);
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
    private String[] primaryKeyColumnNames;
    private ArrayList<IndexDefinition> indexes = Utils.newArrayList();

    TableDefinition(Class<T> clazz) {
        this.clazz = clazz;
        tableName = clazz.getSimpleName();
    }
    
    void setTableName(String tableName) {
        this.tableName = tableName;
    }

    void setPrimaryKey(Object[] primaryKeyColumns) {
        this.primaryKeyColumnNames = mapColumnNames(primaryKeyColumns);
    }
    
    <A> String getColumnName(A fieldObject) {
        FieldDefinition def = fieldMap.get(fieldObject);
        return def == null ? null : def.columnName;
    }
    
    private String[] mapColumnNames(Object[] columns) {
        int len = columns.length;
        String[] columnNames = new String[len];
        for (int i = 0; i < len; i++) {
            columnNames[i] = getColumnName(columns[i]);
        }
        return columnNames;
    }
    
    void addIndex(Object[] columns) {
        IndexDefinition index = new IndexDefinition();
        index.indexName = tableName + "_" + indexes.size();
        index.columnNames = mapColumnNames(columns);
        indexes.add(index);
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
        Class< ? > clazz = field.getType();
        if (clazz == Integer.class) {
            return "INT";
        } else if (clazz == String.class) {
            return "VARCHAR";
        } else if (clazz == Double.class) {
            return "DOUBLE";
        } else if (clazz == java.math.BigDecimal.class) {
            return "DECIMAL";
        } else if (clazz == java.util.Date.class) {
            return "DATE";
        } else if (clazz == java.sql.Date.class) {
            return "DATE";
        } else if (clazz == java.sql.Time.class) {
            return "TIME";
        } else if (clazz == java.sql.Timestamp.class) {
            return "TIMESTAMP";
        }
        return "VARCHAR";
        // TODO add more data types
    }
    
    void insert(Db db, Object obj) {
        SqlStatement stat = new SqlStatement(db);
        StringBuilder buff = new StringBuilder("INSERT INTO ");
        buff.append(tableName);
        buff.append(" VALUES(");
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append('?');
            FieldDefinition field = fields.get(i);
            Object value = field.getValue(obj);
            stat.addParameter(value);
        }        
        buff.append(')');
        stat.setSQL(buff.toString());
        stat.executeUpdate();
    }

    TableDefinition createTableIfRequired(Db db) {
        SqlStatement stat = new SqlStatement(db);
        StringBuilder buff = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        buff.append(tableName);
        buff.append('(');
        for (int i = 0; i < fields.size(); i++) {
            FieldDefinition field = fields.get(i);
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(field.columnName);
            buff.append(' ');
            buff.append(field.dataType);
        }
        if (primaryKeyColumnNames != null) {
            buff.append(", PRIMARY KEY(");
            for (int i = 0; i < primaryKeyColumnNames.length; i++) {
                if (i > 0) {
                    buff.append(", ");
                }                
                buff.append(primaryKeyColumnNames[i]);
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

    void initSelectObject(SelectTable table, Object obj, 
            Map<Object, SelectColumn> map) {
        for (FieldDefinition def : fields) {
            def.initWithNewObject(obj);
            SelectColumn column = new SelectColumn(table, def);
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
    
    <X> SqlStatement getSelectList(Query query, X x) {
        SqlStatement selectList = new SqlStatement(query.getDb());
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
    
    <U, X> void copyAttributeValues(Query query, X to, X map) {
        for (FieldDefinition def : fields) {
            Object obj = def.getValue(map);
            SelectColumn col = query.getSelectColumn(obj);
            Object value = col.getCurrentValue();
            def.setValue(to, value);
        }
    }

}
//## Java 1.5 end ##
