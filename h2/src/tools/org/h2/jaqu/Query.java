/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

/*## Java 1.6 begin ##
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.h2.jaqu.TableDefinition.FieldDefinition;
import org.h2.jaqu.util.Utils;
## Java 1.6 end ##*/

/**
 * This class represents a query.
 *
 * @param <T> the return type
 */
/*## Java 1.6 begin ##
public class Query<T> {
    
    private Db db;
    private T alias;
    private TableDefinition aliasDef;
    private ArrayList<ConditionToken> conditions = Utils.newArrayList();
    // private HashMap<Object, TableDefinition> join = Utils.newHashMap();
    
    public Query(Db db, T alias) {
        this.db = db;
        this.alias = alias;
        aliasDef = db.getTableDefinition(alias.getClass());
    }
    
    @SuppressWarnings("unchecked")
    private <X> Class<X> getClass(X x) {
        return (Class<X>) x.getClass();
    }

    public List<T> select() {
        List<T> result = Utils.newArrayList();
        ResultSet rs = db.executeQuery(toString());
        Class<T> aliasClass = getClass(alias);
        try {
            while (rs.next()) {
                T item = Utils.newObject(aliasClass);
                aliasDef.readRow(item, rs);
                result.add(item);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    public <X, Z> List<X> select(Z x) {
        Class< ? > clazz = x.getClass();
        if (Utils.isSimpleType(clazz)) {
            return selectSimple((X) x);
        }
        clazz = clazz.getSuperclass();
        return select((Class<X>) clazz, (X) x);
    }
    
    private <X> List<X> select(Class<X> clazz, X x) {
        List<X> result = Utils.newArrayList();
        TableDefinition<X> def = db.define(clazz);
        ResultSet rs = db.executeQuery(toString());
        Class<T> aliasClass = getClass(alias);
        try {
            while (rs.next()) {
                T item = Utils.newObject(aliasClass);
                aliasDef.readRow(item, rs);
                X item2 = Utils.newObject(clazz);
                def.copyAttributeValues(db, item, item2, x);
                result.add(item2);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }
    
    public <X> List<X> selectSimple(X x) {
        List<X> result = Utils.newArrayList();
        ResultSet rs = db.executeQuery(toString());
        FieldDefinition<X> def = db.getFieldDefinition(x);
        try {
            while (rs.next()) {
                X item;
                if (def == null) {
                    item = x;
                } else {
                    item = def.read(rs);
                }
                result.add(item);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }
    
    public <A> QueryCondition<T, A> where(A x) {
        return new QueryCondition<T, A>(this, x);
    }
## Java 1.6 end ##*/
    
    /**
     * Order by a number of columns.
     * 
     * @param columns the columns
     * @return the query
     */
/*## Java 1.6 begin ##
    public Query<T> orderBy(Integer... columns) {
        return this;
    }
    
    String getString(Object x) {
        FieldDefinition def = db.getFieldDefinition(x);
        if (def != null) {
            return def.columnName;
        }
        return Utils.quoteSQL(x);
    }

    public void addConditionToken(ConditionToken condition) {
        conditions.add(condition);
    }
    
    public String toString() {
        StringBuffer buff = new StringBuffer("SELECT * FROM ");
        buff.append(aliasDef.tableName);
        if (conditions.size() > 0) {
            buff.append(" WHERE ");
            for (ConditionToken token : conditions) {
                buff.append(token.toString());
                buff.append(' ');
            }
        }
        return buff.toString();
    }
## Java 1.6 end ##*/

    /**
     * Join another table.
     * 
     * @param u an alias for the table to join
     * @return the joined query
     */
/*## Java 1.6 begin ##
    public QueryJoin innerJoin(Object u) {
        return new QueryJoin(this);
    }

}
## Java 1.6 end ##*/
