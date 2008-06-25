/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

import java.util.ArrayList;

//## Java 1.5 begin ##
import org.h2.jaqu.util.ClassUtils;
import org.h2.jaqu.util.Utils;
//## Java 1.5 end ##

/**
 * This class represents a table in a query.
 * 
 * @param <T> the table class
 */
//## Java 1.5 begin ##
class SelectTable <T> {
    
    private static int asCounter;
    private Query query;
    private Class<T> clazz;
    private T current;
    private String as;
    private TableDefinition<T> aliasDef;
    private boolean outerJoin;
    private ArrayList<ConditionToken> joinConditions = Utils.newArrayList();

    SelectTable(Db db, Query query, T alias, boolean outerJoin) {
        this.query = query;
        this.outerJoin = outerJoin;
        aliasDef = db.getTableDefinition(alias.getClass());
        clazz = ClassUtils.getClass(alias);
        as = "T" + asCounter++;
    }

    T newObject() {
        return Utils.newObject(clazz);
    }

    TableDefinition getAliasDefinition() {
        return aliasDef;
    }
    
    String getString() {
        if (query.isJoin()) {
            return aliasDef.tableName + " AS " + as;
        }
        return aliasDef.tableName;
    }
    
    String getStringAsJoin() {
        StringBuilder buff = new StringBuilder();
        if (outerJoin) {
            buff.append(" LEFT OUTER JOIN ");
        } else {
            buff.append(" INNER JOIN ");
        }
        buff.append(getString());
        if (!joinConditions.isEmpty()) {
            buff.append(" ON ");
            for (ConditionToken token : joinConditions) {
                buff.append(token.getString());
                buff.append(' ');
            }
        }
        return buff.toString();
    }

    boolean getOuterJoin() {
        return outerJoin;
    }

    Query getQuery() {
        return query;
    }

    String getAs() {
        return as;
    }
    
    void addConditionToken(ConditionToken condition) {
        joinConditions.add(condition);
    }

    T getCurrent() {
        return current;
    }

    void setCurrent(T current) {
        this.current = current;
    }
    
}
//## Java 1.5 end ##
