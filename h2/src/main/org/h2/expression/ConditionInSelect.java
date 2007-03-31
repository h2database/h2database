/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;

import org.h2.command.dml.Query;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;

/**
 * @author Thomas
 */
public class ConditionInSelect extends Condition {
    private Database database;
    private Expression left;
    private Query query;
    private boolean all;
    private int compareType;
    private int queryLevel;

    public ConditionInSelect(Database database, Expression left, Query query, boolean all, int compareType) {
        this.database = database;
        this.left = left;
        this.query = query;
        this.all = all;
        this.compareType = compareType;
    }

    public Value getValue(Session session) throws SQLException {
        Value l = left.getValue(session);
        if(l == ValueNull.INSTANCE) {
            return l;
        }
        query.setSession(session);
        LocalResult rows = query.query(0);
        boolean hasNull = false;
        boolean result = all;
        try {
            while(rows.next()) {
                boolean value;
                Value r = rows.currentRow()[0];
                if(r == ValueNull.INSTANCE) {
                    value = false;
                    hasNull = true;
                } else {
                    value = Comparison.compareNotNull(database, l, r, compareType);
                }
                if(!value && all) {
                    result = false;
                    break;
                } else if(value && !all) {
                    result = true;
                    break;
                }
            }
            if(!result && hasNull) {
                return ValueNull.INSTANCE;
            }            
            return ValueBoolean.get(result);
        } finally {
            rows.close();
        }
    }

    public void mapColumns(ColumnResolver resolver, int queryLevel) throws SQLException {
        left.mapColumns(resolver, queryLevel);
        query.mapColumns(resolver, queryLevel+1);
        this.queryLevel = Math.max(queryLevel, this.queryLevel);
    }

    public Expression optimize(Session session) throws SQLException {
        left = left.optimize(session);
        if(left == ValueExpression.NULL) {
            return left;
        }
        if(query.getColumnCount() != 1) {
            throw Message.getSQLException(Message.SUBQUERY_IS_NOT_SINGLE_COLUMN);
        }
        query.prepare();
        // Can not optimize IN(SELECT...): the data may change
        // However, could transform to an inner join
        return this;
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
        query.setEvaluatable(tableFilter, b);
    }

    public String getSQL() {
        StringBuffer buff = new StringBuffer("(");
        buff.append(left.getSQL());
        buff.append(" IN(");
        buff.append(query.getPlan());
        buff.append("))");
        return buff.toString();
    }

    public void updateAggregate(Session session) {
        // TODO exists: is it allowed that the subquery contains aggregates? probably not
        // select id from test group by id having 1 in (select * from test2 where id=count(test.id))
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor) && query.isEverything(visitor);
    }
    
    public int getCost() {
        return left.getCost() + 10 + (int)(10 * query.getCost());
    }

}
