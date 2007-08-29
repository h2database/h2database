/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;

import org.h2.command.Parser;
import org.h2.engine.FunctionAlias;
import org.h2.engine.Session;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueResultSet;

public class JavaFunction extends Expression implements FunctionCall {

    private FunctionAlias functionAlias;
    private Expression[] args;

    public JavaFunction(FunctionAlias functionAlias, Expression[] args) {
        this.functionAlias = functionAlias;
        this.args = args;
    }
    
    public Value getValue(Session session) throws SQLException {
        return functionAlias.getValue(session, args);
    }

    public int getType() {
        return functionAlias.getDataType();
    }
    
    public void mapColumns(ColumnResolver resolver, int level) throws SQLException {
        for (int i = 0; i < args.length; i++) {
            args[i].mapColumns(resolver, level);
        }        
    }

    public Expression optimize(Session session) throws SQLException {
        for(int i=0; i<args.length; i++) {
            Expression e = args[i].optimize(session);
            args[i] = e;
        }
        return this;
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        for (int i = 0; i < args.length; i++) {
            Expression e = args[i];
            if (e != null) {
                e.setEvaluatable(tableFilter, b);
            }
        }        
    }

    public int getScale() {
        return 0;
    }

    public long getPrecision() {
        return 0;
    }

    public String getSQL() {
        StringBuffer buff = new StringBuffer();
        buff.append(Parser.quoteIdentifier(functionAlias.getName()));
        buff.append('(');
        for (int i = 0; i < args.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            Expression e = args[i];
            buff.append(e.getSQL());
        }
        buff.append(')');
        return buff.toString();
    }

    public void updateAggregate(Session session) throws SQLException {
        for (int i = 0; i < args.length; i++) {
            Expression e = args[i];
            if (e != null) {
                e.updateAggregate(session);
            }
        }        
    }

    public FunctionAlias getFunctionAlias() {
        return functionAlias;
    }

    public String getName() {
        return functionAlias.getName();
    }

    public int getParameterCount() throws SQLException {
        return functionAlias.getParameterCount();
    }

    public ValueResultSet getValueForColumnList(Session session, Expression[] args) throws SQLException {
        Value v = functionAlias.getValue(session, args, true);
        return v == ValueNull.INSTANCE ? null : (ValueResultSet) v;
    }

    public Expression[] getArgs() {
        return args;
    }
    
    public boolean isEverything(ExpressionVisitor visitor) {
        if(visitor.type == ExpressionVisitor.DETERMINISTIC) {
            // TODO optimization: some functions are deterministic, but we don't know (no setting for that)
            return false;
        }
        for (int i = 0; i < args.length; i++) {
            Expression e = args[i];
            if (e != null && !e.isEverything(visitor)) {
                return false;
            }
        }    
        return true;
    }
    
    public int getCost() {
        int cost = functionAlias.hasConnectionParam() ? 25 : 5;
        for(int i=0; i<args.length; i++) {
            cost += args[i].getCost();
        }
        return cost;
    }

}
