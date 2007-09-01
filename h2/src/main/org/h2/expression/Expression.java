/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;

import org.h2.engine.Session;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.util.StringUtils;
import org.h2.value.Value;


/**
 * @author Thomas
 */
public abstract class Expression {
    
    private boolean addedToFilter;

    public abstract Value getValue(Session session) throws SQLException;
    public abstract int getType();
    public abstract void mapColumns(ColumnResolver resolver, int level) throws SQLException;
    public abstract Expression optimize(Session session) throws SQLException;
    public abstract void setEvaluatable(TableFilter tableFilter, boolean b);
    public abstract int getScale();
    public abstract long getPrecision();
    public abstract String getSQL();
    public abstract void updateAggregate(Session session) throws SQLException;
    public abstract boolean isEverything(ExpressionVisitor visitor);
    public abstract int getCost();

    public final boolean isEverything(int expressionVisitorType) {
        ExpressionVisitor visitor = ExpressionVisitor.get(expressionVisitorType);
        return isEverything(visitor);
    }
    
    public Expression getNotIfPossible(Session session) {
        // by default it is not possible
        return null;
    }
    
    public boolean isConstant() {
        return false;
    }
    
    public boolean isAutoIncrement() {
        return false;
    }
    
    public Boolean getBooleanValue(Session session) throws SQLException {
        // TODO optimization: is this required?
        return getValue(session).getBoolean();
    }

    public void createIndexConditions(Session session, TableFilter filter) throws SQLException {
        // default is do nothing
    }

    public String getColumnName() {
        return getAlias();
    }

    public String getSchemaName() {
        return null;
    }    

    public String getTableName() {
        return null;
    }
    
    public int getNullable() {
        return Column.NULLABLE_UNKNOWN;
    }

    public String getTableAlias() {
        return null;
    }

    public String getAlias() {
        return StringUtils.unEnclose(getSQL());
    }

    public boolean isWildcard() {
        return false;
    }
    
    public Expression getNonAliasExpression() {
        return this;
    }
    
    public void addFilterConditions(TableFilter filter, boolean outerJoin) {
        if (!addedToFilter && !outerJoin && isEverything(ExpressionVisitor.EVALUATABLE)) {
            filter.addFilterCondition(this, false);
            addedToFilter = true;
        }
    }
}
