/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.util.ObjectArray;
import org.h2.value.Value;

/**
 * This class represents a column resolver for the column list of a SELECT
 * statement. It is used to resolve select column aliases in the HAVING clause.
 * Example:
 * <p>
 * SELECT X/3 AS A, COUNT(*) FROM SYSTEM_RANGE(1, 10) GROUP BY A HAVING A>2;
 * </p>
 *
 * @author Thomas Mueller
 */
public class SelectListColumnResolver implements ColumnResolver {

    private Select select;
    private Expression[] expressions;
    private Column[] columns;

    SelectListColumnResolver(Select select) {
        this.select = select;
        int columnCount = select.getColumnCount();
        columns = new Column[columnCount];
        expressions = new Expression[columnCount];
        ObjectArray<Expression> columnList = select.getExpressions();
        for (int i = 0; i < columnCount; i++) {
            Expression expr = columnList.get(i);
            Column column = new Column(expr.getAlias(), Value.NULL);
            column.setTable(null, i);
            columns[i] = column;
            expressions[i] = expr.getNonAliasExpression();
        }
    }

    public Column[] getColumns() {
        return columns;
    }

    public String getSchemaName() {
        return null;
    }

    public Select getSelect() {
        return select;
    }

    public Column[] getSystemColumns() {
        return null;
    }

    public String getTableAlias() {
        return null;
    }

    public TableFilter getTableFilter() {
        return null;
    }

    public Value getValue(Column column) {
        return null;
    }

    public Expression optimize(ExpressionColumn expressionColumn, Column column) {
        return expressions[column.getColumnId()];
    }

}
