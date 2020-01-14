/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.constraint;

import java.util.HashSet;

import org.h2.api.ErrorCode;
import org.h2.command.Parser;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.index.Index;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.schema.Domain;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.util.StringUtils;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * A domain constraint.
 */
public class ConstraintDomain extends Constraint {

    private Domain domain;

    private Expression expr;

    private DomainColumnResolver resolver;

    public ConstraintDomain(Schema schema, int id, String name, Domain domain) {
        super(schema, id, name, null);
        this.domain = domain;
        resolver = new DomainColumnResolver(domain.getColumn().getType());
    }

    @Override
    public Type getConstraintType() {
        return Constraint.Type.DOMAIN;
    }

    /**
     * Returns the domain of this constraint.
     *
     * @return the domain
     */
    public Domain getDomain() {
        return domain;
    }

    public void setExpression(Session session, Expression expr) {
        expr.mapColumns(resolver, 0, Expression.MAP_INITIAL);
        expr = expr.optimize(session);
        // check if the column is mapped
        synchronized (this) {
            resolver.setValue(ValueNull.INSTANCE);
            expr.getValue(session);
        }
        this.expr = expr;
    }

    @Override
    public String getCreateSQLForCopy(Table forTable, String quotedName) {
        throw DbException.throwInternalError(toString());
    }

    @Override
    public String getCreateSQLWithoutIndexes() {
        return getCreateSQL();
    }

    @Override
    public String getCreateSQL() {
        StringBuilder builder = new StringBuilder("ALTER DOMAIN ");
        domain.getSQL(builder, DEFAULT_SQL_FLAGS).append(" ADD CONSTRAINT ");
        getSQL(builder, DEFAULT_SQL_FLAGS);
        if (comment != null) {
            builder.append(" COMMENT ");
            StringUtils.quoteStringSQL(builder, comment);
        }
        builder.append(" CHECK(");
        expr.getUnenclosedSQL(builder, DEFAULT_SQL_FLAGS).append(") NOCHECK");
        return builder.toString();
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        domain.removeConstraint(this);
        database.removeMeta(session, getId());
        domain = null;
        expr = null;
        invalidate();
    }

    @Override
    public void checkRow(Session session, Table t, Row oldRow, Row newRow) {
        DbException.throwInternalError(toString());
    }

    /**
     * Check the specified value.
     *
     * @param session
     *            the session
     * @param value
     *            the value to check
     */
    public void check(Session session, Value value) {
        Value v;
        synchronized (this) {
            resolver.setValue(value);
            v = expr.getValue(session);
        }
        // Both TRUE and NULL are OK
        if (v != ValueNull.INSTANCE && !v.getBoolean()) {
            throw DbException.get(ErrorCode.CHECK_CONSTRAINT_VIOLATED_1, expr.getTraceSQL());
        }
    }

    public Expression getCheckConstraint(Session session, String columnName) {
        String sql;
        if (columnName != null) {
            synchronized (this) {
                try {
                    resolver.setColumnName(columnName);
                    sql = expr.getSQL(DEFAULT_SQL_FLAGS);
                } finally {
                    resolver.resetColumnName();
                }
            }
            return new Parser(session).parseExpression(sql);
        } else {
            synchronized (this) {
                sql = expr.getSQL(DEFAULT_SQL_FLAGS);
            }
            return new Parser(session).parseDomainConstraintExpression(sql);
        }
    }

    @Override
    public boolean usesIndex(Index index) {
        return false;
    }

    @Override
    public void setIndexOwner(Index index) {
        DbException.throwInternalError(toString());
    }

    @Override
    public HashSet<Column> getReferencedColumns(Table table) {
        HashSet<Column> columns = new HashSet<>();
        expr.isEverything(ExpressionVisitor.getColumnsVisitor(columns, table));
        return columns;
    }

    @Override
    public Expression getExpression() {
        return expr;
    }

    @Override
    public boolean isBefore() {
        return true;
    }

    @Override
    public void checkExistingData(Session session) {
        // TODO
    }

    @Override
    public void rebuild() {
        // nothing to do
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return expr.isEverything(visitor);
    }

}
