/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.api.ErrorCode;
import org.h2.constraint.DomainColumnResolver;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.table.ColumnResolver;
import org.h2.util.ParserUtil;
import org.h2.value.TypeInfo;
import org.h2.value.Value;

/**
 * An expression representing a value for domain constraint.
 */
public final class DomainValueExpression extends Operation0 {

    private DomainColumnResolver columnResolver;

    public DomainValueExpression() {
    }

    @Override
    public Value getValue(SessionLocal session) {
        return columnResolver.getValue(null);
    }

    @Override
    public TypeInfo getType() {
        return columnResolver.getValueType();
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        if (resolver instanceof DomainColumnResolver) {
            columnResolver = (DomainColumnResolver) resolver;
        }
    }

    @Override
    public Expression optimize(SessionLocal session) {
        if (columnResolver == null) {
            throw DbException.get(ErrorCode.COLUMN_NOT_FOUND_1, "VALUE");
        }
        return this;
    }

    @Override
    public boolean isValueSet() {
        return columnResolver.getValue(null) != null;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        if (columnResolver != null) {
            String name = columnResolver.getColumnName();
            if (name != null) {
                return ParserUtil.quoteIdentifier(builder, name, sqlFlags);
            }
        }
        return builder.append("VALUE");
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return true;
    }

    @Override
    public int getCost() {
        return 1;
    }

}
