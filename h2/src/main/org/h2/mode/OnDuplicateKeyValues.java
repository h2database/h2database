/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mode;

import org.h2.command.dml.Update;
import org.h2.engine.SessionLocal;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Operation0;
import org.h2.message.DbException;
import org.h2.table.Column;
import org.h2.value.TypeInfo;
import org.h2.value.Value;

/**
 * VALUES(column) function for ON DUPLICATE KEY UPDATE clause.
 */
public final class OnDuplicateKeyValues extends Operation0 {

    private final Column column;

    private final Update update;

    public OnDuplicateKeyValues(Column column, Update update) {
        this.column = column;
        this.update = update;
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value v = update.getOnDuplicateKeyInsert().getOnDuplicateKeyValue(column.getColumnId());
        if (v == null) {
            throw DbException.getUnsupportedException(getTraceSQL());
        }
        return v;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        return column.getSQL(builder.append("VALUES("), sqlFlags).append(')');
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.DETERMINISTIC:
            return false;
        }
        return true;
    }

    @Override
    public TypeInfo getType() {
        return column.getType();
    }

    @Override
    public int getCost() {
        return 1;
    }

}
