/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.constraint;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.index.Index;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.StringUtils;

/**
 * A check constraint.
 */
public class ConstraintCheck extends Constraint {

    private TableFilter filter;
    private Expression expr;

    public ConstraintCheck(Schema schema, int id, String name, Table table) {
        super(schema, id, name, table);
    }

    public String getConstraintType() {
        return Constraint.CHECK;
    }

    public void setTableFilter(TableFilter filter) {
        this.filter = filter;
    }

    public void setExpression(Expression expr) {
        this.expr = expr;
    }

    public String getCreateSQLForCopy(Table forTable, String quotedName) {
        StringBuilder buff = new StringBuilder("ALTER TABLE ");
        buff.append(forTable.getSQL()).append(" ADD CONSTRAINT ").append(quotedName);
        if (comment != null) {
            buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(comment));
        }
        buff.append(" CHECK").append(StringUtils.enclose(expr.getSQL())).append(" NOCHECK");
        return buff.toString();
    }

    public String getShortDescription() {
        return getName() + ": " + expr.getSQL();
    }

    public String  getCreateSQLWithoutIndexes() {
        return getCreateSQL();
    }

    public String getCreateSQL() {
        return getCreateSQLForCopy(table, getSQL());
    }

    public void removeChildrenAndResources(Session session) throws SQLException {
        table.removeConstraint(this);
        database.removeMeta(session, getId());
        filter = null;
        expr = null;
        table = null;
        invalidate();
    }

    public void checkRow(Session session, Table t, Row oldRow, Row newRow) throws SQLException {
        if (newRow == null) {
            return;
        }
        filter.set(newRow);
        // Both TRUE and NULL are ok
        if (Boolean.FALSE.equals(expr.getValue(session).getBoolean())) {
            throw Message.getSQLException(ErrorCode.CHECK_CONSTRAINT_VIOLATED_1, getShortDescription());
        }
    }

    public boolean usesIndex(Index index) {
        return false;
    }

    public void setIndexOwner(Index index) {
        Message.throwInternalError();
    }

    public boolean containsColumn(Column col) {
        // TODO check constraints / containsColumn: this is cheating, maybe the
        // column is not referenced
        String s = col.getSQL();
        String sql = getCreateSQL();
        return sql.indexOf(s) >= 0;
    }

    public Expression getExpression() {
        return expr;
    }

    public boolean isBefore() {
        return true;
    }

    public void checkExistingData(Session session) throws SQLException {
        if (session.getDatabase().isStarting()) {
            // don't check at startup
            return;
        }
        String sql = "SELECT 1 FROM " + filter.getTable().getSQL() + " WHERE NOT(" + expr.getSQL() + ")";
        LocalResult r = session.prepare(sql).query(1);
        if (r.next()) {
            throw Message.getSQLException(ErrorCode.CHECK_CONSTRAINT_VIOLATED_1, getName());
        }
    }

    public Index getUniqueIndex() {
        return null;
    }

    public void rebuild() {
        // nothing to do
    }

}
