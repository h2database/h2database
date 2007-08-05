/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.constraint;

import java.sql.SQLException;

import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.index.Index;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.StringUtils;

/**
 * @author Thomas
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
    
    public String getCreateSQLForCopy(Table table, String quotedName) {
        StringBuffer buff = new StringBuffer();
        buff.append("ALTER TABLE ");
        buff.append(table.getSQL());
        buff.append(" ADD CONSTRAINT ");
        buff.append(quotedName);
        if(comment != null) {
            buff.append(" COMMENT ");
            buff.append(StringUtils.quoteStringSQL(comment));
        }
        buff.append(" CHECK");
        buff.append(StringUtils.enclose(expr.getSQL()));
        return buff.toString();
    }    
    
    public String getShortDescription() {
        StringBuffer buff = new StringBuffer();
        buff.append(getName());
        buff.append(": ");
        buff.append(expr.getSQL());
        return buff.toString();
    }
    
    public String  getCreateSQLWithoutIndexes() {
        return getCreateSQL();
    }    

    public String getCreateSQL() {
        return getCreateSQLForCopy(table, getSQL());
    }
    
    public void removeChildrenAndResources(Session session) {
        table.removeConstraint(this);        
        filter = null;
        expr = null;
        table = null;
        invalidate();
    }
    
    public void checkRow(Session session, Table t, Row oldRow, Row newRow) throws SQLException {
        if(newRow == null) {
            return;
        }
        filter.set(newRow);
        // Both TRUE and NULL are ok
        if(Boolean.FALSE.equals(expr.getValue(session).getBoolean())) {
            throw Message.getSQLException(Message.CHECK_CONSTRAINT_VIOLATED_1, getShortDescription());
        }
    }

    public boolean usesIndex(Index index) {
        return false;
    }

    public boolean containsColumn(Column col) {
        // TODO check constraints / containsColumn: this is cheating, maybe the column is not referenced
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
    
}
