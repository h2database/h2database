/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.constraint;

import java.sql.SQLException;
import org.h2.command.Parser;
import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.result.Row;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.util.StringUtils;

/**
 * A unique constraint. This object always backed by a unique index.
 */
public class ConstraintUnique extends Constraint {

    private Index index;
    private boolean indexOwner;
    private Column[] columns;

    public ConstraintUnique(Schema schema, int id, String name, Table table) {
        super(schema, id, name, table);
    }

    public String getConstraintType() {
        return Constraint.UNIQUE;
    }

    public String getCreateSQLForCopy(Table table, String quotedName) {
        return getCreateSQLForCopy(table, quotedName, true);
    }

    public String getCreateSQLForCopy(Table table, String quotedName, boolean internalIndex) {
        StringBuffer buff = new StringBuffer();
        buff.append("ALTER TABLE ");
        buff.append(table.getSQL());
        buff.append(" ADD CONSTRAINT ");
        buff.append(quotedName);
        if (comment != null) {
            buff.append(" COMMENT ");
            buff.append(StringUtils.quoteStringSQL(comment));
        }
        buff.append(" UNIQUE(");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(Parser.quoteIdentifier(columns[i].getName()));
        }
        buff.append(")");
        if (internalIndex && indexOwner && table == this.table) {
            buff.append(" INDEX ");
            buff.append(index.getSQL());
        }
        return buff.toString();
    }

    public String getShortDescription() {
        StringBuffer buff = new StringBuffer();
        buff.append(getName());
        buff.append(": ");
        buff.append("UNIQUE(");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(Parser.quoteIdentifier(columns[i].getName()));
        }
        buff.append(")");
        return buff.toString();
    }

    public String getCreateSQLWithoutIndexes() {
        return getCreateSQLForCopy(table, getSQL(), false);
    }

    public String getCreateSQL() {
        return getCreateSQLForCopy(table, getSQL());
    }

    public void setColumns(Column[] columns) {
        this.columns = columns;
    }

    public Column[] getColumns() {
        return columns;
    }

    public void setIndex(Index index, boolean isOwner) {
        this.index = index;
        this.indexOwner = isOwner;
    }

    public void removeChildrenAndResources(Session session) throws SQLException {
        table.removeConstraint(this);
        if (indexOwner) {
            database.removeSchemaObject(session, index);
        }
        index = null;
        columns = null;
        table = null;
        invalidate();
    }

    public void checkRow(Session session, Table t, Row oldRow, Row newRow) {
        // unique index check is enough
    }

    public boolean usesIndex(Index idx) {
        return idx == index;
    }

    public boolean containsColumn(Column col) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i] == col) {
                return true;
            }
        }
        return false;
    }

    public boolean isBefore() {
        return true;
    }

    public void checkExistingData(Session session) throws SQLException {
        // no need to check: when creating the unique index any problems are found
    }

}
