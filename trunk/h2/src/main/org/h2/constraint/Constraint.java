/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.constraint;

import java.sql.SQLException;

import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.message.Trace;
import org.h2.result.Row;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObject;
import org.h2.table.Column;
import org.h2.table.Table;

/**
 * @author Thomas
 */
public abstract class Constraint extends SchemaObject {
    
    public static final String CHECK = "CHECK", REFERENTIAL = "REFERENTIAL", UNIQUE = "UNIQUE";
    protected Table table;
    
    public Constraint(Schema schema, int id, String name, Table table) {
        super(schema, id, name, Trace.CONSTRAINT);
        this.table = table;
        this.setTemporary(table.getTemporary());
    }
    
    public void checkRename() throws SQLException {
        // ok
    }

    public int getType() {
        return DbObject.CONSTRAINT;
    }
    
    public abstract String getConstraintType();
    public abstract void checkRow(Session session, Table t, Row oldRow, Row newRow) throws SQLException;
    public abstract boolean usesIndex(Index index);    
    public abstract boolean containsColumn(Column col);
    public abstract String  getCreateSQLWithoutIndexes();
    public abstract boolean isBefore();
    public abstract String getShortDescription();
    public abstract void checkExistingData(Session session) throws SQLException;
    
    public Table getTable() {
        return table;
    }
    
    public Table getRefTable() {
        return table;
    }
    
    public String getDropSQL() {
        return null;
    }

}
