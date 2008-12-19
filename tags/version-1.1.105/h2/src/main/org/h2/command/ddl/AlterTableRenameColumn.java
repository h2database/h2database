/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.util.ObjectArray;

/**
 * This class represents the statement
 * ALTER TABLE ALTER COLUMN RENAME
 */
public class AlterTableRenameColumn extends DefineCommand {

    private Table table;
    private Column column;
    private String newName;

    public AlterTableRenameColumn(Session session) {
        super(session);
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public void setColumn(Column column) {
        this.column = column;
    }

    public void setNewColumnName(String newName) {
        this.newName = newName;
    }

    public int update() throws SQLException {
        session.commit(true);
        Database db = session.getDatabase();
        session.getUser().checkRight(table, Right.ALL);
        table.checkSupportAlter();
        table.renameColumn(column, newName);
        table.setModified();
        db.update(session, table);
        ObjectArray children = table.getChildren();
        for (int i = 0; i < children.size(); i++) {
            DbObject child = (DbObject) children.get(i);
            if (child.getCreateSQL() != null) {
                db.update(session, child);
            }
        }
        return 0;
    }

}
