/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;

import org.h2.engine.Database;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.message.Message;
import org.h2.schema.Schema;

public class AlterIndexRename extends SchemaCommand {

    private Index oldIndex;
    private String newIndexName;

    public AlterIndexRename(Session session, Schema schema) {
        super(session, schema);
    }

    public void setOldIndex(Index index) {
        oldIndex = index;
    }

    public void setNewName(String name) {
        newIndexName = name;
    }

    public int update() throws SQLException {
        session.commit(true);
        Database db = session.getDatabase();
        if(getSchema().findIndex(newIndexName) != null || newIndexName.equals(oldIndex.getName())) {
            throw Message.getSQLException(Message.INDEX_ALREADY_EXISTS_1, newIndexName);
        }
        session.getUser().checkRight(oldIndex.getTable(), Right.ALL);
        db.renameSchemaObject(session, oldIndex, newIndexName);
        return 0;
    }
    
}
