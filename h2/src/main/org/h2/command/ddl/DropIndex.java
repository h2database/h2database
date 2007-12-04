/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;
import org.h2.constant.ErrorCode;
import org.h2.constraint.Constraint;
import org.h2.engine.Database;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.message.Message;
import org.h2.schema.Schema;
import org.h2.table.Table;
import org.h2.util.ObjectArray;

/**
 * This class represents the statement
 * DROP INDEX
 */
public class DropIndex extends SchemaCommand {

    private String indexName;
    private boolean ifExists;

    public DropIndex(Session session, Schema schema) {
        super(session, schema);
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public int update() throws SQLException {
        session.commit(true);
        Database db = session.getDatabase();
        Index index = getSchema().findIndex(indexName);
        if (index == null) {
            if (!ifExists) {
                throw Message.getSQLException(ErrorCode.INDEX_NOT_FOUND_1, indexName);
            }
        } else {
            Table table = index.getTable();
            ObjectArray constraints = table.getConstraints();
            for (int i = 0; constraints != null && i < constraints.size(); i++) {
                Constraint cons = (Constraint) constraints.get(i);
                if (cons.usesIndex(index)) {
                    throw Message.getSQLException(ErrorCode.INDEX_BELONGS_TO_CONSTRAINT_1, indexName);
                }
            }
            session.getUser().checkRight(index.getTable(), Right.ALL);
            index.getTable().setModified();
            db.removeSchemaObject(session, index);
        }
        return 0;
    }

}
