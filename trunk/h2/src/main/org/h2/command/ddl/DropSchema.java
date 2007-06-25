/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;

import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.schema.Schema;

public class DropSchema extends DefineCommand {
    
    private String schemaName;
    private boolean ifExists;

    public DropSchema(Session session) {
        super(session);
    }

    public void setSchemaName(String name) {
        this.schemaName = name;
    }

    public int update() throws SQLException {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        Schema schema = db.findSchema(schemaName);
        if(schema == null) {
            if(!ifExists) {
                throw Message.getSQLException(Message.SCHEMA_NOT_FOUND_1, schemaName);
            }
        } else {
            if(!schema.canDrop()) {
                throw Message.getSQLException(Message.SCHEMA_CAN_NOT_BE_DROPPED_1, schemaName);
            }
            db.removeDatabaseObject(session, schema);
        }
        return 0;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }
    
}
