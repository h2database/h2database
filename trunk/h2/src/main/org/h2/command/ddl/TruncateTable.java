/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;

import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.schema.Schema;
import org.h2.table.Table;

public class TruncateTable extends SchemaCommand {

    private String tableName;

    public TruncateTable(Session session, Schema schema) {
        super(session, schema);
    }
    
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    public int update() throws SQLException {
        session.commit();
        Table table = getSchema().getTableOrView(session, tableName);
        if(!table.canTruncate()) {
            throw Message.getSQLException(Message.CANT_TRUNCATE_1, tableName);
        } else {
            session.getUser().checkRight(table, Right.DELETE);
            table.lock(session, true);
            table.truncate(session);
        }
        return 0;
    }

}
