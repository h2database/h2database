/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.sql.SQLException;
import org.h2.command.ddl.SchemaCommand;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.schema.Schema;
import org.h2.table.Table;

public class AlterTableSet  extends SchemaCommand {
    
    private String tableName;
    private final int type;
    private boolean checkExisting;
    
    public static final int REFERENTIAL_INTEGRITY_TRUE = 4;
    public static final int REFERENTIAL_INTEGRITY_FALSE = 5;

    public AlterTableSet(Session session, Schema schema, int type) {
        super(session, schema);
        this.type = type;
    }
    
    public void setCheckExisting(boolean b) {
        this.checkExisting = b;
    }
    
    public boolean isTransactional() {
        return true;
    }
    
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int update() throws SQLException {
        Table table = getSchema().getTableOrView(session, tableName);   
        session.getUser().checkRight(table, Right.ALL);
        table.lock(session, true, true);
        switch(type) {
        case REFERENTIAL_INTEGRITY_TRUE:
            table.setCheckForeignKeyConstraints(session, true, checkExisting);
            break;
        case REFERENTIAL_INTEGRITY_FALSE:
            table.setCheckForeignKeyConstraints(session, false, false);
            break;
        default:
            throw Message.getInternalError("type="+type);
        }
        return 0;
    }

    public LocalResult queryMeta() {
        return null;
    }
}
