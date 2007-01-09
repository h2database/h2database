/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.schema;

import java.sql.SQLException;

import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.jdbc.JdbcSQLException;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.table.Table;

public class Sequence extends SchemaObject {
    private static final int BLOCK_INCREMENT = 32;
    private long value = 1;
    private long valueWithMargin;
    private long increment = 1;
    private boolean belongsToTable; 

    public Sequence(Schema schema, int id, String name, boolean belongsToTable) {
        super(schema, id, name, Trace.SEQUENCE);
        this.belongsToTable = belongsToTable;
    }

    public synchronized void setStartValue(long value) {
        this.value = value;
        this.valueWithMargin = value;
    }
    
    public boolean getBelongsToTable() {
        return belongsToTable;
    }

    public long getIncrement() {
        return increment;
    }

    public void setIncrement(long inc) throws JdbcSQLException {
        if(increment == 0) {
            throw Message.getSQLException(Message.INVALID_VALUE_2, new String[]{"0", "INCREMENT"}, null);
        }
        this.increment = inc;
    }
    
    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw Message.getInternalError();
    }    

    public synchronized String getCreateSQL() {
        StringBuffer buff = new StringBuffer();
        buff.append("CREATE SEQUENCE ");
        buff.append(getSQL());
        buff.append(" START WITH ");
        buff.append(value);
        if(increment != 1) {
            buff.append(" INCREMENT BY ");
            buff.append(increment);
        }
        if(belongsToTable) {
            buff.append(" BELONGS_TO_TABLE");
        }
        return buff.toString();
    }

    public synchronized long getNext() throws SQLException {
        if((increment > 0 && value >= valueWithMargin) || (increment < 0 && value <= valueWithMargin)) {
            valueWithMargin += increment*BLOCK_INCREMENT;
            flush();
        }
        long v = value;
        value += increment;
        return v;
    }
    
    public void flush() throws SQLException {
        // can not use the session, because it must be committed immediately 
        // otherwise other threads can not access the sys table.
        Session s = database.getSystemSession();
        synchronized(this) {
            // just for this case, use the value with the margin for the script
            long realValue = value;
            try {
                value = valueWithMargin;
                database.update(s, this);
            } finally {
                value = realValue;
            }
        }
        s.commit();
    }

    public void close() throws SQLException {
        valueWithMargin = value;
        flush();
    }

    public int getType() {
        return DbObject.SEQUENCE;
    }

    public void removeChildrenAndResources(Session session) {
        invalidate();
    }

    public void checkRename() {
        // nothing to do
    }

    public long getCurrentValue() {
        return value - increment;
    }

    public void setBelongsToTable(boolean b) {
        this.belongsToTable = b;
    }

}
