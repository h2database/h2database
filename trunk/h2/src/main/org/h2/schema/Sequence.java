/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.schema;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.table.Table;

/**
 *A sequence is created using the statement
 * CREATE SEQUENCE
 */
public class Sequence extends SchemaObjectBase {

    /**
     * The default cache size for sequences.
     */
    public static final int DEFAULT_CACHE_SIZE = 32;

    private long value = 1;
    private long valueWithMargin;
    private long increment = 1;
    private long cacheSize = DEFAULT_CACHE_SIZE;
    private boolean belongsToTable;

    public Sequence(Schema schema, int id, String name, boolean belongsToTable) {
        initSchemaObjectBase(schema, id, name, Trace.SEQUENCE);
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

    public void setIncrement(long inc) throws SQLException {
        if (inc == 0) {
            throw Message.getSQLException(ErrorCode.INVALID_VALUE_2, "0", "INCREMENT");
        }
        this.increment = inc;
    }

    public String getDropSQL() {
        if (getBelongsToTable()) {
            return null;
        }
        return "DROP SEQUENCE IF EXISTS " + getSQL();
    }

    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw Message.throwInternalError();
    }

    public synchronized String getCreateSQL() {
        StringBuilder buff = new StringBuilder("CREATE SEQUENCE ");
        buff.append(getSQL()).append(" START WITH ").append(value);
        if (increment != 1) {
            buff.append(" INCREMENT BY ").append(increment);
        }
        if (cacheSize != DEFAULT_CACHE_SIZE) {
            buff.append(" CACHE ").append(cacheSize);
        }
        if (belongsToTable) {
            buff.append(" BELONGS_TO_TABLE");
        }
        return buff.toString();
    }

    /**
     * Get the next value for this sequence.
     *
     * @param session the session
     * @return the next value
     */
    public synchronized long getNext(Session session) throws SQLException {
        if ((increment > 0 && value >= valueWithMargin) || (increment < 0 && value <= valueWithMargin)) {
            valueWithMargin += increment * cacheSize;
            flush(session);
        }
        long v = value;
        value += increment;
        return v;
    }

    /**
     * Flush the current value, including the margin, to disk.
     *
     * @param session the session
     */
    public synchronized void flush(Session session) throws SQLException {
        Session sysSession = database.getSystemSession();
        if (session == null || !database.isSysTableLocked()) {
            // this session may not lock the sys table (except if it already has locked it)
            // because it must be committed immediately
            // otherwise other threads can not access the sys table.
            session = sysSession;
        }
        synchronized (session) {
            // just for this case, use the value with the margin for the script
            long realValue = value;
            try {
                value = valueWithMargin;
                database.update(session, this);
            } finally {
                value = realValue;
            }
            if (session == sysSession) {
                // if the system session is used,
                // the transaction must be committed immediately
                sysSession.commit(false);
            }
        }
    }

    /**
     * Flush the current value to disk and close this object.
     */
    public void close() throws SQLException {
        valueWithMargin = value;
        flush(null);
    }

    public int getType() {
        return DbObject.SEQUENCE;
    }

    public void removeChildrenAndResources(Session session) throws SQLException {
        database.removeMeta(session, getId());
        invalidate();
    }

    public void checkRename() {
        // nothing to do
    }

    public synchronized long getCurrentValue() {
        return value - increment;
    }

    public void setBelongsToTable(boolean b) {
        this.belongsToTable = b;
    }

    public void setCacheSize(long cacheSize) {
        this.cacheSize = cacheSize;
    }

    public long getCacheSize() {
        return cacheSize;
    }

}
