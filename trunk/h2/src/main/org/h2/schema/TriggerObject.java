/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.schema;

import java.sql.Connection;
import java.sql.SQLException;

import org.h2.api.Trigger;
import org.h2.command.Parser;
import org.h2.constant.ErrorCode;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.result.Row;
import org.h2.table.Table;
import org.h2.value.DataType;
import org.h2.value.Value;

/**
 *A trigger is created using the statement
 * CREATE TRIGGER
 */
public class TriggerObject extends SchemaObjectBase {

    public static final int DEFAULT_QUEUE_SIZE = 1024;

    private boolean before;
    private int typeMask;
    private boolean rowBased;
    // TODO trigger: support queue and noWait = false as well
    private int queueSize = DEFAULT_QUEUE_SIZE;
    private boolean noWait;
    private Table table;
    private String triggerClassName;
    private Trigger triggerCallback;

    public TriggerObject(Schema schema, int id, String name, Table table) {
        super(schema, id, name, Trace.TRIGGER);
        this.table = table;
        setTemporary(table.getTemporary());
    }

    public void setBefore(boolean before) {
        this.before = before;
    }

    private synchronized void load(Session session) throws SQLException {
        if (triggerCallback != null) {
            return;
        }
        try {
            Connection c2 = session.createConnection(false);
            Object obj = session.getDatabase().loadUserClass(triggerClassName).newInstance();
            triggerCallback = (Trigger) obj;
            triggerCallback.init(c2, getSchema().getName(), getName(), table.getName(), before, typeMask);
        } catch (Throwable e) {
            throw Message.getSQLException(ErrorCode.ERROR_CREATING_TRIGGER_OBJECT_3, new String[] { getName(),
                    triggerClassName, e.toString() }, e);
        }
    }

    public void setTriggerClassName(Session session, String triggerClassName, boolean force) throws SQLException {
        this.triggerClassName = triggerClassName;
        try {
            load(session);
        } catch (SQLException e) {
            if (!force) {
                throw e;
            }
        }
    }

    public void fire(Session session, boolean beforeAction) throws SQLException {
        if (rowBased || before != beforeAction) {
            return;
        }
        load(session);
        Connection c2 = session.createConnection(false);
        try {
            triggerCallback.fire(c2, null, null);
        } catch (Throwable e) {
            throw Message.getSQLException(ErrorCode.ERROR_EXECUTING_TRIGGER_3, new String[] { getName(),
                    triggerClassName, e.toString() }, e);
        }
    }

    private Object[] convertToObjectList(Row row) throws SQLException {
        if (row == null) {
            return null;
        }
        int len = row.getColumnCount();
        Object[] list = new Object[len];
        for (int i = 0; i < len; i++) {
            list[i] = row.getValue(i).getObject();
        }
        return list;
    }

    /**
     * Call the fire method of the user defined trigger class.
     * 
     * @param session the session
     * @param oldRow the old row
     * @param newRow the new row
     * @param beforeAction true if this method is called before the operation is
     *            applied
     */
    public void fireRow(Session session, Row oldRow, Row newRow, boolean beforeAction) throws SQLException {
        if (!rowBased || before != beforeAction) {
            return;
        }
        Object[] oldList;
        Object[] newList;
        boolean fire = false;
        if ((typeMask & Trigger.INSERT) != 0) {
            if (oldRow == null && newRow != null) {
                fire = true;
            }
        }
        if ((typeMask & Trigger.UPDATE) != 0) {
            if (oldRow != null && newRow != null) {
                fire = true;
            }
        }
        if ((typeMask & Trigger.DELETE) != 0) {
            if (oldRow != null && newRow == null) {
                fire = true;
            }
        }
        if (!fire) {
            return;
        }
        oldList = convertToObjectList(oldRow);
        newList = convertToObjectList(newRow);
        Object[] newListBackup;
        if (before && newList != null) {
            newListBackup = new Object[newList.length];
            for (int i = 0; i < newList.length; i++) {
                newListBackup[i] = newList[i];
            }
        } else {
            newListBackup = null;
        }
        Connection c2 = session.createConnection(false);
        boolean old = session.getAutoCommit();
        try {
            session.setAutoCommit(false);
            triggerCallback.fire(c2, oldList, newList);
            if (newListBackup != null) {
                for (int i = 0; i < newList.length; i++) {
                    Object o = newList[i];
                    if (o != newListBackup[i]) {
                        Value v = DataType.convertToValue(session, o, Value.UNKNOWN);
                        newRow.setValue(i, v);
                    }
                }
            }
        } finally {
            session.setAutoCommit(old);
        }
    }

    public void setTypeMask(int typeMask) {
        this.typeMask = typeMask;
    }

    public void setRowBased(boolean rowBased) {
        this.rowBased = rowBased;
    }

    public void setQueueSize(int size) {
        this.queueSize = size;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public void setNoWait(boolean noWait) {
        this.noWait = noWait;
    }

    public boolean getNoWait() {
        return noWait;
    }

    public String getDropSQL() {
        return null;
    }

    public String getCreateSQLForCopy(Table table, String quotedName) {
        StringBuffer buff = new StringBuffer();
        buff.append("CREATE FORCE TRIGGER ");
        buff.append(quotedName);
        if (before) {
            buff.append(" BEFORE ");
        } else {
            buff.append(" AFTER ");
        }
        buff.append(getTypeNameList());
        buff.append(" ON ");
        buff.append(table.getSQL());
        if (rowBased) {
            buff.append(" FOR EACH ROW");
        }
        if (noWait) {
            buff.append(" NOWAIT");
        } else {
            buff.append(" QUEUE ");
            buff.append(queueSize);
        }
        buff.append(" CALL ");
        buff.append(Parser.quoteIdentifier(triggerClassName));
        return buff.toString();
    }

    public String getTypeNameList() {
        StringBuffer buff = new StringBuffer();
        if ((typeMask & Trigger.INSERT) != 0) {
            buff.append("INSERT");
        }
        if ((typeMask & Trigger.UPDATE) != 0) {
            if (buff.length() > 0) {
                buff.append(", ");
            }
            buff.append("UPDATE");
        }
        if ((typeMask & Trigger.DELETE) != 0) {
            if (buff.length() > 0) {
                buff.append(", ");
            }
            buff.append("DELETE");
        }
        return buff.toString();
    }

    public String getCreateSQL() {
        return getCreateSQLForCopy(table, getSQL());
    }

    public int getType() {
        return DbObject.TRIGGER;
    }

    public void removeChildrenAndResources(Session session) throws SQLException {
        table.removeTrigger(session, this);
        database.removeMeta(session, getId());
        table = null;
        triggerClassName = null;
        triggerCallback = null;
        invalidate();
    }

    public void checkRename() {
        // nothing to do
    }

    public Table getTable() {
        return table;
    }

    public boolean getBefore() {
        return before;
    }

    public String getTriggerClassName() {
        return triggerClassName;
    }

}
