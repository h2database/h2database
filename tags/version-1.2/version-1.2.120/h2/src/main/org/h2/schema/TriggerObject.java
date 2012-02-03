/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.schema;

import java.sql.Connection;
import java.sql.SQLException;

import org.h2.api.CloseListener;
import org.h2.api.Trigger;
import org.h2.command.Parser;
import org.h2.constant.ErrorCode;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.result.Row;
import org.h2.table.Table;
import org.h2.util.ClassUtils;
import org.h2.value.DataType;
import org.h2.value.Value;

/**
 *A trigger is created using the statement
 * CREATE TRIGGER
 */
public class TriggerObject extends SchemaObjectBase {

    /**
     * The default queue size.
     */
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
        initSchemaObjectBase(schema, id, name, Trace.TRIGGER);
        this.table = table;
        setTemporary(table.isTemporary());
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
            Object obj = ClassUtils.loadUserClass(triggerClassName).newInstance();
            triggerCallback = (Trigger) obj;
            triggerCallback.init(c2, getSchema().getName(), getName(), table.getName(), before, typeMask);
        } catch (Throwable e) {
            // try again later
            triggerCallback = null;
            throw Message.getSQLException(ErrorCode.ERROR_CREATING_TRIGGER_OBJECT_3, e, getName(),
                            triggerClassName, e.toString());
        }
    }

    /**
     * Set the trigger class name and load the class if possible.
     *
     * @param session the session
     * @param triggerClassName the name of the trigger class
     * @param force whether exceptions (due to missing class or access rights)
     *            should be ignored
     */
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

    /**
     * Call the trigger class if required. This method does nothing if the
     * trigger is not defined for the given action. This method is called before
     * or after any rows have been processed, once for each statement.
     *
     * @param session the session
     * @param beforeAction if this method is called before applying the changes
     */
    public void fire(Session session, boolean beforeAction) throws SQLException {
        if (rowBased || before != beforeAction) {
            return;
        }
        load(session);
        Connection c2 = session.createConnection(false);
        boolean old = session.setCommitOrRollbackDisabled(true);
        try {
            triggerCallback.fire(c2, null, null);
        } catch (Throwable e) {
            throw Message.getSQLException(ErrorCode.ERROR_EXECUTING_TRIGGER_3, e, getName(),
                            triggerClassName, e.toString());
        } finally {
            session.setCommitOrRollbackDisabled(old);
        }
    }

    private Object[] convertToObjectList(Row row) {
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
     * Call the fire method of the user-defined trigger class if required. This
     * method does nothing if the trigger is not defined for the given action.
     * This method is called before or after a row is processed, possibly many
     * times for each statement.
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
        load(session);
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
        boolean oldDisabled = session.setCommitOrRollbackDisabled(true);
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
            session.setCommitOrRollbackDisabled(oldDisabled);
            session.setAutoCommit(old);
        }
    }

    /**
     * Set the trigger type.
     *
     * @param typeMask the type
     */
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

    public boolean isNoWait() {
        return noWait;
    }

    public String getDropSQL() {
        return null;
    }

    public String getCreateSQLForCopy(Table table, String quotedName) {
        StringBuilder buff = new StringBuilder("CREATE FORCE TRIGGER ");
        buff.append(quotedName);
        if (before) {
            buff.append(" BEFORE ");
        } else {
            buff.append(" AFTER ");
        }
        buff.append(getTypeNameList()).append(" ON ").append(table.getSQL());
        if (rowBased) {
            buff.append(" FOR EACH ROW");
        }
        if (noWait) {
            buff.append(" NOWAIT");
        } else {
            buff.append(" QUEUE ").append(queueSize);
        }
        buff.append(" CALL ").append(Parser.quoteIdentifier(triggerClassName));
        return buff.toString();
    }

    public String getTypeNameList() {
        StringBuilder buff = new StringBuilder();
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
        table.removeTrigger(this);
        database.removeMeta(session, getId());
        if (triggerCallback != null) {
            if (triggerCallback instanceof CloseListener) {
                ((CloseListener) triggerCallback).remove();
            }
        }
        table = null;
        triggerClassName = null;
        triggerCallback = null;
        invalidate();
    }

    public void checkRename() {
        // nothing to do
    }

    /**
     * Get the table of this trigger.
     *
     * @return the table
     */
    public Table getTable() {
        return table;
    }

    /**
     * Check if this is a before trigger.
     *
     * @return true if it is
     */
    public boolean isBefore() {
        return before;
    }

    /**
     * Get the trigger class name.
     *
     * @return the class name
     */
    public String getTriggerClassName() {
        return triggerClassName;
    }

    /**
     * Close the trigger.
     */
    public void close() throws SQLException {
        if (triggerCallback != null) {
            if (triggerCallback instanceof CloseListener) {
                ((CloseListener) triggerCallback).close();
            }
        }
    }

}
