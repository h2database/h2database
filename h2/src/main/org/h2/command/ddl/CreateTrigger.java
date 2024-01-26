/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.api.Trigger;
import org.h2.command.CommandInterface;
import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.schema.TriggerObject;
import org.h2.table.Table;

/**
 * This class represents the statement
 * CREATE TRIGGER
 */
public class CreateTrigger extends SchemaCommand {

    private String triggerName;
    private boolean ifNotExists;

    private boolean insteadOf;
    private boolean before;
    private int typeMask;
    private boolean rowBased;
    private int queueSize = TriggerObject.DEFAULT_QUEUE_SIZE;
    private boolean noWait;
    private String tableName;
    private String triggerClassName;
    private String triggerSource;
    private boolean force;
    private boolean onRollback;

    public CreateTrigger(SessionLocal session, Schema schema) {
        super(session, schema);
    }

    public void setInsteadOf(boolean insteadOf) {
        this.insteadOf = insteadOf;
    }

    public void setBefore(boolean before) {
        this.before = before;
    }

    public void setTriggerClassName(String triggerClassName) {
        this.triggerClassName = triggerClassName;
    }

    public void setTriggerSource(String triggerSource) {
        this.triggerSource = triggerSource;
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

    public void setNoWait(boolean noWait) {
        this.noWait = noWait;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setTriggerName(String name) {
        this.triggerName = name;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    @Override
    public long update() {
        session.getUser().checkAdmin();
        Database db = getDatabase();
        if (getSchema().findTrigger(triggerName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(
                    ErrorCode.TRIGGER_ALREADY_EXISTS_1,
                    triggerName);
        }
        if ((typeMask & Trigger.SELECT) != 0) {
            if (rowBased) {
                throw DbException.get(ErrorCode.INVALID_TRIGGER_FLAGS_1, "SELECT + FOR EACH ROW");
            }
            if (onRollback) {
                throw DbException.get(ErrorCode.INVALID_TRIGGER_FLAGS_1, "SELECT + ROLLBACK");
            }
        } else if ((typeMask & (Trigger.INSERT | Trigger.UPDATE | Trigger.DELETE)) == 0) {
            if (onRollback) {
                throw DbException.get(ErrorCode.INVALID_TRIGGER_FLAGS_1, "(!INSERT & !UPDATE & !DELETE) + ROLLBACK");
            }
            throw DbException.getInternalError();
        }
        int id = getObjectId();
        Table table = getSchema().getTableOrView(session, tableName);
        TriggerObject trigger = new TriggerObject(getSchema(), id, triggerName, table);
        trigger.setInsteadOf(insteadOf);
        trigger.setBefore(before);
        trigger.setNoWait(noWait);
        trigger.setQueueSize(queueSize);
        trigger.setRowBased(rowBased);
        trigger.setTypeMask(typeMask);
        trigger.setOnRollback(onRollback);
        if (this.triggerClassName != null) {
            trigger.setTriggerClassName(triggerClassName, force);
        } else {
            trigger.setTriggerSource(triggerSource, force);
        }
        db.addSchemaObject(session, trigger);
        table.addTrigger(trigger);
        return 0;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    public void setOnRollback(boolean onRollback) {
        this.onRollback = onRollback;
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_TRIGGER;
    }

}
