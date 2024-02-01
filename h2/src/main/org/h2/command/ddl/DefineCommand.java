/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.command.Prepared;
import org.h2.engine.SessionLocal;
import org.h2.result.ResultInterface;

/**
 * This class represents a non-transaction statement, for example a CREATE or
 * DROP.
 */
public abstract class DefineCommand extends Prepared {

    /**
     * The transactional behavior. The default is disabled, meaning the command
     * commits an open transaction.
     */
    protected boolean transactional;

    /**
     * Create a new command for the given session.
     *
     * @param session the session
     */
    DefineCommand(SessionLocal session) {
        super(session);
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public ResultInterface queryMeta() {
        return null;
    }

    public void setTransactional(boolean transactional) {
        this.transactional = transactional;
    }

    @Override
    public boolean isTransactional() {
        return transactional;
    }

    @Override
    public boolean isRetryable() {
        return false;
    }

}
