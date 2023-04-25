/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import org.h2.command.CommandInterface;
import org.h2.command.Prepared;
import org.h2.engine.IsolationLevel;
import org.h2.engine.SessionLocal;
import org.h2.result.ResultInterface;

/**
 * This class represents the statement SET SESSION CHARACTERISTICS
 */
public class SetSessionCharacteristics extends Prepared {

    private final IsolationLevel isolationLevel;

    public SetSessionCharacteristics(SessionLocal session, IsolationLevel isolationLevel) {
        super(session);
        this.isolationLevel = isolationLevel;
    }

    @Override
    public boolean isTransactional() {
        return false;
    }

    @Override
    public long update() {
        session.setIsolationLevel(isolationLevel);
        return 0;
    }

    @Override
    public boolean needRecompile() {
        return false;
    }

    @Override
    public ResultInterface queryMeta() {
        return null;
    }

    @Override
    public int getType() {
        return CommandInterface.SET;
    }

}
