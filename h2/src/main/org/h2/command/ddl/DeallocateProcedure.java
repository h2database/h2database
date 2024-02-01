/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.command.CommandInterface;
import org.h2.engine.SessionLocal;

/**
 * This class represents the statement
 * DEALLOCATE
 */
public class DeallocateProcedure extends DefineCommand {

    private String procedureName;

    public DeallocateProcedure(SessionLocal session) {
        super(session);
    }

    @Override
    public long update() {
        session.removeProcedure(procedureName);
        return 0;
    }

    public void setProcedureName(String name) {
        this.procedureName = name;
    }

    @Override
    public int getType() {
        return CommandInterface.DEALLOCATE;
    }

}
