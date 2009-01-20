/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.command.Prepared;
import org.h2.engine.Session;
import org.h2.result.LocalResult;

/**
 * This class represents a non-transaction statement, for example a CREATE or
 * DROP.
 */
public abstract class DefineCommand extends Prepared {

    /**
     * Create a new command for the given session.
     *
     * @param session the session
     */
    public DefineCommand(Session session) {
        super(session);
    }

    public boolean isTransactional() {
        return false;
    }

    public boolean isReadOnly() {
        return false;
    }

    public LocalResult queryMeta() {
        return null;
    }

}
