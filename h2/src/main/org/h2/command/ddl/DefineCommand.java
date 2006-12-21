/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.command.Prepared;
import org.h2.engine.Session;

public abstract class DefineCommand extends Prepared {
    
    public DefineCommand(Session session) {
        super(session);
    }
    
    public boolean isTransactional() {
        return false;
    }      
    
    public boolean isReadOnly() {
        return false;
    }
    
}
