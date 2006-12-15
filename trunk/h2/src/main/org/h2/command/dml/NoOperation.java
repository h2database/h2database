/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import org.h2.command.Prepared;
import org.h2.engine.Session;

public class NoOperation extends Prepared {

    public NoOperation(Session session) {
        super(session);
    }
    
    public int update() {
        return 0;
    }
    
    public boolean isQuery() {
        return false;
    }
    
    public boolean isTransactional() {
        return true;
    }
    
    public boolean needRecompile() {
        return false;
    }
    
}
