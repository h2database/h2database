/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import org.h2.command.Prepared;

public class Procedure {
    
    private final String name;
    private final Prepared prepared;
    
    public Procedure(String name, Prepared prepared) {
        this.name = name;
        this.prepared = prepared;
    }
    
    public String getName() {
        return name;
    }
    
    public Prepared getPrepared() {
        return prepared;
    }
    
}
