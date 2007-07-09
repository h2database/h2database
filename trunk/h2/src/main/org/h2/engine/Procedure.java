/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import org.h2.command.Prepared;

public class Procedure {
    private String name;
    private Prepared prepared;
    
    public void setName(String name) {
        this.name = name;
    }
    
    public String getName() {
        return name;
    }
    
    public void setPrepared(Prepared prepared) {
        this.prepared = prepared;
    }
    
    public Prepared getPrepared() {
        return prepared;
    }
    
}
