/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: James Moger
 */
package org.h2.jaqu;

import org.h2.jaqu.Table.JQColumn;
import org.h2.jaqu.Table.JQTable;

/**
 * Model class for JaQu to track db and table versions. 
 *
 */
@JQTable(name="_jq_versions", primaryKey="schemaName tableName", memoryTable=true)
public class DbVersion {
    
    @JQColumn(name="schemaName", allowNull=false)
    String schema = "";
    
    @JQColumn(name="tableName", allowNull = false)
    String table = "";
    
    @JQColumn(name="version")
    Integer version;
    
    public DbVersion() {        
    }
    
    /**
     * Constructor for defining a version entry.
     * (SCHEMA="" && TABLE="") == DATABASE
     * 
     * @param version
     */
    public DbVersion(int version) {
        this.schema = "";
        this.table = "";
        this.version = version;
    }
}
