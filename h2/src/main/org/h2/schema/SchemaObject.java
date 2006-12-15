/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.schema;

import org.h2.engine.DbObject;

public abstract class SchemaObject extends DbObject {

    private Schema schema;
    
    protected SchemaObject(Schema schema, int id, String name, String traceModule) {
        super(schema.getDatabase(), id, name, traceModule);
        this.schema = schema;
    }
    
    public Schema getSchema() {
        return schema;
    }
    
    public String getSQL() {
        return schema.getSQL() + "." + super.getSQL();
    }
    
}
