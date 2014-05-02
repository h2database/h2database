/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.schema;

import org.h2.engine.DbObjectBase;

/**
 * The base class for classes implementing SchemaObject.
 */
public abstract class SchemaObjectBase extends DbObjectBase implements SchemaObject {

    private Schema schema;

    /**
     * Initialize some attributes of this object.
     *
     * @param newSchema the schema
     * @param id the object id
     * @param name the name
     * @param traceModule the trace module name
     */
    protected void initSchemaObjectBase(Schema newSchema, int id, String name, String traceModule) {
        initDbObjectBase(newSchema.getDatabase(), id, name, traceModule);
        this.schema = newSchema;
    }

    public Schema getSchema() {
        return schema;
    }

    public String getSQL() {
        return schema.getSQL() + "." + super.getSQL();
    }

    public boolean isHidden() {
        return false;
    }

}
