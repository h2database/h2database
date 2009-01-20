/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
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
     * @param schema the schema
     * @param id the object id
     * @param name the name
     * @param traceModule the trace module name
     */
    protected void initSchemaObjectBase(Schema schema, int id, String name, String traceModule) {
        initDbObjectBase(schema.getDatabase(), id, name, traceModule);
        this.schema = schema;
    }

    public Schema getSchema() {
        return schema;
    }

    public String getSQL() {
        return schema.getSQL() + "." + super.getSQL();
    }

}
