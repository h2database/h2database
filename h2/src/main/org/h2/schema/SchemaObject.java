/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.schema;

import org.h2.engine.DbObject;

/**
 * Any database object that is stored in a schema.
 */
public abstract class SchemaObject extends DbObject {

    private final Schema schema;

    /**
     * Initialize some attributes of this object.
     *
     * @param newSchema the schema
     * @param id the object id
     * @param name the name
     * @param traceModuleId the trace module id
     */
    protected SchemaObject(Schema newSchema, int id, String name, int traceModuleId) {
        super(newSchema.getDatabase(), id, name, traceModuleId);
        this.schema = newSchema;
    }

    /**
     * Get the schema in which this object is defined
     *
     * @return the schema
     */
    public final Schema getSchema() {
        return schema;
    }

    @Override
    public String getSQL(int sqlFlags) {
        return getSQL(new StringBuilder(), sqlFlags).toString();
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        schema.getSQL(builder, sqlFlags).append('.');
        return super.getSQL(builder, sqlFlags);
    }

}
