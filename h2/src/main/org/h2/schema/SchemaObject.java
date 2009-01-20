/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.schema;

import org.h2.engine.DbObject;

/**
 * Any database object that is stored in a schema.
 */
public interface SchemaObject extends DbObject {

    /**
     * Get the schema in which this object is defined
     *
     * @return the schema
     */
    Schema getSchema();
}
