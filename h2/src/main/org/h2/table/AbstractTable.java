/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import org.h2.schema.SchemaObjectBase;

/**
 * Abstract base class for tables and table synonyms.
 */
public abstract class AbstractTable extends SchemaObjectBase {

    /**
     * Resolves the "real" table behind this abstract table. For table this is the table itself for
     * a synonym this is the backing table of the synonym. This method should be used in places,
     * where synonym support is desired.
     */
    public abstract Table resolve();

    /**
     * Returns the current table or fails with an unsupported database exception for synonyms.
     * This method should be used in places that do not support the usage of synonyms.
     */
    public abstract Table asTable();

    public abstract TableType getTableType();


}
