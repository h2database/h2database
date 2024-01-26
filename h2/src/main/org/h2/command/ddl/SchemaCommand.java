/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.engine.SessionLocal;
import org.h2.schema.Schema;

/**
 * This class represents a non-transaction statement that involves a schema.
 */
public abstract class SchemaCommand extends DefineCommand {

    private final Schema schema;

    /**
     * Create a new command.
     *
     * @param session the session
     * @param schema the schema
     */
    public SchemaCommand(SessionLocal session, Schema schema) {
        super(session);
        this.schema = schema;
    }

    /**
     * Get the schema
     *
     * @return the schema
     */
    protected final Schema getSchema() {
        return schema;
    }

}
