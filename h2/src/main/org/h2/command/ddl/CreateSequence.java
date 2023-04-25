/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.schema.Sequence;

/**
 * This class represents the statement CREATE SEQUENCE.
 */
public class CreateSequence extends SchemaOwnerCommand {

    private String sequenceName;

    private boolean ifNotExists;

    private SequenceOptions options;

    private boolean belongsToTable;

    public CreateSequence(SessionLocal session, Schema schema) {
        super(session, schema);
        transactional = true;
    }

    public void setSequenceName(String sequenceName) {
        this.sequenceName = sequenceName;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setOptions(SequenceOptions options) {
        this.options = options;
    }

    @Override
    long update(Schema schema) {
        Database db = getDatabase();
        if (schema.findSequence(sequenceName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.SEQUENCE_ALREADY_EXISTS_1, sequenceName);
        }
        int id = getObjectId();
        Sequence sequence = new Sequence(session, schema, id, sequenceName, options, belongsToTable);
        db.addSchemaObject(session, sequence);
        return 0;
    }

    public void setBelongsToTable(boolean belongsToTable) {
        this.belongsToTable = belongsToTable;
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_SEQUENCE;
    }

}
