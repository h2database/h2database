/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.message.Message;
import org.h2.schema.Schema;
import org.h2.schema.Sequence;

/**
 * This class represents the statement
 * CREATE SEQUENCE
 */
public class CreateSequence extends SchemaCommand {

    private String sequenceName;
    private boolean ifNotExists;
    private Expression start;
    private Expression increment;
    private Expression cacheSize;
    private boolean belongsToTable;

    public CreateSequence(Session session, Schema schema) {
        super(session, schema);
    }

    public void setSequenceName(String sequenceName) {
        this.sequenceName = sequenceName;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public int update() throws SQLException {
        session.commit(true);
        Database db = session.getDatabase();
        if (getSchema().findSequence(sequenceName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw Message.getSQLException(ErrorCode.SEQUENCE_ALREADY_EXISTS_1, sequenceName);
        }
        int id = getObjectId(false, true);
        Sequence sequence = new Sequence(getSchema(), id, sequenceName, belongsToTable);
        sequence.setStartValue(getLong(start, 1));
        sequence.setIncrement(getLong(increment, 1));
        sequence.setCacheSize(getLong(cacheSize, Sequence.DEFAULT_CACHE_SIZE));
        db.addSchemaObject(session, sequence);
        return 0;
    }

    private long getLong(Expression expr, long defaultValue) throws SQLException {
        if (expr == null) {
            return defaultValue;
        }
        return expr.optimize(session).getValue(session).getLong();
    }

    public void setStartWith(Expression start) {
        this.start = start;
    }

    public void setIncrement(Expression increment) {
        this.increment = increment;
    }

    public void setBelongsToTable(boolean belongsToTable) {
        this.belongsToTable = belongsToTable;
    }

    public void setCacheSize(Expression cacheSize) {
        this.cacheSize = cacheSize;
    }

}
