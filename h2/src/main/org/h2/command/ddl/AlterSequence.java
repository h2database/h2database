/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
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
import org.h2.schema.Sequence;

/**
 * This class represents the statement
 * ALTER SEQUENCE
 */
public class AlterSequence extends DefineCommand {

    private Sequence sequence;
    private Expression start;
    private Expression increment;

    public AlterSequence(Session session) {
        super(session);
    }

    public void setSequence(Sequence sequence) {
        this.sequence = sequence;
    }

    public void setStartWith(Expression start) {
        this.start = start;
    }

    public void setIncrement(Expression increment) throws SQLException {
        this.increment = increment;
    }

    public int update() throws SQLException {
        // TODO rights: what are the rights required for a sequence?
        session.commit(true);
        Database db = session.getDatabase();
        if (start != null) {
            long startValue = start.optimize(session).getValue(session).getLong();
            sequence.setStartValue(startValue);
        }
        if (increment != null) {
            long incrementValue = increment.optimize(session).getValue(session).getLong();
            if (incrementValue == 0) {
                throw Message.getSQLException(ErrorCode.INVALID_VALUE_2, new String[] { "0", "INCREMENT" });
            }
            sequence.setIncrement(incrementValue);
        }
        db.update(session, sequence);
        return 0;
    }

}
