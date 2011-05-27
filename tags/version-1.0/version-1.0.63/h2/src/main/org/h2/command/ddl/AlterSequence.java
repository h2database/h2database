/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.schema.Sequence;

public class AlterSequence extends DefineCommand {

    private Sequence sequence;
    private boolean newStart;
    private long start;
    private boolean newIncrement;
    private long increment;

    public AlterSequence(Session session) {
        super(session);
    }

    public void setSequence(Sequence sequence) {
        this.sequence = sequence;
    }

    public void setStartWith(long start) {
        newStart = true;
        this.start = start;
    }

    public void setIncrement(long increment) throws SQLException {
        newIncrement = true;
        if (increment == 0) {
            throw Message.getSQLException(ErrorCode.INVALID_VALUE_2, new String[] { "0", "INCREMENT" });
        }
        this.increment = increment;
    }

    public int update() throws SQLException {
        // TODO rights: what are the rights required for a sequence?
        session.commit(true);
        Database db = session.getDatabase();
        if (newStart) {
            sequence.setStartValue(start);
        }
        if (newIncrement) {
            sequence.setIncrement(increment);
        }
        db.update(session, sequence);
        return 0;
    }

}
