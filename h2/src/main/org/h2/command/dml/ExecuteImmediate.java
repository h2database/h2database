/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.command.Prepared;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;

/**
 * This class represents the statement
 * EXECUTE IMMEDIATE.
 */
public class ExecuteImmediate extends Prepared {

    private Expression statement;

    public ExecuteImmediate(Session session, Expression statement) {
        super(session);
        this.statement = statement.optimize(session);
    }

    @Override
    public int update() {
        String sql = statement.getValue(session).getString();
        if (sql == null) {
            throw DbException.getInvalidValueException("SQL command", null);
        }
        Prepared command = session.prepare(sql);
        if (command.isQuery()) {
            throw DbException.get(ErrorCode.SYNTAX_ERROR_2, sql, "<not a query>");
        }
        return command.update();
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public int getType() {
        return CommandInterface.EXECUTE_IMMEDIATELY;
    }

    @Override
    public ResultInterface queryMeta() {
        return null;
    }

}
