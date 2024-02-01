/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import org.h2.command.Command;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.message.DbException;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;

/**
 * An ABORT_SESSION() or CANCEL_SESSION() function.
 */
public final class SessionControlFunction extends Function1 {

    /**
     * ABORT_SESSION().
     */
    public static final int ABORT_SESSION = 0;

    /**
     * CANCEL_SESSION().
     */
    public static final int CANCEL_SESSION = ABORT_SESSION + 1;

    private static final String[] NAMES = { //
            "ABORT_SESSION", "CANCEL_SESSION" //
    };

    private final int function;

    public SessionControlFunction(Expression arg, int function) {
        super(arg);
        this.function = function;
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value v = arg.getValue(session);
        if (v == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        int targetSessionId = v.getInt();
        session.getUser().checkAdmin();
        loop: for (SessionLocal s : session.getDatabase().getSessions(false)) {
            if (s.getId() == targetSessionId) {
                Command c = s.getCurrentCommand();
                switch (function) {
                case ABORT_SESSION:
                    if (c != null) {
                        c.cancel();
                    }
                    s.close();
                    return ValueBoolean.TRUE;
                case CANCEL_SESSION:
                    if (c != null) {
                        c.cancel();
                        return ValueBoolean.TRUE;
                    }
                    break loop;
                default:
                    throw DbException.getInternalError("function=" + function);
                }
            }
        }
        return ValueBoolean.FALSE;
    }

    @Override
    public Expression optimize(SessionLocal session) {
        arg = arg.optimize(session);
        type = TypeInfo.TYPE_BOOLEAN;
        return this;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.DETERMINISTIC:
        case ExpressionVisitor.READONLY:
        case ExpressionVisitor.QUERY_COMPARABLE:
            return false;
        }
        return super.isEverything(visitor);
    }

    @Override
    public String getName() {
        return NAMES[function];
    }

}
