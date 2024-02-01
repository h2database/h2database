/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import org.h2.engine.Constants;
import org.h2.engine.SessionLocal;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Operation0;
import org.h2.message.DbException;
import org.h2.util.Utils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;
import org.h2.value.ValueVarchar;

/**
 * Database or session information function.
 */
public final class SysInfoFunction extends Operation0 implements NamedExpression {

    /**
     * AUTOCOMMIT().
     */
    public static final int AUTOCOMMIT = 0;

    /**
     * DATABASE_PATH().
     */
    public static final int DATABASE_PATH = AUTOCOMMIT + 1;

    /**
     * H2VERSION().
     */
    public static final int H2VERSION = DATABASE_PATH + 1;

    /**
     * LOCK_MODE().
     */
    public static final int LOCK_MODE = H2VERSION + 1;

    /**
     * LOCK_TIMEOUT().
     */
    public static final int LOCK_TIMEOUT = LOCK_MODE + 1;

    /**
     * MEMORY_FREE().
     */
    public static final int MEMORY_FREE = LOCK_TIMEOUT + 1;

    /**
     * MEMORY_USED().
     */
    public static final int MEMORY_USED = MEMORY_FREE + 1;

    /**
     * READONLY().
     */
    public static final int READONLY = MEMORY_USED + 1;

    /**
     * SESSION_ID().
     */
    public static final int SESSION_ID = READONLY + 1;

    /**
     * TRANSACTION_ID().
     */
    public static final int TRANSACTION_ID = SESSION_ID + 1;

    private static final int[] TYPES = { Value.BOOLEAN, Value.VARCHAR, Value.VARCHAR, Value.INTEGER, Value.INTEGER,
            Value.BIGINT, Value.BIGINT, Value.BOOLEAN, Value.INTEGER, Value.VARCHAR };

    private static final String[] NAMES = { "AUTOCOMMIT", "DATABASE_PATH", "H2VERSION", "LOCK_MODE", "LOCK_TIMEOUT",
            "MEMORY_FREE", "MEMORY_USED", "READONLY", "SESSION_ID", "TRANSACTION_ID" };

    /**
     * Get the name for this function id.
     *
     * @param function
     *            the function id
     * @return the name
     */
    public static String getName(int function) {
        return NAMES[function];
    }

    private final int function;

    private final TypeInfo type;

    public SysInfoFunction(int function) {
        this.function = function;
        type = TypeInfo.getTypeInfo(TYPES[function]);
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value result;
        switch (function) {
        case AUTOCOMMIT:
            result = ValueBoolean.get(session.getAutoCommit());
            break;
        case DATABASE_PATH: {
            String path = session.getDatabase().getDatabasePath();
            result = path != null ? ValueVarchar.get(path, session) : ValueNull.INSTANCE;
            break;
        }
        case H2VERSION:
            result = ValueVarchar.get(Constants.VERSION, session);
            break;
        case LOCK_MODE:
            result = ValueInteger.get(session.getDatabase().getLockMode());
            break;
        case LOCK_TIMEOUT:
            result = ValueInteger.get(session.getLockTimeout());
            break;
        case MEMORY_FREE:
            session.getUser().checkAdmin();
            result = ValueBigint.get(Utils.getMemoryFree());
            break;
        case MEMORY_USED:
            session.getUser().checkAdmin();
            result = ValueBigint.get(Utils.getMemoryUsed());
            break;
        case READONLY:
            result = ValueBoolean.get(session.getDatabase().isReadOnly());
            break;
        case SESSION_ID:
            result = ValueInteger.get(session.getId());
            break;
        case TRANSACTION_ID:
            result = session.getTransactionId();
            break;
        default:
            throw DbException.getInternalError("function=" + function);
        }
        return result;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        return builder.append(getName()).append("()");
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.DETERMINISTIC:
            return false;
        }
        return true;
    }

    @Override
    public TypeInfo getType() {
        return type;
    }

    @Override
    public int getCost() {
        return 1;
    }

    @Override
    public String getName() {
        return NAMES[function];
    }

}
