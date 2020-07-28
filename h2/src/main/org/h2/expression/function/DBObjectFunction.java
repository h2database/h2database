/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.OperationN;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;
import org.h2.value.ValueVarchar;

/**
 * DB_OBJECT_ID() and DB_OBJECT_SQL() functions.
 */
public final class DBObjectFunction extends OperationN implements NamedExpression {

    /**
     * DB_OBJECT_ID() (non-standard).
     */
    public static final int DB_OBJECT_ID = 0;

    /**
     * DB_OBJECT_SQL() (non-standard).
     */
    public static final int DB_OBJECT_SQL = DB_OBJECT_ID + 1;

    private static final String[] NAMES = { //
            "DB_OBJECT_ID", "DB_OBJECT_SQL" //
    };

    private final int function;

    public DBObjectFunction(Expression objectType, Expression arg1, Expression arg2, int function) {
        super(arg2 == null ? new Expression[] { objectType, arg1, } : new Expression[] { objectType, arg1, arg2 });
        this.function = function;
    }

    @Override
    public Value getValue(SessionLocal session) {
        session.getUser().checkAdmin();
        String objectType = args[0].getValue(session).getString();
        if (objectType == null) {
            return ValueNull.INSTANCE;
        }
        DbObject object;
        if (args.length == 3) {
            Schema schema = session.getDatabase().findSchema(args[1].getValue(session).getString());
            if (schema == null) {
                return ValueNull.INSTANCE;
            }
            String objectName = args[2].getValue(session).getString();
            if (objectName == null) {
                return ValueNull.INSTANCE;
            }
            switch (objectType) {
            case "CONSTANT":
                object = schema.findConstant(objectName);
                break;
            case "CONSTRAINT":
                object = schema.findConstraint(session, objectName);
                break;
            case "DOMAIN":
                object = schema.findDomain(objectName);
                break;
            case "INDEX":
                object = schema.findIndex(session, objectName);
                break;
            case "ROUTINE":
                object = schema.findFunction(objectName);
                if (object == null) {
                    object = schema.findAggregate(objectName);
                }
                break;
            case "SEQUENCE":
                object = schema.findSequence(objectName);
                break;
            case "SYNONYM":
                object = schema.getSynonym(objectName);
                break;
            case "TABLE":
                object = schema.findTableOrView(session, objectName);
                break;
            case "TRIGGER":
                object = schema.findTrigger(objectName);
                break;
            default:
                return ValueNull.INSTANCE;
            }
        } else {
            String objectName = args[1].getValue(session).getString();
            if (objectName == null) {
                return ValueNull.INSTANCE;
            }
            Database database = session.getDatabase();
            switch (objectType) {
            case "ROLE":
                object = database.findRole(objectName);
                break;
            case "SETTING":
                object = database.findSetting(objectName);
                break;
            case "SCHEMA":
                object = database.findSchema(objectName);
                break;
            case "USER":
                object = database.findUser(objectName);
                break;
            default:
                return ValueNull.INSTANCE;
            }
        }
        if (object == null) {
            return ValueNull.INSTANCE;
        }
        switch (function) {
        case DB_OBJECT_ID:
            return ValueInteger.get(object.getId());
        case DB_OBJECT_SQL:
            String sql = object.getCreateSQLForMeta();
            return sql != null ? ValueVarchar.get(sql, session) : ValueNull.INSTANCE;
        default:
            throw DbException.throwInternalError("function=" + function);
        }
    }

    @Override
    public Expression optimize(SessionLocal session) {
        optimizeArguments(session, false);
        type = function == DB_OBJECT_ID ? TypeInfo.TYPE_INTEGER : TypeInfo.TYPE_VARCHAR;
        return this;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        return writeExpressions(builder.append(getName()).append('('), args, sqlFlags).append(')');
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.DETERMINISTIC:
            return false;
        }
        return super.isEverything(visitor);
    }

    @Override
    public String getName() {
        return NAMES[function];
    }

}
