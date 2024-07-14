/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.index.Index;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.table.Table;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;
import org.h2.value.ValueVarchar;

/**
 * DB_OBJECT_ID() and DB_OBJECT_SQL() functions.
 */
public final class DBObjectFunction extends FunctionN {

    /**
     * DB_OBJECT_ID() (non-standard).
     */
    public static final int DB_OBJECT_ID = 0;

    /**
     * DB_OBJECT_SQL() (non-standard).
     */
    public static final int DB_OBJECT_SQL = DB_OBJECT_ID + 1;

    /**
     * DB_OBJECT_SIZE() (non-standard).
     */
    public static final int DB_OBJECT_SIZE = DB_OBJECT_SQL + 1;

    /**
     * DB_OBJECT_TOTAL_SIZE() (non-standard).
     */
    public static final int DB_OBJECT_TOTAL_SIZE = DB_OBJECT_SIZE + 1;

    /**
     * DB_OBJECT_APPROXIMATE_SIZE() (non-standard).
     */
    public static final int DB_OBJECT_APPROXIMATE_SIZE = DB_OBJECT_TOTAL_SIZE + 1;

    /**
     * DB_OBJECT_APPROXIMATE_TOTAL_SIZE() (non-standard).
     */
    public static final int DB_OBJECT_APPROXIMATE_TOTAL_SIZE = DB_OBJECT_APPROXIMATE_SIZE + 1;

    private static final String[] NAMES = { //
            "DB_OBJECT_ID", "DB_OBJECT_SQL", "DB_OBJECT_SIZE", "DB_OBJECT_TOTAL_SIZE", //
            "DB_OBJECT_APPROXIMATE_SIZE", "DB_OBJECT_APPROXIMATE_TOTAL_SIZE" //
    };

    private final int function;

    public DBObjectFunction(Expression objectType, Expression arg1, Expression arg2, int function) {
        super(arg2 == null ? new Expression[] { objectType, arg1, } : new Expression[] { objectType, arg1, arg2 });
        this.function = function;
    }

    @Override
    public Value getValue(SessionLocal session, Value v1, Value v2, Value v3) {
        session.getUser().checkAdmin();
        String objectType = v1.getString();
        DbObject object;
        if (v3 != null) {
            Schema schema = session.getDatabase().findSchema(v2.getString());
            if (schema == null) {
                return ValueNull.INSTANCE;
            }
            String objectName = v3.getString();
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
                object = schema.findFunctionOrAggregate(objectName);
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
            String objectName = v2.getString();
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
        case DB_OBJECT_SQL: {
            String sql = object.getCreateSQLForMeta();
            return sql != null ? ValueVarchar.get(sql, session) : ValueNull.INSTANCE;
        }
        case DB_OBJECT_SIZE:
            return getDbObjectSize(object, false, false);
        case DB_OBJECT_TOTAL_SIZE:
            return getDbObjectSize(object, true, false);
        case DB_OBJECT_APPROXIMATE_SIZE:
            return getDbObjectSize(object, false, true);
        case DB_OBJECT_APPROXIMATE_TOTAL_SIZE:
            return getDbObjectSize(object, true, true);
        default:
            throw DbException.getInternalError("function=" + function);
        }
    }

    private static Value getDbObjectSize(DbObject object, boolean total, boolean approximate) {
        long size = 0L;
        if (object instanceof Table) {
            size = ((Table) object).getDiskSpaceUsed(total, approximate);
        } else if (object instanceof Index) {
            size = ((Index) object).getDiskSpaceUsed(approximate);
        }
        return ValueBigint.get(size);
    }

    @Override
    public Expression optimize(SessionLocal session) {
        optimizeArguments(session, false);
        switch (function) {
        case DB_OBJECT_ID:
            type = TypeInfo.TYPE_INTEGER;
            break;
        case DB_OBJECT_SQL:
            type = TypeInfo.TYPE_VARCHAR;
            break;
        case DB_OBJECT_SIZE:
        case DB_OBJECT_TOTAL_SIZE:
        case DB_OBJECT_APPROXIMATE_SIZE:
        case DB_OBJECT_APPROXIMATE_TOTAL_SIZE:
            type = TypeInfo.TYPE_BIGINT;
            break;
        default:
            throw DbException.getInternalError("function=" + function);
        }
        return this;
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
