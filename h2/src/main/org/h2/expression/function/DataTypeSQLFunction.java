/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import org.h2.engine.FunctionAlias;
import org.h2.engine.FunctionAlias.JavaMethod;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.OperationN;
import org.h2.message.DbException;
import org.h2.schema.Constant;
import org.h2.schema.Domain;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueToObjectConverter2;
import org.h2.value.ValueVarchar;

/**
 * DATA_TYPE_SQL() function.
 */
public final class DataTypeSQLFunction extends OperationN implements NamedExpression {

    public DataTypeSQLFunction(Expression objectSchema, Expression objectName, Expression objectType,
            Expression typeIdentifier) {
        super(new Expression[] { objectSchema, objectName, objectType, typeIdentifier });
    }

    @Override
    public Value getValue(SessionLocal session) {
        Schema schema = session.getDatabase().findSchema(args[0].getValue(session).getString());
        if (schema == null) {
            return ValueNull.INSTANCE;
        }
        String objectName = args[1].getValue(session).getString();
        if (objectName == null) {
            return ValueNull.INSTANCE;
        }
        String objectType = args[2].getValue(session).getString();
        if (objectType == null) {
            return ValueNull.INSTANCE;
        }
        String typeIdentifier = args[3].getValue(session).getString();
        if (typeIdentifier == null) {
            return ValueNull.INSTANCE;
        }
        TypeInfo t;
        switch (objectType) {
        case "CONSTANT": {
            Constant constant = schema.findConstant(objectName);
            if (constant == null || !typeIdentifier.equals("TYPE")) {
                return ValueNull.INSTANCE;
            }
            t = constant.getValue().getType();
            break;
        }
        case "DOMAIN": {
            Domain domain = schema.findDomain(objectName);
            if (domain == null || !typeIdentifier.equals("TYPE")) {
                return ValueNull.INSTANCE;
            }
            t = domain.getDataType();
            break;
        }
        case "ROUTINE": {
            int idx = objectName.lastIndexOf('_');
            if (idx < 0) {
                return ValueNull.INSTANCE;
            }
            FunctionAlias function = schema.findFunction(objectName.substring(0, idx));
            if (function == null) {
                return ValueNull.INSTANCE;
            }
            int ordinal;
            try {
                ordinal = Integer.parseInt(objectName.substring(idx + 1));
            } catch (NumberFormatException e) {
                return ValueNull.INSTANCE;
            }
            JavaMethod[] methods;
            try {
                methods = function.getJavaMethods();
            } catch (DbException e) {
                return ValueNull.INSTANCE;
            }
            if (ordinal < 1 || ordinal > methods.length) {
                return ValueNull.INSTANCE;
            }
            FunctionAlias.JavaMethod method = methods[ordinal - 1];
            if (typeIdentifier.equals("RESULT")) {
                t = method.getDataType();
            } else {
                try {
                    ordinal = Integer.parseInt(typeIdentifier);
                } catch (NumberFormatException e) {
                    return ValueNull.INSTANCE;
                }
                if (ordinal < 1) {
                    return ValueNull.INSTANCE;
                }
                if (!method.hasConnectionParam()) {
                    ordinal--;
                }
                Class<?>[] columnList = method.getColumnClasses();
                if (ordinal >= columnList.length) {
                    return ValueNull.INSTANCE;
                }
                t = ValueToObjectConverter2.classToType(columnList[ordinal]);
            }
            break;
        }
        case "TABLE": {
            Table table = schema.findTableOrView(session, objectName);
            if (table == null) {
                return ValueNull.INSTANCE;
            }
            int ordinal;
            try {
                ordinal = Integer.parseInt(typeIdentifier);
            } catch (NumberFormatException e) {
                return ValueNull.INSTANCE;
            }
            Column[] columns = table.getColumns();
            if (ordinal < 1 || ordinal > columns.length) {
                return ValueNull.INSTANCE;
            }
            t = columns[ordinal - 1].getType();
            break;
        }
        default:
            return ValueNull.INSTANCE;
        }
        return ValueVarchar.get(t.getSQL(DEFAULT_SQL_FLAGS));
    }

    @Override
    public Expression optimize(SessionLocal session) {
        optimizeArguments(session, false);
        type = TypeInfo.TYPE_VARCHAR;
        return this;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        return writeExpressions(builder.append(getName()).append('('), args, sqlFlags).append(')');
    }

    @Override
    public String getName() {
        return "DATA_TYPE_SQL";
    }

}
