/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;

import org.h2.api.AggregateFunction;
import org.h2.command.Parser;
import org.h2.command.dml.Select;
import org.h2.constant.ErrorCode;
import org.h2.engine.Session;
import org.h2.engine.UserAggregate;
import org.h2.message.Message;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.util.StatementBuilder;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * This class wraps a user-defined aggregate.
 */
public class JavaAggregate extends Expression {

    private final UserAggregate userAggregate;
    private final Select select;
    private AggregateFunction aggregate;
    private Expression[] args;
    private int[] argTypes;
    private int dataType;
    private Connection userConnection;
    private int lastGroupRowId;

    public JavaAggregate(UserAggregate userAggregate, Expression[] args, Select select) {
        this.userAggregate = userAggregate;
        this.args = args;
        this.select = select;
    }

    public int getCost() {
        int cost = 5;
        for (Expression e : args) {
            cost += e.getCost();
        }
        return cost;
    }

    public long getPrecision() {
        return Integer.MAX_VALUE;
    }

    public int getDisplaySize() {
        return Integer.MAX_VALUE;
    }

    public int getScale() {
        return DataType.getDataType(dataType).defaultScale;
    }

    public String getSQL() {
        StatementBuilder buff = new StatementBuilder();
        buff.append(Parser.quoteIdentifier(userAggregate.getName())).append('(');
        for (Expression e : args) {
            buff.appendExceptFirst(", ");
            buff.append(e.getSQL());
        }
        return buff.append(')').toString();
    }

    public int getType() {
        return dataType;
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        switch(visitor.getType()) {
        case ExpressionVisitor.DETERMINISTIC:
            // TODO optimization: some functions are deterministic, but we don't
            // know (no setting for that)
            return false;
        case ExpressionVisitor.GET_DEPENDENCIES:
            visitor.addDependency(userAggregate);
            break;
        case ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL:
            // user defined aggregate functions can not be optimized
            return false;
        default:
        }
        for (Expression e : args) {
            if (e != null && !e.isEverything(visitor)) {
                return false;
            }
        }
        return true;
    }

    public void mapColumns(ColumnResolver resolver, int level) throws SQLException {
        for (int i = 0; i < args.length; i++) {
            args[i].mapColumns(resolver, level);
        }
    }

    public Expression optimize(Session session) throws SQLException {
        userConnection = session.createConnection(false);
        argTypes = new int[args.length];
        int[] argSqlTypes = new int[args.length];
        for (int i = 0; i < args.length; i++) {
            Expression expr = args[i];
            args[i] = expr.optimize(session);
            int type = expr.getType();
            argTypes[i] = type;
            argSqlTypes[i] = DataType.convertTypeToSQLType(type);
        }
        aggregate = getInstance();
        dataType = DataType.convertSQLTypeToValueType(aggregate.getType(argSqlTypes));
        return this;
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        for (Expression e : args) {
            e.setEvaluatable(tableFilter, b);
        }
    }

    private AggregateFunction getInstance() throws SQLException {
        AggregateFunction agg = userAggregate.getInstance();
        agg.init(userConnection);
        return agg;
    }

    public Value getValue(Session session) throws SQLException {
        HashMap<Expression, Object> group = select.getCurrentGroup();
        if (group == null) {
            throw Message.getSQLException(ErrorCode.INVALID_USE_OF_AGGREGATE_FUNCTION_1, getSQL());
        }
        AggregateFunction agg = (AggregateFunction) group.get(this);
        if (agg == null) {
            agg = getInstance();
        }
        Object obj = agg.getResult();
        if (obj == null) {
            return ValueNull.INSTANCE;
        }
        return DataType.convertToValue(session, obj, dataType);
    }

    public void updateAggregate(Session session) throws SQLException {
        HashMap<Expression, Object> group = select.getCurrentGroup();
        if (group == null) {
            // this is a different level (the enclosing query)
            return;
        }

        int groupRowId = select.getCurrentGroupRowId();
        if (lastGroupRowId == groupRowId) {
            // already visited
            return;
        }
        lastGroupRowId = groupRowId;

        AggregateFunction agg = (AggregateFunction) group.get(this);
        if (agg == null) {
            agg = getInstance();
            group.put(this, agg);
        }
        Object[] argValues = new Object[args.length];
        Object arg = null;
        for (int i = 0; i < args.length; i++) {
            Value v = args[i].getValue(session);
            v = v.convertTo(argTypes[i]);
            arg = v.getObject();
            argValues[i] = arg;
        }
        if (args.length == 1) {
            agg.add(arg);
        } else {
            agg.add(argValues);
        }
    }

}
