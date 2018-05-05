/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import org.h2.api.Aggregate;
import org.h2.api.ErrorCode;
import org.h2.command.Parser;
import org.h2.command.dml.Select;
import org.h2.engine.Session;
import org.h2.engine.UserAggregate;
import org.h2.message.DbException;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.util.StatementBuilder;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueNull;

/**
 * This class wraps a user-defined aggregate.
 */
public class JavaAggregate extends Expression {

    private final UserAggregate userAggregate;
    private final Select select;
    private final Expression[] args;
    private int[] argTypes;
    private final boolean distinct;
    private Expression filterCondition;
    private int dataType;
    private Connection userConnection;
    private int lastGroupRowId;

    public JavaAggregate(UserAggregate userAggregate, Expression[] args,
            Select select, boolean distinct, Expression filterCondition) {
        this.userAggregate = userAggregate;
        this.args = args;
        this.select = select;
        this.distinct = distinct;
        this.filterCondition = filterCondition;
    }

    @Override
    public int getCost() {
        int cost = 5;
        for (Expression e : args) {
            cost += e.getCost();
        }
        if (filterCondition != null) {
            cost += filterCondition.getCost();
        }
        return cost;
    }

    @Override
    public long getPrecision() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getDisplaySize() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getScale() {
        return DataType.getDataType(dataType).defaultScale;
    }

    @Override
    public String getSQL() {
        StatementBuilder buff = new StatementBuilder();
        buff.append(Parser.quoteIdentifier(userAggregate.getName())).append('(');
        for (Expression e : args) {
            buff.appendExceptFirst(", ");
            buff.append(e.getSQL());
        }
        buff.append(')');
        if (filterCondition != null) {
            buff.append(" FILTER (WHERE ").append(filterCondition.getSQL()).append(')');
        }
        return buff.toString();
    }

    @Override
    public int getType() {
        return dataType;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.DETERMINISTIC:
            // TODO optimization: some functions are deterministic, but we don't
            // know (no setting for that)
        case ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL:
            // user defined aggregate functions can not be optimized
            return false;
        case ExpressionVisitor.GET_DEPENDENCIES:
            visitor.addDependency(userAggregate);
            break;
        default:
        }
        for (Expression e : args) {
            if (e != null && !e.isEverything(visitor)) {
                return false;
            }
        }
        return filterCondition == null || filterCondition.isEverything(visitor);
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        for (Expression arg : args) {
            arg.mapColumns(resolver, level);
        }
        if (filterCondition != null) {
            filterCondition.mapColumns(resolver, level);
        }
    }

    @Override
    public Expression optimize(Session session) {
        userConnection = session.createConnection(false);
        int len = args.length;
        argTypes = new int[len];
        for (int i = 0; i < len; i++) {
            Expression expr = args[i];
            args[i] = expr.optimize(session);
            int type = expr.getType();
            argTypes[i] = type;
        }
        try {
            Aggregate aggregate = getInstance();
            dataType = aggregate.getInternalType(argTypes);
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
        if (filterCondition != null) {
            filterCondition = filterCondition.optimize(session);
        }
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        for (Expression e : args) {
            e.setEvaluatable(tableFilter, b);
        }
        if (filterCondition != null) {
            filterCondition.setEvaluatable(tableFilter, b);
        }
    }

    private Aggregate getInstance() throws SQLException {
        Aggregate agg = userAggregate.getInstance();
        agg.init(userConnection);
        return agg;
    }

    @Override
    public Value getValue(Session session) {
        if (!select.isCurrentGroup()) {
            throw DbException.get(ErrorCode.INVALID_USE_OF_AGGREGATE_FUNCTION_1, getSQL());
        }
        try {
            Aggregate agg;
            if (distinct) {
                agg = getInstance();
                AggregateDataCollecting data = (AggregateDataCollecting) select.getCurrentGroupExprData(this);
                if (data != null) {
                    for (Value value : data.values) {
                        if (args.length == 1) {
                            agg.add(value.getObject());
                        } else {
                            Value[] values = ((ValueArray) value).getList();
                            Object[] argValues = new Object[args.length];
                            for (int i = 0, len = args.length; i < len; i++) {
                                argValues[i] = values[i].getObject();
                            }
                            agg.add(argValues);
                        }
                    }
                }
            } else {
                agg = (Aggregate) select.getCurrentGroupExprData(this);
                if (agg == null) {
                    agg = getInstance();
                }
            }
            Object obj = agg.getResult();
            if (obj == null) {
                return ValueNull.INSTANCE;
            }
            return DataType.convertToValue(session, obj, dataType);
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public void updateAggregate(Session session) {
        if (!select.isCurrentGroup()) {
            // this is a different level (the enclosing query)
            return;
        }

        int groupRowId = select.getCurrentGroupRowId();
        if (lastGroupRowId == groupRowId) {
            // already visited
            return;
        }
        lastGroupRowId = groupRowId;

        if (filterCondition != null) {
            if (!filterCondition.getBooleanValue(session)) {
                return;
            }
        }

        try {
            if (distinct) {
                AggregateDataCollecting data = (AggregateDataCollecting) select.getCurrentGroupExprData(this);
                if (data == null) {
                    data = new AggregateDataCollecting();
                    select.setCurrentGroupExprData(this, data);
                }
                Value[] argValues = new Value[args.length];
                Value arg = null;
                for (int i = 0, len = args.length; i < len; i++) {
                    arg = args[i].getValue(session);
                    arg = arg.convertTo(argTypes[i]);
                    argValues[i] = arg;
                }
                data.add(session.getDatabase(), dataType, true, args.length == 1 ? arg : ValueArray.get(argValues));
            } else {
                Aggregate agg = (Aggregate) select.getCurrentGroupExprData(this);
                if (agg == null) {
                    agg = getInstance();
                    select.setCurrentGroupExprData(this, agg);
                }
                Object[] argValues = new Object[args.length];
                Object arg = null;
                for (int i = 0, len = args.length; i < len; i++) {
                    Value v = args[i].getValue(session);
                    v = v.convertTo(argTypes[i]);
                    arg = v.getObject();
                    argValues[i] = arg;
                }
                agg.add(args.length == 1 ? arg : argValues);
            }
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

}
