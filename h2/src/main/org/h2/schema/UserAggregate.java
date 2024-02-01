/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.schema;

import java.sql.Connection;
import java.sql.SQLException;

import org.h2.api.Aggregate;
import org.h2.api.AggregateFunction;
import org.h2.engine.DbObject;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.util.JdbcUtils;
import org.h2.util.StringUtils;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;

/**
 * Represents a user-defined aggregate function.
 */
public final class UserAggregate extends UserDefinedFunction {

    private Class<?> javaClass;

    public UserAggregate(Schema schema, int id, String name, String className,
            boolean force) {
        super(schema, id, name, Trace.FUNCTION);
        this.className = className;
        if (!force) {
            getInstance();
        }
    }

    public Aggregate getInstance() {
        if (javaClass == null) {
            javaClass = JdbcUtils.loadUserClass(className);
        }
        Object obj;
        try {
            obj = javaClass.getDeclaredConstructor().newInstance();
            Aggregate agg;
            if (obj instanceof Aggregate) {
                agg = (Aggregate) obj;
            } else {
                agg = new AggregateWrapper((AggregateFunction) obj);
            }
            return agg;
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public String getDropSQL() {
        StringBuilder builder = new StringBuilder("DROP AGGREGATE IF EXISTS ");
        return getSQL(builder, DEFAULT_SQL_FLAGS).toString();
    }

    @Override
    public String getCreateSQL() {
        StringBuilder builder = new StringBuilder("CREATE FORCE AGGREGATE ");
        getSQL(builder, DEFAULT_SQL_FLAGS).append(" FOR ");
        return StringUtils.quoteStringSQL(builder, className).toString();
    }

    @Override
    public int getType() {
        return DbObject.AGGREGATE;
    }

    @Override
    public synchronized void removeChildrenAndResources(SessionLocal session) {
        database.removeMeta(session, getId());
        className = null;
        javaClass = null;
        invalidate();
    }

    /**
     * Wrap {@link AggregateFunction} in order to behave as
     * {@link org.h2.api.Aggregate}
     **/
    private static class AggregateWrapper implements Aggregate {
        private final AggregateFunction aggregateFunction;

        AggregateWrapper(AggregateFunction aggregateFunction) {
            this.aggregateFunction = aggregateFunction;
        }

        @Override
        public void init(Connection conn) throws SQLException {
            aggregateFunction.init(conn);
        }

        @Override
        public int getInternalType(int[] inputTypes) throws SQLException {
            int[] sqlTypes = new int[inputTypes.length];
            for (int i = 0; i < inputTypes.length; i++) {
                sqlTypes[i] = DataType.convertTypeToSQLType(TypeInfo.getTypeInfo(inputTypes[i]));
            }
            return  DataType.convertSQLTypeToValueType(aggregateFunction.getType(sqlTypes));
        }

        @Override
        public void add(Object value) throws SQLException {
            aggregateFunction.add(value);
        }

        @Override
        public Object getResult() throws SQLException {
            return aggregateFunction.getResult();
        }
    }

}
