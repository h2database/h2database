/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.value.DataType;
import org.h2.value.Value;

/**
 * Creates local result.
 */
public abstract class LocalResultFactory {
    /**
     * Default implementation of local result factory.
     */
    public static final LocalResultFactory DEFAULT = new DefaultLocalResultFactory();

    /**
     * Create a local result object.
     *
     * @param session the session
     * @param expressions the expression array
     * @param visibleColumnCount the number of visible columns
     * @return object to collect local result.
     */
    public abstract LocalResult create(Session session, Expression[] expressions, int visibleColumnCount);

    /**
     * Create a local result object.
     * @return object to collect local result.
     */
    public abstract LocalResult create();

    /**
     * Construct a local result set by reading all data from a regular result
     * set.
     *
     * @param session the session
     * @param rs the result set
     * @param maxrows the maximum number of rows to read (0 for no limit)
     * @return the local result set
     */
    public static LocalResult read(Session session, ResultSet rs, int maxrows) {
        Expression[] cols = Expression.getExpressionColumns(session, rs);
        int columnCount = cols.length;
        LocalResult result = session.getDatabase().getResultFactory().create(session, cols, columnCount);
        try {
            for (int i = 0; (maxrows == 0 || i < maxrows) && rs.next(); i++) {
                Value[] list = new Value[columnCount];
                for (int j = 0; j < columnCount; j++) {
                    int type = result.getColumnType(j);
                    list[j] = DataType.readValue(session, rs, j + 1, type);
                }
                result.addRow(list);
            }
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
        result.done();
        return result;
    }

    /**
     * Default implementation of local result factory.
     */
    private static final class DefaultLocalResultFactory extends LocalResultFactory {
        /**
         *
         */
        DefaultLocalResultFactory() {
            //No-op.
        }

        @Override
        public LocalResult create(Session session, Expression[] expressions, int visibleColumnCount) {
            return new LocalResultImpl(session, expressions, visibleColumnCount);
        }

        @Override
        public LocalResult create() {
            return new LocalResultImpl();
        }
    }
}
