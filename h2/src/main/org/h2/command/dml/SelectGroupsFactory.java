/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.result.LocalResult;
import org.h2.result.LocalResultFactory;
import org.h2.result.LocalResultImpl;
import org.h2.value.DataType;
import org.h2.value.Value;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Creates holder of grouped data for aggregation.
 */
public abstract class SelectGroupsFactory {
    /**
     * Default implementation of local result factory.
     */
    public static final SelectGroupsFactory DEFAULT = new SelectGroupsFactory.DefaultSelectGroupsFactory();

    /**
     * Creates new instance of grouped data.
     *
     * @param session
     *            the session
     * @param expressions
     *            the expressions
     * @param isGroupQuery
     *            is this query is a group query
     * @param groupIndex
     *            the indexes of group expressions, or null
     */
    public abstract SelectGroups create(Session session, ArrayList<Expression> expressions, boolean isGroupQuery,
                                        int[] groupIndex);

    /**
     * Default implementation of select groups factory.
     */
    private static final class DefaultSelectGroupsFactory extends SelectGroupsFactory {
        /**
         *
         */
        DefaultSelectGroupsFactory() {
            //No-op.
        }

        @Override
        public SelectGroups create(Session session, ArrayList<Expression> expressions, boolean isGroupQuery,
                                   int[] groupIndex) {
            return isGroupQuery ? new SelectGroups.Grouped(session, expressions, groupIndex)
                    : new SelectGroups.Plain(session, expressions);
        }
    }
}
