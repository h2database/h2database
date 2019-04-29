/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.engine.Session;
import org.h2.expression.Expression;

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
     * @param session
     *            the session
     * @param expressions
     *            the expression array
     * @param visibleColumnCount
     *            the number of visible columns
     * @param resultColumnCount
     *            the number of columns including visible columns and additional
     *            virtual columns for ORDER BY and DISTINCT ON clauses
     * @return object to collect local result.
     */
    public abstract LocalResult create(Session session, Expression[] expressions, int visibleColumnCount,
            int resultColumnCount);

    /**
     * Create a local result object.
     *
     * @return object to collect local result.
     */
    public abstract LocalResult create();

    /**
     * Default implementation of local result factory.
     */
    private static final class DefaultLocalResultFactory extends LocalResultFactory {
        DefaultLocalResultFactory() {
        }

        @Override
        public LocalResult create(Session session, Expression[] expressions, int visibleColumnCount,
                int resultColumnCount) {
            return new LocalResultImpl(session, expressions, visibleColumnCount, resultColumnCount);
        }

        @Override
        public LocalResult create() {
            return new LocalResultImpl();
        }
    }

}
