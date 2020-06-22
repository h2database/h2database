/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.condition;

import org.h2.command.query.Query;
import org.h2.engine.Session;
import org.h2.result.ResultInterface;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;

/**
 * Exists predicate as in EXISTS(SELECT ...)
 */
public class ExistsPredicate extends PredicateWithSubquery {

    public ExistsPredicate(Query query) {
        super(query);
    }

    @Override
    public Value getValue(Session session) {
        query.setSession(session);
        ResultInterface result = query.query(1);
        session.addTemporaryResult(result);
        return ValueBoolean.get(result.hasNext());
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        return super.getUnenclosedSQL(builder.append("EXISTS"), sqlFlags);
    }

}
