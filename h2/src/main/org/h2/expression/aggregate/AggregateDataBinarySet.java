/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.value.Value;

/**
 * Aggregate data of binary set functions.
 */
abstract class AggregateDataBinarySet extends AggregateData {

    abstract void add(SessionLocal session, Value yValue, Value xValue);

    @Override
    final void add(SessionLocal session, Value v) {
        throw DbException.getInternalError();
    }

}
