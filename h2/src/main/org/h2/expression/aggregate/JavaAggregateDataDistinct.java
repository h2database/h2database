/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import org.h2.api.Aggregate;
import org.h2.value.*;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * TODO:
 */
class JavaAggregateDataDistinct extends JavaAggregateData {
    private Set<Value> values = new HashSet<>();
    private Aggregate userAgg;
    private int argsCount;

    public JavaAggregateDataDistinct(Aggregate userAgg, int argsCount)  {
        this.userAgg = userAgg;
        this.argsCount = argsCount;
    }

    @Override
    void add(Value val) {
        values.add(val);
    }

    @Override
    Object getValue() throws SQLException {
        for (Value value : values) {
            if (argsCount == 1) {
                userAgg.add(value.getObject());
            } else {
                Value[] values = ((ValueArray) value).getList();
                Object[] argValues = new Object[argsCount];
                for (int i = 0, len = argsCount; i < len; i++) {
                    argValues[i] = values[i].getObject();
                }
                userAgg.add(argValues);
            }
        }

        return userAgg.getResult();
    }
}
