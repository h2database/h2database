/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import org.h2.command.dml.Select;
import org.h2.command.dml.SelectOrderBy;
import org.h2.engine.Database;
import org.h2.result.SortOrder;
import org.h2.util.StatementBuilder;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;

import java.util.ArrayList;

/**
 * TODO:
 */
class AggregateDataGroupConcat extends AggregateDataCollecting {
    String sep;

    public AggregateDataGroupConcat(Select select, ArrayList<SelectOrderBy> orderByList, SortOrder sortOrder, String sep) {
        super(select, orderByList, sortOrder);
        this.sep = sep;
    }

    @Override
    Value getValue(Database database, int dataType, boolean distinct) {
        Value[] array = getArray();
        if (array == null) {
            return ValueNull.INSTANCE;
        }
        if (orderByList != null || distinct) {
            sortWithOrderBy(array);
        }
        StatementBuilder buff = new StatementBuilder();
        for (Value val : array) {
            String s;
            if (val.getType() == Value.ARRAY) {
                s = ((ValueArray) val).getList()[0].getString();
            } else {
                s = val.getString();
            }
            if (s == null) {
                continue;
            }
            if (sep != null) {
                buff.appendExceptFirst(sep);
            }
            buff.append(s);
        }
        return ValueString.get(buff.toString());
    }
}
