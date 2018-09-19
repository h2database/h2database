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
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueNull;

import java.util.ArrayList;

/**
 * TODO:
 */
class AggregateDataArray extends AggregateDataCollecting {

    public AggregateDataArray(Select select, ArrayList<SelectOrderBy> orderByList, SortOrder sortOrder) {
        super(select, orderByList, sortOrder);
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
        if (orderByList != null) {
            for (int i = 0; i < array.length; i++) {
                array[i] = ((ValueArray) array[i]).getList()[0];
            }
        }
        return ValueArray.get(array);
    }
}
