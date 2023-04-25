/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.value.Value;
import org.h2.value.ValueBigint;

/**
 * Class Sparse.
 * <UL>
 * <LI> 11/16/19 7:35 PM initial creation
 * </UL>
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public final class Sparse extends DefaultRow {
    private final int   columnCount;
    private final int[] map;

    Sparse(int columnCount, int capacity, int[] map) {
        super(new Value[capacity]);
        this.columnCount = columnCount;
        this.map = map;
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public Value getValue(int i) {
        if (i == ROWID_INDEX) {
            return ValueBigint.get(getKey());
        }
        int index = map[i];
        return index > 0 ? super.getValue(index - 1) : null;
    }

    @Override
    public void setValue(int i, Value v) {
        if (i == ROWID_INDEX) {
            setKey(v.getLong());
        }
        int index = map[i];
        if (index > 0) {
            super.setValue(index - 1, v);
        }
    }

    @Override
    public void copyFrom(SearchRow source) {
        setKey(source.getKey());
        for (int i = 0; i < map.length; i++) {
            int index = map[i];
            if (index > 0) {
                super.setValue(index - 1, source.getValue(i));
            }
        }
    }
}
