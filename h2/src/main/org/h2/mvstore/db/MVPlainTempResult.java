/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import org.h2.engine.Database;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.mvstore.Cursor;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVMap.Builder;
import org.h2.mvstore.type.LongDataType;
import org.h2.result.ResultExternal;
import org.h2.result.RowFactory.DefaultRowFactory;
import org.h2.value.Value;
import org.h2.value.ValueRow;

/**
 * Plain temporary result.
 */
class MVPlainTempResult extends MVTempResult {

    /**
     * Map with identities of rows as keys rows as values.
     */
    private final MVMap<Long, ValueRow> map;

    /**
     * Counter for the identities of rows. A separate counter is used instead of
     * {@link #rowCount} because rows due to presence of {@link #removeRow(Value[])}
     * method to ensure that each row will have an own identity.
     */
    private long counter;

    /**
     * Cursor for the {@link #next()} method.
     */
    private Cursor<Long, ValueRow> cursor;

    /**
     * Creates a shallow copy of the result.
     *
     * @param parent
     *                   parent result
     */
    private MVPlainTempResult(MVPlainTempResult parent) {
        super(parent);
        this.map = parent.map;
    }

    /**
     * Creates a new plain temporary result. This result does not sort its rows,
     * but it can be used in index-sorted queries and it can preserve additional
     * columns for WITH TIES processing.
     *
     * @param database
     *            database
     * @param expressions
     *            column expressions
     * @param visibleColumnCount
     *            count of visible columns
     * @param resultColumnCount
     *            the number of columns including visible columns and additional
     *            virtual columns for ORDER BY clause
     */
    MVPlainTempResult(Database database, Expression[] expressions, int visibleColumnCount, int resultColumnCount) {
        super(database, expressions, visibleColumnCount, resultColumnCount);
        ValueDataType valueType = new ValueDataType(database, new int[resultColumnCount]);
        valueType.setRowFactory(DefaultRowFactory.INSTANCE.createRowFactory(database, database.getCompareMode(),
                database, expressions, null, false));
        Builder<Long, ValueRow> builder = new MVMap.Builder<Long, ValueRow>().keyType(LongDataType.INSTANCE)
                .valueType(valueType).singleWriter();
        map = store.openMap("tmp", builder);
    }

    @Override
    public int addRow(Value[] values) {
        assert parent == null;
        map.append(counter++, ValueRow.get(values));
        return ++rowCount;
    }

    @Override
    public boolean contains(Value[] values) {
        throw DbException.getUnsupportedException("contains()");
    }

    @Override
    public synchronized ResultExternal createShallowCopy() {
        if (parent != null) {
            return parent.createShallowCopy();
        }
        if (closed) {
            return null;
        }
        childCount++;
        return new MVPlainTempResult(this);
    }

    @Override
    public Value[] next() {
        if (cursor == null) {
            cursor = map.cursor(null);
        }
        if (!cursor.hasNext()) {
            return null;
        }
        cursor.next();
        return cursor.getValue().getList();
    }

    @Override
    public int removeRow(Value[] values) {
        throw DbException.getUnsupportedException("removeRow()");
    }

    @Override
    public void reset() {
        cursor = null;
    }

}
