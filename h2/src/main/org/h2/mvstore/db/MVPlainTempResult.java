/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.mvstore.Cursor;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVMap.Builder;
import org.h2.result.ResultExternal;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueLong;

/**
 * Plain temporary result.
 */
class MVPlainTempResult extends MVTempResult {

    private final MVMap<ValueLong, ValueArray> map;
    private long counter;

    private Cursor<ValueLong, ValueArray> cursor;

    private MVPlainTempResult(MVPlainTempResult parent) {
        super(parent);
        this.map = parent.map;
    }

    MVPlainTempResult(Session session, Expression[] expressions) {
        super(session);
        Database db = session.getDatabase();
        ValueDataType keyType = new ValueDataType(null, null, null);
        ValueDataType valueType = new ValueDataType(db.getCompareMode(), db, new int[expressions.length]);
        Builder<ValueLong, ValueArray> builder = new MVMap.Builder<ValueLong, ValueArray>().keyType(keyType)
                .valueType(valueType);
        map = store.openMap("tmp", builder);
    }

    @Override
    public int addRow(Value[] values) {
        map.put(ValueLong.get(counter++), ValueArray.get(values));
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
