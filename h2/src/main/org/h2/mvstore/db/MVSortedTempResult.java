/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.util.BitSet;

import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.mvstore.Cursor;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVMap.Builder;
import org.h2.result.ResultExternal;
import org.h2.result.SortOrder;
import org.h2.value.Value;
import org.h2.value.ValueArray;

/**
 * Sorted temporary result.
 */
class MVSortedTempResult extends MVTempResult {

    private final boolean distinct;
    private final int[] indexes;
    private final MVMap<ValueArray, Long> map;

    private Cursor<ValueArray, Long> cursor;
    private Value[] current;
    private long valueCount;

    private MVSortedTempResult(MVSortedTempResult parent) {
        super(parent);
        this.distinct = parent.distinct;
        this.indexes = parent.indexes;
        this.map = parent.map;
        this.rowCount = parent.rowCount;
    }

    /**
     * Creates a new sorted temporary result.
     */
    MVSortedTempResult(Session session, Expression[] expressions, boolean distinct, SortOrder sort) {
        super(session);
        this.distinct = distinct;
        Database db = session.getDatabase();
        int length = expressions.length;
        int[] sortTypes = new int[length];
        int[] indexes;
        if (sort != null) {
            indexes = new int[length];
            int[] colIndex = sort.getQueryColumnIndexes();
            int len = colIndex.length;
            BitSet used = new BitSet();
            for (int i = 0; i < len; i++) {
                int idx = colIndex[i];
                used.set(idx);
                indexes[i] = idx;
                sortTypes[i] = sort.getSortTypes()[i];
            }
            int idx = 0;
            for (int i = len; i < length; i++) {
                idx = used.nextClearBit(idx);
                indexes[i] = idx;
                idx++;
            }
            sameOrder: {
                for (int i = 0; i < length; i++) {
                    if (indexes[i] != i) {
                        break sameOrder;
                    }
                }
                indexes = null;
            }
        } else {
            indexes = null;
        }
        this.indexes = indexes;
        ValueDataType keyType = new ValueDataType(db.getCompareMode(), db, sortTypes);
        Builder<ValueArray, Long> builder = new MVMap.Builder<ValueArray, Long>().keyType(keyType);
        map = store.openMap("tmp", builder);
    }

    @Override
    public int addRow(Value[] values) {
        ValueArray key = getKey(values);
        if (distinct) {
            if (map.putIfAbsent(key, 1L) == null) {
                rowCount++;
            }
        } else {
            Long old = map.putIfAbsent(key, 1L);
            if (old != null) {
                map.put(key, old + 1);
            }
            rowCount++;
        }
        return rowCount;
    }

    @Override
    public boolean contains(Value[] values) {
        return map.containsKey(getKey(values));
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
        return new MVSortedTempResult(this);
    }

    private ValueArray getKey(Value[] values) {
        if (indexes != null) {
            Value[] r = new Value[indexes.length];
            for (int i = 0; i < indexes.length; i++) {
                r[indexes[i]] = values[i];
            }
            values = r;
        }
        return ValueArray.get(values);
    }

    private Value[] getValue(Value[] key) {
        if (indexes != null) {
            Value[] r = new Value[indexes.length];
            for (int i = 0; i < indexes.length; i++) {
                r[i] = key[indexes[i]];
            }
            key = r;
        }
        return key;
    }

    @Override
    public Value[] next() {
        if (cursor == null) {
            cursor = map.cursor(null);
            current = null;
            valueCount = 0L;
        }
        if (--valueCount > 0) {
            return current;
        }
        current = null;
        if (!cursor.hasNext()) {
            return null;
        }
        current = getValue(cursor.next().getList());
        valueCount = cursor.getValue();
        return current;
    }

    @Override
    public int removeRow(Value[] values) {
        ValueArray key = getKey(values);
        if (distinct) {
            if (map.remove(key) != null) {
                rowCount--;
            }
        } else {
            Long old = map.remove(key);
            if (old != null) {
                long l = old;
                if (l > 1) {
                    map.put(key, l - 1);
                }
                rowCount--;
            }
        }
        return rowCount;
    }

    @Override
    public void reset() {
        cursor = null;
        current = null;
        valueCount = 0L;
    }

}
