/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.util.BitSet;

import org.h2.engine.Database;
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
 *
 * <p>
 * This result is used for distinct and/or sorted results.
 * </p>
 */
class MVSortedTempResult extends MVTempResult {

    /**
     * Whether this result is distinct.
     */
    private final boolean distinct;

    /**
     * Mapping of indexes of columns to its positions in the store, or {@code null}
     * if columns are not reordered.
     */
    private final int[] indexes;

    /**
     * Map with rows as keys and counts of duplicate rows as values. If this map is
     * distinct all values are 1.
     */
    private final MVMap<ValueArray, Long> map;

    /**
     * Cursor for the {@link #next()} method.
     */
    private Cursor<ValueArray, Long> cursor;

    /**
     * Current value for the {@link #next()} method. Used in non-distinct results
     * with duplicate rows.
     */
    private Value[] current;

    /**
     * Count of remaining duplicate rows for the {@link #next()} method. Used in
     * non-distinct results.
     */
    private long valueCount;

    /**
     * Creates a shallow copy of the result.
     *
     * @param parent
     *                   parent result
     */
    private MVSortedTempResult(MVSortedTempResult parent) {
        super(parent);
        this.distinct = parent.distinct;
        this.indexes = parent.indexes;
        this.map = parent.map;
        this.rowCount = parent.rowCount;
    }

    /**
     * Creates a new sorted temporary result.
     *
     * @param database
     *                        database
     * @param expressions
     *                        column expressions
     * @param distinct
     *                        whether this result should be distinct
     * @param sort
     *                        sort order, or {@code null} if this result does not
     *                        need any sorting
     */
    MVSortedTempResult(Database database, Expression[] expressions, boolean distinct, SortOrder sort) {
        super(database);
        this.distinct = distinct;
        int length = expressions.length;
        int[] sortTypes = new int[length];
        int[] indexes;
        if (sort != null) {
            /*
             * If sorting is specified we need to reorder columns in requested order and set
             * sort types (ASC, DESC etc) for them properly.
             */
            indexes = new int[length];
            int[] colIndex = sort.getQueryColumnIndexes();
            int len = colIndex.length;
            // This set is used to remember columns that are already included
            BitSet used = new BitSet();
            for (int i = 0; i < len; i++) {
                int idx = colIndex[i];
                assert !used.get(idx);
                used.set(idx);
                indexes[i] = idx;
                sortTypes[i] = sort.getSortTypes()[i];
            }
            /*
             * Because this result may have more columns than specified in sorting we need
             * to add all remaining columns to the mapping of columns. A default sorting
             * order (ASC / 0) will be used for them.
             */
            int idx = 0;
            for (int i = len; i < length; i++) {
                idx = used.nextClearBit(idx);
                indexes[i] = idx;
                idx++;
            }
            /*
             * Sometimes columns may be not reordered. Because reordering of columns
             * slightly slows down other methods we check whether columns are really
             * reordered or have the same order.
             */
            sameOrder: {
                for (int i = 0; i < length; i++) {
                    if (indexes[i] != i) {
                        // Columns are reordered
                        break sameOrder;
                    }
                }
                /*
                 * Columns are not reordered, set this field to null to disable reordering in
                 * other methods.
                 */
                indexes = null;
            }
        } else {
            // Columns are not reordered if sort order is not specified
            indexes = null;
        }
        this.indexes = indexes;
        ValueDataType keyType = new ValueDataType(database.getCompareMode(), database, sortTypes);
        Builder<ValueArray, Long> builder = new MVMap.Builder<ValueArray, Long>().keyType(keyType);
        map = store.openMap("tmp", builder);
    }

    @Override
    public int addRow(Value[] values) {
        assert parent == null;
        ValueArray key = getKey(values);
        if (distinct) {
            // Add a row and increment the counter only if row does not exist
            if (map.putIfAbsent(key, 1L) == null) {
                rowCount++;
            }
        } else {
            // Try to set counter to 1 first if such row does not exist yet
            Long old = map.putIfAbsent(key, 1L);
            if (old != null) {
                // This rows is already in the map, increment its own counter
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

    /**
     * Reorder values if required and convert them into {@link ValueArray}.
     *
     * @param values
     *                   values
     * @return ValueArray for maps
     */
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

    /**
     * Reorder values back if required.
     *
     * @param key
     *                reordered values
     * @return original values
     */
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
        // If we have multiple rows with the same values return them all
        if (--valueCount > 0) {
            /*
             * Underflow in valueCount is hypothetically possible after a lot of invocations
             * (not really possible in practice), but current will be null anyway.
             */
            return current;
        }
        if (!cursor.hasNext()) {
            // Set current to null to be sure
            current = null;
            return null;
        }
        // Read the next row
        current = getValue(cursor.next().getList());
        /*
         * If valueCount is greater than 1 that is possible for non-distinct results the
         * following invocations of next() will use this.current and this.valueCount.
         */
        valueCount = cursor.getValue();
        return current;
    }

    @Override
    public int removeRow(Value[] values) {
        assert parent == null;
        ValueArray key = getKey(values);
        if (distinct) {
            // If an entry was removed decrement the counter
            if (map.remove(key) != null) {
                rowCount--;
            }
        } else {
            Long old = map.remove(key);
            if (old != null) {
                long l = old;
                if (l > 1) {
                    /*
                     * We have more than one such row. Decrement its counter by 1 and put this row
                     * back into map.
                     */
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
