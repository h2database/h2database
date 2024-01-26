/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.util.Arrays;
import java.util.BitSet;

import org.h2.engine.Database;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.mvstore.Cursor;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVMap.Builder;
import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.LongDataType;
import org.h2.result.ResultExternal;
import org.h2.result.RowFactory.DefaultRowFactory;
import org.h2.result.SortOrder;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueRow;

/**
 * Sorted temporary result.
 *
 * <p>
 * This result is used for distinct and/or sorted results.
 * </p>
 */
class MVSortedTempResult extends MVTempResult {

    /**
     * Whether this result is a standard distinct result.
     */
    private final boolean distinct;

    /**
     * Distinct indexes for DISTINCT ON results.
     */
    private final int[] distinctIndexes;

    /**
     * Mapping of indexes of columns to its positions in the store, or {@code null}
     * if columns are not reordered.
     */
    private final int[] indexes;

    /**
     * Map with rows as keys and counts of duplicate rows as values. If this map is
     * distinct all values are 1.
     */
    private final MVMap<ValueRow, Long> map;

    /**
     * Optional index. This index is created only if result is distinct and
     * {@code columnCount != distinctColumnCount} or if
     * {@link #contains(Value[])} method is invoked. Only the root result should
     * have an index if required.
     */
    private MVMap<ValueRow, Value> index;

    /**
     * Used for DISTINCT ON in presence of ORDER BY.
     */
    private ValueDataType orderedDistinctOnType;

    /**
     * Cursor for the {@link #next()} method.
     */
    private Cursor<ValueRow, Long> cursor;

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
        this.distinctIndexes = parent.distinctIndexes;
        this.indexes = parent.indexes;
        this.map = parent.map;
        this.rowCount = parent.rowCount;
    }

    /**
     * Creates a new sorted temporary result.
     *
     * @param database
     *            database
     * @param expressions
     *            column expressions
     * @param distinct
     *            whether this result should be distinct
     * @param distinctIndexes
     *            indexes of distinct columns for DISTINCT ON results
     * @param visibleColumnCount
     *            count of visible columns
     * @param resultColumnCount
     *            the number of columns including visible columns and additional
     *            virtual columns for ORDER BY and DISTINCT ON clauses
     * @param sort
     *            sort order, or {@code null} if this result does not need any
     *            sorting
     */
    MVSortedTempResult(Database database, Expression[] expressions, boolean distinct, int[] distinctIndexes,
            int visibleColumnCount, int resultColumnCount, SortOrder sort) {
        super(database, expressions, visibleColumnCount, resultColumnCount);
        this.distinct = distinct;
        this.distinctIndexes = distinctIndexes;
        int[] sortTypes = new int[resultColumnCount];
        int[] indexes;
        if (sort != null) {
            /*
             * If sorting is specified we need to reorder columns in requested order and set
             * sort types (ASC, DESC etc) for them properly.
             */
            indexes = new int[resultColumnCount];
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
            for (int i = len; i < resultColumnCount; i++) {
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
                for (int i = 0; i < resultColumnCount; i++) {
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
        ValueDataType keyType = new ValueDataType(database, SortOrder.addNullOrdering(database, sortTypes));
        if (indexes != null) {
            int l = indexes.length;
            TypeInfo[] types = new TypeInfo[l];
            for (int i = 0; i < l; i++) {
                types[i] = expressions[indexes[i]].getType();
            }
            keyType.setRowFactory(DefaultRowFactory.INSTANCE.createRowFactory(database, database.getCompareMode(),
                    database, types, null, false));
        } else {
            keyType.setRowFactory(DefaultRowFactory.INSTANCE.createRowFactory(database, database.getCompareMode(),
                    database, expressions, null, false));
        }
        Builder<ValueRow, Long> builder = new MVMap.Builder<ValueRow, Long>().keyType(keyType)
                .valueType(LongDataType.INSTANCE);
        map = store.openMap("tmp", builder);
        if (distinct && resultColumnCount != visibleColumnCount || distinctIndexes != null) {
            int count;
            TypeInfo[] types;
            if (distinctIndexes != null) {
                count = distinctIndexes.length;
                types = new TypeInfo[count];
                for (int i = 0; i < count; i++) {
                    types[i] = expressions[distinctIndexes[i]].getType();
                }
            } else {
                count = visibleColumnCount;
                types = new TypeInfo[count];
                for (int i = 0; i < count; i++) {
                    types[i] = expressions[i].getType();
                }
            }
            ValueDataType distinctType = new ValueDataType(database, new int[count]);
            distinctType.setRowFactory(DefaultRowFactory.INSTANCE.createRowFactory(database, database.getCompareMode(),
                    database, types, null, false));
            DataType<Value> distinctValueType;
            if (distinctIndexes != null && sort != null) {
                distinctValueType = orderedDistinctOnType = keyType;
            } else {
                distinctValueType = NullValueDataType.INSTANCE;
            }
            Builder<ValueRow, Value> indexBuilder = new MVMap.Builder<ValueRow, Value>().keyType(distinctType)
                    .valueType(distinctValueType);
            index = store.openMap("idx", indexBuilder);
        }
    }

    @Override
    public int addRow(Value[] values) {
        assert parent == null;
        ValueRow key = getKey(values);
        if (distinct || distinctIndexes != null) {
            if (distinctIndexes != null) {
                int cnt = distinctIndexes.length;
                Value[] newValues = new Value[cnt];
                for (int i = 0; i < cnt; i++) {
                    newValues[i] = values[distinctIndexes[i]];
                }
                ValueRow distinctRow = ValueRow.get(newValues);
                if (orderedDistinctOnType == null) {
                    if (index.putIfAbsent(distinctRow, ValueNull.INSTANCE) != null) {
                        return rowCount;
                    }
                } else {
                    ValueRow previous = (ValueRow) index.get(distinctRow);
                    if (previous == null) {
                        index.put(distinctRow, key);
                    } else if (orderedDistinctOnType.compare(previous, key) > 0) {
                        map.remove(previous);
                        rowCount--;
                        index.put(distinctRow, key);
                    } else {
                        return rowCount;
                    }
                }
            } else if (visibleColumnCount != resultColumnCount) {
                ValueRow distinctRow = ValueRow.get(Arrays.copyOf(values, visibleColumnCount));
                if (index.putIfAbsent(distinctRow, ValueNull.INSTANCE) != null) {
                    return rowCount;
                }
            }
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
        // Only parent result maintains the index
        if (parent != null) {
            return parent.contains(values);
        }
        assert distinct;
        if (visibleColumnCount != resultColumnCount) {
            return index.containsKey(ValueRow.get(values));
        }
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
     * Reorder values if required and convert them into {@link ValueRow}.
     *
     * @param values
     *                   values
     * @return ValueRow for maps
     */
    private ValueRow getKey(Value[] values) {
        if (indexes != null) {
            Value[] r = new Value[indexes.length];
            for (int i = 0; i < indexes.length; i++) {
                r[i] = values[indexes[i]];
            }
            values = r;
        }
        return ValueRow.get(values);
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
                r[indexes[i]] = key[i];
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
        assert parent == null && distinct;
        if (visibleColumnCount != resultColumnCount) {
            throw DbException.getUnsupportedException("removeRow()");
        }
        // If an entry was removed decrement the counter
        if (map.remove(getKey(values)) != null) {
            rowCount--;
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
