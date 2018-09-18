/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.h2.message.DbException;
import org.h2.value.Value;

/**
 * Window frame clause.
 */
public final class WindowFrame {

    /**
     * Simple extent.
     */
    public enum SimpleExtent {

    /**
     * RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW frame specification.
     */
    RANGE_BETWEEN_UNBOUNDED_PRECEDING_AND_CURRENT_ROW("RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),

    /**
     * RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING frame specification.
     */
    RANGE_BETWEEN_CURRENT_ROW_AND_UNBOUNDED_FOLLOWING("RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING"),

    /**
     * RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING frame
     * specification.
     */
    RANGE_BETWEEN_UNBOUNDED_PRECEDING_AND_UNBOUNDED_FOLLOWING(
            "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"),

        ;

        private final String sql;

        private SimpleExtent(String sql) {
            this.sql = sql;
        }

        /**
         * Returns SQL representation.
         *
         * @return SQL representation.
         * @see org.h2.expression.Expression#getSQL()
         */
        public String getSQL() {
            return sql;
        }

    }

    private final SimpleExtent extent;

    /**
     * Creates new instance of window frame clause.
     *
     * @param extent
     *            window frame extent
     */
    public WindowFrame(SimpleExtent extent) {
        this.extent = extent;
    }

    /**
     * Returns whether window frame specification can be omitted.
     *
     * @return whether window frame specification can be omitted
     */
    public boolean isDefault() {
        return extent == SimpleExtent.RANGE_BETWEEN_UNBOUNDED_PRECEDING_AND_CURRENT_ROW;
    }

    /**
     * Returns whether window frame specification contains all rows in
     * partition.
     *
     * @return whether window frame specification contains all rows in partition
     */
    public boolean isFullPartition() {
        return extent == SimpleExtent.RANGE_BETWEEN_UNBOUNDED_PRECEDING_AND_UNBOUNDED_FOLLOWING;
    }

    /**
     * Returns iterator.
     *
     * @param orderedRows
     *            ordered rows
     * @param currentRow
     *            index of the current row
     * @return iterator
     */
    public Iterator<Value[]> iterator(final ArrayList<Value[]> orderedRows, int currentRow) {
        int size = orderedRows.size();
        final int startIndex, endIndex;
        switch (extent) {
        case RANGE_BETWEEN_UNBOUNDED_PRECEDING_AND_CURRENT_ROW:
            startIndex = 0;
            endIndex = currentRow;
            break;
        case RANGE_BETWEEN_CURRENT_ROW_AND_UNBOUNDED_FOLLOWING:
            startIndex = currentRow;
            endIndex = size - 1;
            break;
        case RANGE_BETWEEN_UNBOUNDED_PRECEDING_AND_UNBOUNDED_FOLLOWING:
            startIndex = 0;
            endIndex = size - 1;
            break;
        default:
            throw DbException.getUnsupportedException("window frame extent =" + extent);
        }
        return new Iterator<Value[]>() {

            private int cursor = startIndex;

            @Override
            public boolean hasNext() {
                return cursor <= endIndex;
            }

            @Override
            public Value[] next() {
                if (cursor > endIndex) {
                    throw new NoSuchElementException();
                }
                return orderedRows.get(cursor++);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
    }

    /**
     * Returns iterator in descending order.
     *
     * @param orderedRows
     *            ordered rows
     * @param currentRow
     *            index of the current row
     * @return iterator in descending order
     */
    public Iterator<Value[]> reverseIterator(final ArrayList<Value[]> orderedRows, int currentRow) {
        int size = orderedRows.size();
        final int startIndex, endIndex;
        switch (extent) {
        case RANGE_BETWEEN_UNBOUNDED_PRECEDING_AND_CURRENT_ROW:
            startIndex = 0;
            endIndex = currentRow;
            break;
        case RANGE_BETWEEN_CURRENT_ROW_AND_UNBOUNDED_FOLLOWING:
            startIndex = currentRow;
            endIndex = size - 1;
            break;
        case RANGE_BETWEEN_UNBOUNDED_PRECEDING_AND_UNBOUNDED_FOLLOWING:
            startIndex = 0;
            endIndex = size - 1;
            break;
        default:
            throw DbException.getUnsupportedException("window frame extent =" + extent);
        }
        return new Iterator<Value[]>() {

            private int cursor = endIndex;

            @Override
            public boolean hasNext() {
                return cursor >= startIndex;
            }

            @Override
            public Value[] next() {
                if (cursor < startIndex) {
                    throw new NoSuchElementException();
                }
                return orderedRows.get(cursor--);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
    }

    /**
     * Returns SQL representation.
     *
     * @return SQL representation.
     * @see org.h2.expression.Expression#getSQL()
     */
    public String getSQL() {
        return extent.getSQL();
    }

}
