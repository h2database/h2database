/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.h2.message.DbException;
import org.h2.result.SortOrder;
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

    /**
     * Window frame exclusion clause.
     */
    public enum WindowFrameExclusion {
        /**
         * EXCLUDE CURRENT ROW exclusion clause.
         */
        EXCLUDE_CURRENT_ROW("EXCLUDE CURRENT ROW"),

        /**
         * EXCLUDE GROUP exclusion clause.
         */
        EXCLUDE_GROUP("EXCLUDE GROUP"),

        /**
         * EXCLUDE TIES exclusion clause.
         */
        EXCLUDE_TIES("EXCLUDE TIES"),

        /**
         * EXCLUDE NO OTHERS exclusion clause.
         */
        EXCLUDE_NO_OTHERS("EXCLUDE NO OTHERS"),

        ;

        private final String sql;

        private WindowFrameExclusion(String sql) {
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

    private abstract class Itr implements Iterator<Value[]> {

        final ArrayList<Value[]> orderedRows;

        Itr(ArrayList<Value[]> orderedRows) {
            this.orderedRows = orderedRows;
        }

        @Override
        public final void remove() {
            throw new UnsupportedOperationException();
        }

    }

    private final class PlainItr extends Itr {

        private final int endIndex;

        private int cursor;

        PlainItr(ArrayList<Value[]> orderedRows, int startIndex, int endIndex) {
            super(orderedRows);
            this.endIndex = endIndex;
            cursor = startIndex;
        }

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

    }

    private final class PlainReverseItr extends Itr {

        private final int startIndex;

        private int cursor;

        PlainReverseItr(ArrayList<Value[]> orderedRows, int startIndex, int endIndex) {
            super(orderedRows);
            this.startIndex = startIndex;
            cursor = endIndex;
        }

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

    }

    private abstract class AbstractBitSetItr extends Itr {

        final BitSet set;

        int cursor;

        AbstractBitSetItr(ArrayList<Value[]> orderedRows, BitSet set) {
            super(orderedRows);
            this.set = set;
        }

        @Override
        public final boolean hasNext() {
            return cursor >= 0;
        }

    }

    private final class BitSetItr extends AbstractBitSetItr {

        BitSetItr(ArrayList<Value[]> orderedRows, BitSet set) {
            super(orderedRows, set);
            cursor = set.nextSetBit(0);
        }

        @Override
        public Value[] next() {
            if (cursor < 0) {
                throw new NoSuchElementException();
            }
            Value[] result = orderedRows.get(cursor);
            cursor = set.nextSetBit(cursor + 1);
            return result;
        }

    }

    private final class BitSetReverseItr extends AbstractBitSetItr {

        BitSetReverseItr(ArrayList<Value[]> orderedRows, BitSet set) {
            super(orderedRows, set);
            cursor = set.length() - 1;
        }

        @Override
        public Value[] next() {
            if (cursor < 0) {
                throw new NoSuchElementException();
            }
            Value[] result = orderedRows.get(cursor);
            cursor = set.previousSetBit(cursor - 1);
            return result;
        }

    }

    private final SimpleExtent extent;

    private final WindowFrameExclusion exclusion;

    /**
     * Creates new instance of window frame clause.
     *
     * @param extent
     *            window frame extent
     * @param exclusion
     *            window frame exclusion
     */
    public WindowFrame(SimpleExtent extent, WindowFrameExclusion exclusion) {
        this.extent = extent;
        this.exclusion = exclusion;
    }

    /**
     * Returns whether window frame specification can be omitted.
     *
     * @return whether window frame specification can be omitted
     */
    public boolean isDefault() {
        return extent == SimpleExtent.RANGE_BETWEEN_UNBOUNDED_PRECEDING_AND_CURRENT_ROW
                && exclusion == WindowFrameExclusion.EXCLUDE_NO_OTHERS;
    }

    /**
     * Returns whether window frame specification contains all rows in
     * partition.
     *
     * @return whether window frame specification contains all rows in partition
     */
    public boolean isFullPartition() {
        return extent == SimpleExtent.RANGE_BETWEEN_UNBOUNDED_PRECEDING_AND_UNBOUNDED_FOLLOWING
                && exclusion == WindowFrameExclusion.EXCLUDE_NO_OTHERS;
    }

    /**
     * Returns iterator.
     *
     * @param orderedRows
     *            ordered rows
     * @param sortOrder
     *            sort order
     * @param currentRow
     *            index of the current row
     * @param reverse
     *            whether iterator should iterate in reverse order
     * @return iterator
     */
    public Iterator<Value[]> iterator(ArrayList<Value[]> orderedRows, SortOrder sortOrder, int currentRow,
            boolean reverse) {
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
        if (exclusion != WindowFrameExclusion.EXCLUDE_NO_OTHERS) {
            return complexIterator(orderedRows, sortOrder, currentRow, startIndex, endIndex, reverse);
        }
        return reverse ? new PlainReverseItr(orderedRows, startIndex, endIndex)
                : new PlainItr(orderedRows, startIndex, endIndex);
    }

    private Iterator<Value[]> complexIterator(ArrayList<Value[]> orderedRows, SortOrder sortOrder, int currentRow,
            int startIndex, int endIndex, boolean reverse) {
        int size = orderedRows.size();
        BitSet set = new BitSet(size);
        set.set(startIndex, endIndex + 1);
        switch (exclusion) {
        case EXCLUDE_CURRENT_ROW:
            set.clear(currentRow);
            break;
        case EXCLUDE_GROUP:
        case EXCLUDE_TIES: {
            int exStart = currentRow;
            Value[] row = orderedRows.get(currentRow);
            while (exStart > startIndex && sortOrder.compare(row, orderedRows.get(exStart - 1)) == 0) {
                exStart--;
            }
            int exEnd = currentRow;
            while (exEnd < endIndex && sortOrder.compare(row, orderedRows.get(exEnd + 1)) == 0) {
                exEnd++;
            }
            set.clear(exStart, exEnd + 1);
            if (exclusion == WindowFrameExclusion.EXCLUDE_TIES) {
                set.set(currentRow);
            }
        }
        //$FALL-THROUGH$
        default:
        }
        if (set.isEmpty()) {
            return Collections.emptyIterator();
        }
        return reverse ? new BitSetReverseItr(orderedRows, set) : new BitSetItr(orderedRows, set);
    }

    /**
     * Returns SQL representation.
     *
     * @return SQL representation.
     * @see org.h2.expression.Expression#getSQL()
     */
    public String getSQL() {
        String sql = extent.getSQL();
        if (exclusion != WindowFrameExclusion.EXCLUDE_NO_OTHERS) {
            sql = sql + ' ' + exclusion.getSQL();
        }
        return sql;
    }

}
