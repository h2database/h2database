/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.h2.engine.Session;
import org.h2.expression.BinaryOperation;
import org.h2.expression.BinaryOperation.OpType;
import org.h2.expression.ValueExpression;
import org.h2.message.DbException;
import org.h2.result.SortOrder;
import org.h2.value.Value;

/**
 * Window frame clause.
 */
public final class WindowFrame {

    private static abstract class Itr implements Iterator<Value[]> {

        final ArrayList<Value[]> orderedRows;

        int cursor;

        Itr(ArrayList<Value[]> orderedRows) {
            this.orderedRows = orderedRows;
        }

        @Override
        public final void remove() {
            throw new UnsupportedOperationException();
        }

    }

    private static class PlainItr extends Itr {

        final int endIndex;

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

    private static class PlainReverseItr extends Itr {

        final int startIndex;

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

    private static class BiItr extends PlainItr {

        final int end1, start1;

        BiItr(ArrayList<Value[]> orderedRows, int startIndex1, int endIndex1, int startIndex2, int endIndex2) {
            super(orderedRows, startIndex1, endIndex2);
            end1 = endIndex1;
            start1 = startIndex2;
        }

        @Override
        public Value[] next() {
            if (cursor > endIndex) {
                throw new NoSuchElementException();
            }
            Value[] r = orderedRows.get(cursor);
            cursor = cursor != end1 ? cursor + 1 : start1;
            return r;
        }

    }

    private static class BiReverseItr extends PlainReverseItr {

        final int end1, start1;

        BiReverseItr(ArrayList<Value[]> orderedRows, int startIndex1, int endIndex1, int startIndex2, int endIndex2) {
            super(orderedRows, startIndex1, endIndex2);
            end1 = endIndex1;
            start1 = startIndex2;
        }

        @Override
        public Value[] next() {
            if (cursor < startIndex) {
                throw new NoSuchElementException();
            }
            Value[] r = orderedRows.get(cursor);
            cursor = cursor != start1 ? cursor - 1 : end1;
            return r;
        }

    }

    private static final class TriItr extends BiItr {

        private final int end2, start2;

        TriItr(ArrayList<Value[]> orderedRows, int startIndex1, int endIndex1, int startIndex2, int endIndex2,
                int startIndex3, int endIndex3) {
            super(orderedRows, startIndex1, endIndex1, startIndex2, endIndex3);
            end2 = endIndex2;
            start2 = startIndex3;
        }

        @Override
        public Value[] next() {
            if (cursor > endIndex) {
                throw new NoSuchElementException();
            }
            Value[] r = orderedRows.get(cursor);
            cursor = cursor != end1 ? cursor != end2 ? cursor + 1 : start2 : start1;
            return r;
        }

    }

    private static final class TriReverseItr extends BiReverseItr {

        private final int end2, start2;

        TriReverseItr(ArrayList<Value[]> orderedRows, int startIndex1, int endIndex1, int startIndex2, int endIndex2,
                int startIndex3, int endIndex3) {
            super(orderedRows, startIndex1, endIndex1, startIndex2, endIndex3);
            end2 = endIndex2;
            start2 = startIndex3;
        }

        @Override
        public Value[] next() {
            if (cursor < startIndex) {
                throw new NoSuchElementException();
            }
            Value[] r = orderedRows.get(cursor);
            cursor = cursor != start1 ? cursor != start2 ? cursor - 1 : end2 : end1;
            return r;
        }

    }

    private final WindowFrameUnits units;

    private final WindowFrameBound starting;

    private final WindowFrameBound following;

    private final WindowFrameExclusion exclusion;

    /**
     * Returns iterator for the specified frame, or default iterator if frame is
     * null.
     *
     * @param frame
     *            window frame, or null
     * @param session
     *            the session
     * @param orderedRows
     *            ordered rows
     * @param sortOrder
     *            sort order
     * @param currentRow
     *            index of the current row
     * @param reverse
     *            whether iterator should iterate in reverse order
     *
     * @return iterator
     */
    public static Iterator<Value[]> iterator(WindowFrame frame, Session session, ArrayList<Value[]> orderedRows,
            SortOrder sortOrder, int currentRow, boolean reverse) {
        return frame != null ? frame.iterator(session, orderedRows, sortOrder, currentRow, reverse)
                : plainIterator(orderedRows, 0, currentRow, reverse);
    }

    private static Iterator<Value[]> plainIterator(ArrayList<Value[]> orderedRows, int startIndex, int endIndex,
            boolean reverse) {
        if (endIndex < startIndex) {
            return Collections.emptyIterator();
        }
        return reverse ? new PlainReverseItr(orderedRows, startIndex, endIndex)
                : new PlainItr(orderedRows, startIndex, endIndex);
    }

    private static Iterator<Value[]> biIterator(ArrayList<Value[]> orderedRows, int startIndex1, int endIndex1,
            int startIndex2, int endIndex2, boolean reverse) {
        return reverse ? new BiReverseItr(orderedRows, startIndex1, endIndex1, startIndex2, endIndex2)
                : new BiItr(orderedRows, startIndex1, endIndex1, startIndex2, endIndex2);
    }

    private static Iterator<Value[]> triIterator(ArrayList<Value[]> orderedRows, int startIndex1, int endIndex1,
            int startIndex2, int endIndex2, int startIndex3, int endIndex3, boolean reverse) {
        return reverse ? new TriReverseItr(orderedRows, startIndex1, endIndex1, startIndex2, endIndex2, //
                startIndex3, endIndex3)
                : new TriItr(orderedRows, startIndex1, endIndex1, startIndex2, endIndex2, startIndex3, endIndex3);
    }

    private static int toGroupStart(ArrayList<Value[]> orderedRows, SortOrder sortOrder, int offset, int minOffset) {
        Value[] row = orderedRows.get(offset);
        while (offset > minOffset && sortOrder.compare(row, orderedRows.get(offset - 1)) == 0) {
            offset--;
        }
        return offset;
    }

    private static int toGroupEnd(ArrayList<Value[]> orderedRows, SortOrder sortOrder, int offset, int maxOffset) {
        Value[] row = orderedRows.get(offset);
        while (offset < maxOffset && sortOrder.compare(row, orderedRows.get(offset + 1)) == 0) {
            offset++;
        }
        return offset;
    }

    private static int getIntOffset(WindowFrameBound bound, Session session) {
        int value = bound.getValue().getValue(session).getInt();
        if (value < 0) {
            throw DbException.getInvalidValueException("unsigned", value);
        }
        return value;
    }

    private static Value[] getCompareRow(Session session, ArrayList<Value[]> orderedRows, SortOrder sortOrder,
            int currentRow, WindowFrameBound bound, boolean add) {
        int sortIndex = sortOrder.getQueryColumnIndexes()[0];
        OpType opType = add ^ (sortOrder.getSortTypes()[0] & SortOrder.DESCENDING) != 0 ? OpType.PLUS : OpType.MINUS;
        Value[] row = orderedRows.get(currentRow);
        Value[] newRow = row.clone();
        newRow[sortIndex] = new BinaryOperation(opType, //
                ValueExpression.get(row[sortIndex]), ValueExpression.get(getValueOffset(bound, session))) //
                        .optimize(session).getValue(session);
        return newRow;
    }

    private static Value getValueOffset(WindowFrameBound bound, Session session) {
        Value value = bound.getValue().getValue(session);
        if (value.getSignum() < 0) {
            throw DbException.getInvalidValueException("unsigned", value.getTraceSQL());
        }
        return value;
    }

    /**
     * Creates new instance of window frame clause.
     *
     * @param units
     *            units
     * @param starting
     *            starting clause
     * @param following
     *            following clause
     * @param exclusion
     *            exclusion clause
     */
    public WindowFrame(WindowFrameUnits units, WindowFrameBound starting, WindowFrameBound following,
            WindowFrameExclusion exclusion) {
        this.units = units;
        this.starting = starting;
        if (following != null && following.getType() == WindowFrameBoundType.CURRENT_ROW) {
            following = null;
        }
        this.following = following;
        this.exclusion = exclusion;
    }

    /**
     * Checks validity of this frame.
     *
     * @return whether bounds of this frame valid
     */
    public boolean isValid() {
        WindowFrameBoundType s = starting.getType(),
                f = following != null ? following.getType() : WindowFrameBoundType.CURRENT_ROW;
        return s != WindowFrameBoundType.UNBOUNDED_FOLLOWING && f != WindowFrameBoundType.UNBOUNDED_PRECEDING
                && s.compareTo(f) <= 0;
    }

    /**
     * Returns whether window frame specification can be omitted.
     *
     * @return whether window frame specification can be omitted
     */
    public boolean isDefault() {
        return starting.getType() == WindowFrameBoundType.UNBOUNDED_PRECEDING && following == null
                && exclusion == WindowFrameExclusion.EXCLUDE_NO_OTHERS;
    }

    /**
     * Returns whether window frame specification contains all rows in
     * partition.
     *
     * @return whether window frame specification contains all rows in partition
     */
    public boolean isFullPartition() {
        return starting.getType() == WindowFrameBoundType.UNBOUNDED_PRECEDING && following != null
                && following.getType() == WindowFrameBoundType.UNBOUNDED_FOLLOWING
                && exclusion == WindowFrameExclusion.EXCLUDE_NO_OTHERS;
    }

    /**
     * Returns iterator.
     *
     * @param session
     *            the session
     * @param orderedRows
     *            ordered rows
     * @param sortOrder
     *            sort order
     * @param currentRow
     *            index of the current row
     * @param reverse
     *            whether iterator should iterate in reverse order
     *
     * @return iterator
     */
    public Iterator<Value[]> iterator(Session session, ArrayList<Value[]> orderedRows, SortOrder sortOrder,
            int currentRow, boolean reverse) {
        int startIndex = getIndex(session, orderedRows, sortOrder, currentRow, starting, false);
        int endIndex = following != null ? getIndex(session, orderedRows, sortOrder, currentRow, following, true)
                : currentRow;
        if (endIndex < startIndex) {
            return Collections.emptyIterator();
        }
        int size = orderedRows.size();
        if (startIndex >= size || endIndex < 0) {
            return Collections.emptyIterator();
        }
        if (startIndex < 0) {
            startIndex = 0;
        }
        if (endIndex >= size) {
            endIndex = size - 1;
        }
        return exclusion != WindowFrameExclusion.EXCLUDE_NO_OTHERS
                ? complexIterator(orderedRows, sortOrder, currentRow, startIndex, endIndex, reverse)
                : plainIterator(orderedRows, startIndex, endIndex, reverse);
    }

    private int getIndex(Session session, ArrayList<Value[]> orderedRows, SortOrder sortOrder, int currentRow,
            WindowFrameBound bound, boolean forFollowing) {
        int size = orderedRows.size();
        int last = size - 1;
        int index;
        switch (bound.getType()) {
        case UNBOUNDED_PRECEDING:
            index = -1;
            break;
        case PRECEDING:
            switch (units) {
            case ROWS: {
                int value = getIntOffset(bound, session);
                index = value > currentRow ? -1 : currentRow - value;
                break;
            }
            case GROUPS: {
                int value = getIntOffset(bound, session);
                if (!forFollowing) {
                    index = toGroupStart(orderedRows, sortOrder, currentRow, 0);
                    while (value > 0 && index > 0) {
                        value--;
                        index = toGroupStart(orderedRows, sortOrder, index - 1, 0);
                    }
                    if (value > 0) {
                        index = -1;
                    }
                } else {
                    if (value == 0) {
                        index = toGroupEnd(orderedRows, sortOrder, currentRow, last);
                    } else {
                        index = currentRow;
                        while (value > 0 && index >= 0) {
                            value--;
                            index = toGroupStart(orderedRows, sortOrder, index, 0) - 1;
                        }
                    }
                }
                break;
            }
            case RANGE: {
                index = currentRow;
                Value[] row = getCompareRow(session, orderedRows, sortOrder, index, bound, false);
                index = Collections.binarySearch(orderedRows, row, sortOrder);
                if (index >= 0) {
                    if (!forFollowing) {
                        while (index > 0 && sortOrder.compare(row, orderedRows.get(index - 1)) == 0) {
                            index--;
                        }
                    } else {
                        while (index < last && sortOrder.compare(row, orderedRows.get(index + 1)) == 0) {
                            index++;
                        }
                    }
                } else {
                    index = ~index;
                    if (!forFollowing) {
                        if (index == 0) {
                            index = -1;
                        }
                    } else {
                        index--;
                    }
                }
                break;
            }
            default:
                throw DbException.getUnsupportedException("units=" + units);
            }
            break;
        case CURRENT_ROW:
            index = currentRow;
            break;
        case FOLLOWING:
            switch (units) {
            case ROWS: {
                int value = getIntOffset(bound, session);
                int rem = last - currentRow;
                index = value > rem ? size : currentRow + value;
                break;
            }
            case GROUPS: {
                int value = getIntOffset(bound, session);
                if (forFollowing) {
                    index = toGroupEnd(orderedRows, sortOrder, currentRow, last);
                    while (value > 0 && index < last) {
                        value--;
                        index = toGroupEnd(orderedRows, sortOrder, index + 1, last);
                    }
                    if (value > 0) {
                        index = size;
                    }
                } else {
                    if (value == 0) {
                        index = toGroupStart(orderedRows, sortOrder, currentRow, 0);
                    } else {
                        index = currentRow;
                        while (value > 0 && index <= last) {
                            value--;
                            index = toGroupEnd(orderedRows, sortOrder, index, last) + 1;
                        }
                    }
                }
                break;
            }
            case RANGE: {
                index = currentRow;
                Value[] row = getCompareRow(session, orderedRows, sortOrder, index, bound, true);
                index = Collections.binarySearch(orderedRows, row, sortOrder);
                if (index >= 0) {
                    if (forFollowing) {
                        while (index < last && sortOrder.compare(row, orderedRows.get(index + 1)) == 0) {
                            index++;
                        }
                    } else {
                        while (index > 0 && sortOrder.compare(row, orderedRows.get(index - 1)) == 0) {
                            index--;
                        }
                    }
                } else {
                    index = ~index;
                    if (forFollowing) {
                        if (index != size) {
                            index--;
                        }
                    }
                }
                break;
            }
            default:
                throw DbException.getUnsupportedException("units=" + units);
            }
            break;
        case UNBOUNDED_FOLLOWING:
            index = size;
            break;
        default:
            throw DbException.getUnsupportedException("window frame bound type=" + bound.getType());
        }
        return index;
    }

    private Iterator<Value[]> complexIterator(ArrayList<Value[]> orderedRows, SortOrder sortOrder, int currentRow,
            int startIndex, int endIndex, boolean reverse) {
        if (exclusion == WindowFrameExclusion.EXCLUDE_CURRENT_ROW) {
            if (currentRow < startIndex || currentRow > endIndex) {
                // Nothing to exclude
            } else if (currentRow == startIndex) {
                startIndex++;
            } else if (currentRow == endIndex) {
                endIndex--;
            } else {
                return biIterator(orderedRows, startIndex, currentRow - 1, currentRow + 1, endIndex, reverse);
            }
        } else {
            // Do not include previous rows if they are not in the range
            int exStart = toGroupStart(orderedRows, sortOrder, currentRow, startIndex);
            // Do not include next rows if they are not in the range
            int exEnd = toGroupEnd(orderedRows, sortOrder, currentRow, endIndex);
            boolean includeCurrentRow = exclusion == WindowFrameExclusion.EXCLUDE_TIES;
            if (includeCurrentRow) {
                // Simplify exclusion if possible
                if (currentRow == exStart) {
                    exStart++;
                    includeCurrentRow = false;
                } else if (currentRow == exEnd) {
                    exEnd--;
                    includeCurrentRow = false;
                }
            }
            if (exStart > exEnd || exEnd < startIndex || exStart > endIndex) {
                // Empty range or nothing to exclude
            } else if (includeCurrentRow) {
                if (startIndex == exStart) {
                    if (endIndex == exEnd) {
                        return Collections.singleton(orderedRows.get(currentRow)).iterator();
                    } else {
                        return biIterator(orderedRows, currentRow, currentRow, exEnd + 1, endIndex, reverse);
                    }
                } else {
                    if (endIndex == exEnd) {
                        return biIterator(orderedRows, startIndex, exStart - 1, currentRow, currentRow, reverse);
                    } else {
                        return triIterator(orderedRows, startIndex, exStart - 1, currentRow, currentRow, exEnd + 1,
                                endIndex, reverse);
                    }
                }
            } else {
                if (startIndex >= exStart) {
                    startIndex = exEnd + 1;
                } else if (endIndex <= exEnd) {
                    endIndex = exStart - 1;
                } else {
                    return biIterator(orderedRows, startIndex, exStart - 1, exEnd + 1, endIndex, reverse);
                }
            }
        }
        return plainIterator(orderedRows, startIndex, endIndex, reverse);
    }

    /**
     * Returns SQL representation.
     *
     * @return SQL representation.
     * @see org.h2.expression.Expression#getSQL()
     */
    public String getSQL() {
        StringBuilder builder = new StringBuilder();
        builder.append(units.getSQL());
        if (following == null) {
            builder.append(' ').append(starting.getSQL(false));
        } else {
            builder.append(" BETWEEN ").append(starting.getSQL(false)).append(" AND ").append(following.getSQL(true));
        }
        if (exclusion != WindowFrameExclusion.EXCLUDE_NO_OTHERS) {
            builder.append(' ').append(exclusion.getSQL());
        }
        return builder.toString();
    }

}
