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

import org.h2.api.ErrorCode;
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

    private final WindowFrameUnits units;

    private final WindowFrameBound starting;

    private final WindowFrameBound following;

    private final WindowFrameExclusion exclusion;

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

    private static Value plus(Session session, ArrayList<Value[]> orderedRows, int currentRow, WindowFrameBound bound,
            int index) {
        return new BinaryOperation(OpType.PLUS, //
                ValueExpression.get(orderedRows.get(currentRow)[index]),
                ValueExpression.get(getValueOffset(bound, session))) //
                        .optimize(session).getValue(session);
    }

    private static Value minus(Session session, ArrayList<Value[]> orderedRows, int currentRow, WindowFrameBound bound,
            int index) {
        return new BinaryOperation(OpType.MINUS, //
                ValueExpression.get(orderedRows.get(currentRow)[index]),
                ValueExpression.get(getValueOffset(bound, session))) //
                        .optimize(session).getValue(session);
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
            throw DbException.get(ErrorCode.SYNTAX_ERROR_1, getSQL());
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
        if (exclusion != WindowFrameExclusion.EXCLUDE_NO_OTHERS) {
            return complexIterator(orderedRows, sortOrder, currentRow, startIndex, endIndex, reverse);
        }
        return reverse ? new PlainReverseItr(orderedRows, startIndex, endIndex)
                : new PlainItr(orderedRows, startIndex, endIndex);
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
                int sortIndex = sortOrder.getQueryColumnIndexes()[0];
                if ((sortOrder.getSortTypes()[0] & SortOrder.DESCENDING) != 0) {
                    Value c = plus(session, orderedRows, currentRow, bound, sortIndex);
                    while (index > 0 && session.getDatabase().compare(c, orderedRows.get(index - 1)[sortIndex]) >= 0) {
                        index--;
                    }
                } else {
                    Value c = minus(session, orderedRows, currentRow, bound, sortIndex);
                    while (index > 0 && session.getDatabase().compare(c, orderedRows.get(index - 1)[sortIndex]) <= 0) {
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
                int sortIndex = sortOrder.getQueryColumnIndexes()[0];
                if ((sortOrder.getSortTypes()[0] & SortOrder.DESCENDING) != 0) {
                    Value c = minus(session, orderedRows, currentRow, bound, sortIndex);
                    while (index < last
                            && session.getDatabase().compare(c, orderedRows.get(index + 1)[sortIndex]) <= 0) {
                        index++;
                    }
                } else {
                    Value c = plus(session, orderedRows, currentRow, bound, sortIndex);
                    while (index < last
                            && session.getDatabase().compare(c, orderedRows.get(index + 1)[sortIndex]) >= 0) {
                        index++;
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
        int size = orderedRows.size();
        BitSet set = new BitSet(size);
        set.set(startIndex, endIndex + 1);
        switch (exclusion) {
        case EXCLUDE_CURRENT_ROW:
            set.clear(currentRow);
            break;
        case EXCLUDE_GROUP:
        case EXCLUDE_TIES: {
            int exStart = toGroupStart(orderedRows, sortOrder, currentRow, startIndex);
            int exEnd = toGroupEnd(orderedRows, sortOrder, currentRow, endIndex);
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
