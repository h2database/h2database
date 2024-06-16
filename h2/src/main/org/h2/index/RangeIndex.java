/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import org.h2.api.ErrorCode;
import org.h2.command.query.AllColumnsForPlan;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.table.RangeTable;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueBigint;

/**
 * An index for the SYSTEM_RANGE table.
 * This index can only scan through all rows, search is not supported.
 */
public class RangeIndex extends VirtualTableIndex {

    private final RangeTable rangeTable;

    public RangeIndex(RangeTable table, IndexColumn[] columns) {
        super(table, "RANGE_INDEX", columns);
        this.rangeTable = table;
    }

    @Override
    public Cursor find(SessionLocal session, SearchRow first, SearchRow last, boolean reverse) {
        assert !reverse;
        long min = rangeTable.getMin(session);
        long max = rangeTable.getMax(session);
        long step = rangeTable.getStep(session);
        if (step == 0L) {
            throw DbException.get(ErrorCode.STEP_SIZE_MUST_NOT_BE_ZERO);
        }
        if (first != null) {
            try {
                long v = first.getValue(0).getLong();
                if (step > 0) {
                    if (v > min) {
                        min += (v - min + step - 1) / step * step;
                    }
                } else if (v > max) {
                    max = v;
                }
            } catch (DbException e) {
                // error when converting the value - ignore
            }
        }
        if (last != null) {
            try {
                long v = last.getValue(0).getLong();
                if (step > 0) {
                    if (v < max) {
                        max = v;
                    }
                } else if (v < min) {
                    min -= (min - v - step - 1) / step * step;
                }
            } catch (DbException e) {
                // error when converting the value - ignore
            }
        }
        return new RangeCursor(min, max, step);
    }

    @Override
    public double getCost(SessionLocal session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            AllColumnsForPlan allColumnsSet) {
        return 1d;
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public Cursor findFirstOrLast(SessionLocal session, boolean first) {
        long min = rangeTable.getMin(session);
        long max = rangeTable.getMax(session);
        long step = rangeTable.getStep(session);
        if (step == 0L) {
            throw DbException.get(ErrorCode.STEP_SIZE_MUST_NOT_BE_ZERO);
        }
        return (step > 0 ? min <= max : min >= max)
                ? new SingleRowCursor(Row.get(new Value[] { ValueBigint.get(first ^ min >= max ? min : max) }, 1))
                : SingleRowCursor.EMPTY;
    }

    @Override
    public String getPlanSQL() {
        return "range index";
    }

}
