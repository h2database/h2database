/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.util.ArrayList;
import org.h2.api.ErrorCode;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.index.Index;
import org.h2.index.RangeIndex;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.value.TypeInfo;

/**
 * The table SYSTEM_RANGE is a virtual table that generates incrementing numbers
 * with a given start end point.
 */
public class RangeTable extends VirtualTable {

    /**
     * The name of the range table.
     */
    public static final String NAME = "SYSTEM_RANGE";

    /**
     * The PostgreSQL alias for the range table.
     */
    public static final String ALIAS = "GENERATE_SERIES";

    private Expression min, max, step;
    private boolean optimized;

    private final RangeIndex index;

    /**
     * Create a new range with the given start and end expressions.
     *
     * @param schema the schema (always the main schema)
     * @param min the start expression
     * @param max the end expression
     */
    public RangeTable(Schema schema, Expression min, Expression max) {
        super(schema, 0, NAME);
        this.min = min;
        this.max = max;
        Column[] columns = new Column[] { new Column("X", TypeInfo.TYPE_BIGINT) };
        setColumns(columns);
        index = new RangeIndex(this, IndexColumn.wrap(columns));
    }

    public RangeTable(Schema schema, Expression min, Expression max, Expression step) {
        this(schema, min, max);
        this.step = step;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        builder.append(NAME).append('(');
        min.getUnenclosedSQL(builder, sqlFlags).append(", ");
        max.getUnenclosedSQL(builder, sqlFlags);
        if (step != null) {
            step.getUnenclosedSQL(builder.append(", "), sqlFlags);
        }
        return builder.append(')');
    }

    @Override
    public boolean canGetRowCount(SessionLocal session) {
        return true;
    }

    @Override
    public long getRowCount(SessionLocal session) {
        long step = getStep(session);
        if (step == 0L) {
            throw DbException.get(ErrorCode.STEP_SIZE_MUST_NOT_BE_ZERO);
        }
        long delta = getMax(session) - getMin(session);
        if (step > 0) {
            if (delta < 0) {
                return 0;
            }
        } else if (delta > 0) {
            return 0;
        }
        return delta / step + 1;
    }

    @Override
    public TableType getTableType() {
        return TableType.SYSTEM_TABLE;
    }

    @Override
    public Index getScanIndex(SessionLocal session) {
        return index;
    }

    @Override
    public ArrayList<Index> getIndexes() {
        ArrayList<Index> list = new ArrayList<>(2);
        // Scan index (ignored by MIN/MAX optimization)
        list.add(index);
        // Normal index
        list.add(index);
        return list;
    }

    /**
     * Calculate and get the start value of this range.
     *
     * @param session the session
     * @return the start value
     */
    public long getMin(SessionLocal session) {
        optimize(session);
        return min.getValue(session).getLong();
    }

    /**
     * Calculate and get the end value of this range.
     *
     * @param session the session
     * @return the end value
     */
    public long getMax(SessionLocal session) {
        optimize(session);
        return max.getValue(session).getLong();
    }

    /**
     * Get the increment.
     *
     * @param session the session
     * @return the increment (1 by default)
     */
    public long getStep(SessionLocal session) {
        optimize(session);
        if (step == null) {
            return 1;
        }
        return step.getValue(session).getLong();
    }

    private void optimize(SessionLocal s) {
        if (!optimized) {
            min = min.optimize(s);
            max = max.optimize(s);
            if (step != null) {
                step = step.optimize(s);
            }
            optimized = true;
        }
    }

    @Override
    public long getMaxDataModificationId() {
        return 0;
    }

    @Override
    public long getRowCountApproximation(SessionLocal session) {
        return 100;
    }

    @Override
    public boolean isDeterministic() {
        return true;
    }

}
