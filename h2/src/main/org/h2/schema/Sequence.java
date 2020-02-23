/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.schema;

import java.math.BigDecimal;
import org.h2.api.ErrorCode;
import org.h2.command.ddl.SequenceOptions;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.table.Table;
import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueNumeric;

/**
 * A sequence is created using the statement
 * CREATE SEQUENCE
 */
public class Sequence extends SchemaObjectBase {

    /**
     * The default cache size for sequences.
     */
    public static final int DEFAULT_CACHE_SIZE = 32;

    private long value;
    private long valueWithMargin;
    private long increment;
    private long cacheSize;
    private long startValue;
    private long minValue;
    private long maxValue;
    private boolean cycle;
    private boolean belongsToTable;
    private boolean writeWithMargin;

    /**
     * Creates a new sequence.
     *
     * @param session
     *            the session
     * @param schema
     *            the schema
     * @param id
     *            the object id
     * @param name
     *            the sequence name
     * @param options
     *            the sequence options
     * @param belongsToTable
     *            whether this sequence belongs to a table (for generated
     *            columns)
     */
    public Sequence(Session session, Schema schema, int id, String name, SequenceOptions options,
            boolean belongsToTable) {
        super(schema, id, name, Trace.SEQUENCE);
        Long t = options.getIncrement(session);
        long increment = t != null ? t : 1;
        Long start = options.getStartValue(session);
        Long min = options.getMinValue(null, session);
        Long max = options.getMaxValue(null, session);
        long minValue = min != null ? min : getDefaultMinValue(start, increment);
        long maxValue = max != null ? max : getDefaultMaxValue(start, increment);
        long startValue = start != null ? start : increment >= 0 ? minValue : maxValue;
        Long restart = options.getRestartValue(session, startValue);
        long value = restart != null ? restart : startValue;
        if (!isValid(value, startValue, minValue, maxValue, increment)) {
            throw DbException.get(ErrorCode.SEQUENCE_ATTRIBUTES_INVALID_6, name, Long.toString(value),
                    Long.toString(startValue), Long.toString(minValue), Long.toString(maxValue),
                    Long.toString(increment));
        }
        this.valueWithMargin = this.value = value;
        this.increment = increment;
        t = options.getCacheSize(session);
        this.cacheSize = t != null ? Math.max(1, t) : DEFAULT_CACHE_SIZE;
        this.startValue = startValue;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.cycle = Boolean.TRUE.equals(options.getCycle());
        this.belongsToTable = belongsToTable;
    }

    /**
     * Allows the start value, increment, min value and max value to be updated
     * atomically, including atomic validation. Useful because setting these
     * attributes one after the other could otherwise result in an invalid
     * sequence state (e.g. min value > max value, start value < min value,
     * etc).
     *
     * @param startValue
     *            the new start value ({@code null} if no change)
     * @param restartValue
     *            the restart value ({@code null} if restart is not requested)
     * @param minValue
     *            the new min value ({@code null} if no change)
     * @param maxValue
     *            the new max value ({@code null} if no change)
     * @param increment
     *            the new increment ({@code null} if no change)
     */
    public synchronized void modify(Long startValue, Long restartValue, Long minValue, Long maxValue, Long increment) {
        if (startValue == null) {
            startValue = this.startValue;
        }
        if (minValue == null) {
            minValue = this.minValue;
        }
        if (maxValue == null) {
            maxValue = this.maxValue;
        }
        if (increment == null) {
            increment = this.increment;
        }
        long value = restartValue != null ? restartValue : this.value;
        if (!isValid(value, startValue, minValue, maxValue, increment)) {
            throw DbException.get(ErrorCode.SEQUENCE_ATTRIBUTES_INVALID_6, getName(), String.valueOf(value),
                    String.valueOf(startValue), String.valueOf(minValue), String.valueOf(maxValue),
                    String.valueOf(increment));
        }
        this.valueWithMargin = this.value = value;
        this.startValue = startValue;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.increment = increment;
    }

    /**
     * Validates the specified prospective value, start value, min value, max
     * value and increment relative to each other, since each of their
     * respective validities are contingent on the values of the other
     * parameters.
     *
     * @param value
     *            the prospective value
     * @param startValue
     *            the prospective start value
     * @param minValue
     *            the prospective min value
     * @param maxValue
     *            the prospective max value
     * @param increment
     *            the prospective increment
     */
    private static boolean isValid(long value, long startValue, long minValue, long maxValue, long increment) {
        return minValue <= value && maxValue >= value //
                && minValue <= startValue && maxValue >= startValue //
                && maxValue > minValue && increment != 0 //
                && Long.compareUnsigned(Math.abs(increment), maxValue - minValue) <= 0;
    }

    /**
     * Calculates default min value.
     *
     * @param startValue the start value of the sequence.
     * @param increment the increment of the sequence value.
     * @return min value.
     */
    public static long getDefaultMinValue(Long startValue, long increment) {
        long v = increment >= 0 ? 1 : Long.MIN_VALUE;
        if (startValue != null && increment >= 0 && startValue < v) {
            v = startValue;
        }
        return v;
    }

    /**
     * Calculates default max value.
     *
     * @param startValue the start value of the sequence.
     * @param increment the increment of the sequence value.
     * @return min value.
     */
    public static long getDefaultMaxValue(Long startValue, long increment) {
        long v = increment >= 0 ? Long.MAX_VALUE : -1;
        if (startValue != null && increment < 0 && startValue > v) {
            v = startValue;
        }
        return v;
    }

    public boolean getBelongsToTable() {
        return belongsToTable;
    }

    public long getIncrement() {
        return increment;
    }

    public long getStartValue() {
        return startValue;
    }

    public long getMinValue() {
        return minValue;
    }

    public long getMaxValue() {
        return maxValue;
    }

    public boolean getCycle() {
        return cycle;
    }

    public void setCycle(boolean cycle) {
        this.cycle = cycle;
    }

    @Override
    public String getDropSQL() {
        if (getBelongsToTable()) {
            return null;
        }
        StringBuilder builder = new StringBuilder("DROP SEQUENCE IF EXISTS ");
        return getSQL(builder, DEFAULT_SQL_FLAGS).toString();
    }

    @Override
    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw DbException.throwInternalError(toString());
    }

    @Override
    public String getCreateSQL() {
        return getCreateSQL(false, false);
    }

    /**
     * Constructs the CREATE statement(s) for this sequence.
     *
     * @param forExport
     *            if {@code true}, generate the standard-compliant SQL with
     *            possible two commands, if {@code false} generate an
     *            H2-specific SQL (always one command)
     * @param secondCommand
     *            if {@code false}, generates a {@code CREATE SEQUENCE} command,
     *            if {@code true} generates an {@code ALTER SEQUENCE} command or
     *            returns {@code null}. If {@code forExport == false}, has no
     *            effect.
     * @return the SQL statement, or {@code null}
     */
    public synchronized String getCreateSQL(boolean forExport, boolean secondCommand) {
        long v = !forExport && writeWithMargin ? valueWithMargin : value;
        long startValue = this.startValue;
        if (forExport && secondCommand) {
            if (v == startValue) {
                return null;
            }
            return getSQL(new StringBuilder("ALTER SEQUENCE "), DEFAULT_SQL_FLAGS).append(" RESTART WITH ").append(v)
                    .toString();
        }
        StringBuilder builder = new StringBuilder("CREATE SEQUENCE ");
        getSQL(builder, DEFAULT_SQL_FLAGS).append(" START WITH ").append(startValue);
        if (!forExport && v != startValue) {
            builder.append(" RESTART WITH ").append(v);
        }
        if (increment != 1) {
            builder.append(" INCREMENT BY ").append(increment);
        }
        if (minValue != getDefaultMinValue(v, increment)) {
            builder.append(" MINVALUE ").append(minValue);
        }
        if (maxValue != getDefaultMaxValue(v, increment)) {
            builder.append(" MAXVALUE ").append(maxValue);
        }
        if (cycle) {
            builder.append(" CYCLE");
        }
        if (cacheSize != DEFAULT_CACHE_SIZE) {
            if (cacheSize == 1) {
                builder.append(" NO CACHE");
            } else {
                builder.append(" CACHE ").append(cacheSize);
            }
        }
        if (belongsToTable) {
            builder.append(" BELONGS_TO_TABLE");
        }
        return builder.toString();
    }

    /**
     * Get the next value for this sequence. Should not be called directly, use
     * {@link Session#getNextValueFor(Sequence, org.h2.command.Prepared)} instead.
     *
     * @param session the session
     * @return the next value
     */
    public Value getNext(Session session) {
        boolean needsFlush = false;
        long resultAsLong;
        synchronized (this) {
            if ((increment > 0 && value >= valueWithMargin) || (increment < 0 && value <= valueWithMargin)) {
                valueWithMargin += increment * cacheSize;
                needsFlush = true;
            }
            if ((increment > 0 && value > maxValue) || (increment < 0 && value < minValue)) {
                if (cycle) {
                    value = increment > 0 ? minValue : maxValue;
                    valueWithMargin = value + (increment * cacheSize);
                    needsFlush = true;
                } else {
                    throw DbException.get(ErrorCode.SEQUENCE_EXHAUSTED, getName());
                }
            }
            resultAsLong = value;
            value += increment;
        }
        if (needsFlush) {
            flush(session);
        }
        Value result;
        if (database.getMode().decimalSequences) {
            result = ValueNumeric.get(BigDecimal.valueOf(resultAsLong));
        } else {
            result = ValueBigint.get(resultAsLong);
        }
        return result;
    }

    /**
     * Flush the current value to disk.
     */
    public void flushWithoutMargin() {
        if (valueWithMargin != value) {
            valueWithMargin = value;
            flush(null);
        }
    }

    /**
     * Flush the current value, including the margin, to disk.
     *
     * @param session the session
     */
    public void flush(Session session) {
        if (isTemporary()) {
            return;
        }
        if (session == null || !database.isSysTableLockedBy(session)) {
            // This session may not lock the sys table (except if it has already
            // locked it) because it must be committed immediately, otherwise
            // other threads can not access the sys table.
            Session sysSession = database.getSystemSession();
            synchronized (database.isMVStore() ? sysSession : database) {
                flushInternal(sysSession);
                sysSession.commit(false);
            }
        } else {
            synchronized (database.isMVStore() ? session : database) {
                flushInternal(session);
            }
        }
    }

    private void flushInternal(Session session) {
        final boolean metaWasLocked = database.lockMeta(session);
        // just for this case, use the value with the margin
        try {
            writeWithMargin = true;
            database.updateMeta(session, this);
        } finally {
            writeWithMargin = false;
            if (!metaWasLocked) {
                database.unlockMeta(session);
            }
        }
    }

    /**
     * Flush the current value to disk and close this object.
     */
    public void close() {
        flushWithoutMargin();
    }

    @Override
    public int getType() {
        return DbObject.SEQUENCE;
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        database.removeMeta(session, getId());
        invalidate();
    }

    public synchronized long getCurrentValue() {
        return value - increment;
    }

    public void setBelongsToTable(boolean b) {
        this.belongsToTable = b;
    }

    public void setCacheSize(long cacheSize) {
        this.cacheSize = Math.max(1, cacheSize);
    }

    public long getCacheSize() {
        return cacheSize;
    }

}
