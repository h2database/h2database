/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.schema;

import org.h2.api.ErrorCode;
import org.h2.command.ddl.SequenceOptions;
import org.h2.engine.DbObject;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBigint;

/**
 * A sequence is created using the statement
 * CREATE SEQUENCE
 */
public final class Sequence extends SchemaObject {

    /**
     * CYCLE clause and sequence state.
     */
    public enum Cycle {

        /**
         * Sequence is cycled.
         */
        CYCLE,

        /**
         * Sequence is not cycled and isn't exhausted yet.
         */
        NO_CYCLE,

        /**
         * Sequence is not cycled and was already exhausted.
         */
        EXHAUSTED;

        /**
         * Return whether sequence is cycled.
         *
         * @return {@code true} if sequence is cycled, {@code false} if sequence
         *         is not cycled
         */
        public boolean isCycle() {
            return this == CYCLE;
        }

    }

    /**
     * The default cache size for sequences.
     */
    public static final int DEFAULT_CACHE_SIZE = 32;

    private long baseValue;
    private long margin;

    private TypeInfo dataType;

    private long increment;
    private long cacheSize;
    private long startValue;
    private long minValue;
    private long maxValue;
    private Cycle cycle;
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
    public Sequence(SessionLocal session, Schema schema, int id, String name, SequenceOptions options,
            boolean belongsToTable) {
        super(schema, id, name, Trace.SEQUENCE);
        dataType = options.getDataType();
        if (dataType == null) {
            options.setDataType(dataType = session.getMode().decimalSequences ? TypeInfo.TYPE_NUMERIC_BIGINT
                    : TypeInfo.TYPE_BIGINT);
        }
        long bounds[] = options.getBounds();
        Long t = options.getIncrement(session);
        long increment = t != null ? t : 1;
        Long start = options.getStartValue(session);
        Long min = options.getMinValue(null, session);
        Long max = options.getMaxValue(null, session);
        long minValue = min != null ? min : getDefaultMinValue(start, increment, bounds);
        long maxValue = max != null ? max : getDefaultMaxValue(start, increment, bounds);
        long startValue = start != null ? start : increment >= 0 ? minValue : maxValue;
        Long restart = options.getRestartValue(session, startValue);
        long baseValue = restart != null ? restart : startValue;
        t = options.getCacheSize(session);
        long cacheSize;
        boolean mayAdjustCacheSize;
        if (t != null) {
            cacheSize = t;
            mayAdjustCacheSize = false;
        } else {
            cacheSize = DEFAULT_CACHE_SIZE;
            mayAdjustCacheSize = true;
        }
        cacheSize = checkOptions(baseValue, startValue, minValue, maxValue, increment, cacheSize, mayAdjustCacheSize);
        Cycle cycle = options.getCycle();
        if (cycle == null) {
            cycle = Cycle.NO_CYCLE;
        } else if (cycle == Cycle.EXHAUSTED) {
            baseValue = startValue;
        }
        this.margin = this.baseValue = baseValue;
        this.increment = increment;
        this.cacheSize = cacheSize;
        this.startValue = startValue;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.cycle = cycle;
        this.belongsToTable = belongsToTable;
    }

    /**
     * Allows the base value, start value, min value, max value, increment and
     * cache size to be updated atomically, including atomic validation. Useful
     * because setting these attributes one after the other could otherwise
     * result in an invalid sequence state (e.g. min value &gt; max value, start
     * value &lt; min value, etc).
     * @param baseValue
     *            the base value ({@code null} if restart is not requested)
     * @param startValue
     *            the new start value ({@code null} if no change)
     * @param minValue
     *            the new min value ({@code null} if no change)
     * @param maxValue
     *            the new max value ({@code null} if no change)
     * @param increment
     *            the new increment ({@code null} if no change)
     * @param cycle
     *            the new cycle value, or {@code null} if no change
     * @param cacheSize
     *            the new cache size ({@code null} if no change)
     */
    public synchronized void modify(Long baseValue, Long startValue, Long minValue, Long maxValue, Long increment,
            Cycle cycle, Long cacheSize) {
        long baseValueAsLong = baseValue != null ? baseValue : this.baseValue;
        long startValueAsLong = startValue != null ? startValue : this.startValue;
        long minValueAsLong = minValue != null ? minValue : this.minValue;
        long maxValueAsLong = maxValue != null ? maxValue : this.maxValue;
        long incrementAsLong = increment != null ? increment : this.increment;
        long cacheSizeAsLong;
        boolean mayAdjustCacheSize;
        if (cacheSize != null) {
            cacheSizeAsLong = cacheSize;
            mayAdjustCacheSize = false;
        } else {
            cacheSizeAsLong = this.cacheSize;
            mayAdjustCacheSize = true;
        }
        cacheSizeAsLong = checkOptions(baseValueAsLong, startValueAsLong, minValueAsLong, maxValueAsLong,
                incrementAsLong, cacheSizeAsLong, mayAdjustCacheSize);
        if (cycle == null) {
            cycle = this.cycle;
            if (cycle == Cycle.EXHAUSTED && baseValue != null) {
                cycle = Cycle.NO_CYCLE;
            }
        } else if (cycle == Cycle.EXHAUSTED) {
            baseValueAsLong = startValueAsLong;
        }
        this.margin = this.baseValue = baseValueAsLong;
        this.startValue = startValueAsLong;
        this.minValue = minValueAsLong;
        this.maxValue = maxValueAsLong;
        this.increment = incrementAsLong;
        this.cacheSize = cacheSizeAsLong;
        this.cycle = cycle;
    }

    /**
     * Validates the specified prospective base value, start value, min value,
     * max value, increment, and cache size relative to each other, since each
     * of their respective validities are contingent on the values of the other
     * parameters.
     *
     * @param baseValue
     *            the prospective base value
     * @param startValue
     *            the prospective start value
     * @param minValue
     *            the prospective min value
     * @param maxValue
     *            the prospective max value
     * @param increment
     *            the prospective increment
     * @param cacheSize
     *            the prospective cache size
     * @param mayAdjustCacheSize
     *            whether cache size may be adjusted, cache size 0 is adjusted
     *            unconditionally to 1
     * @return the prospective or adjusted cache size
     */
    private long checkOptions(long baseValue, long startValue, long minValue, long maxValue, long increment,
            long cacheSize, boolean mayAdjustCacheSize) {
        if (minValue <= baseValue && baseValue <= maxValue //
                && minValue <= startValue && startValue <= maxValue //
                && minValue < maxValue && increment != 0L) {
            long range = maxValue - minValue;
            if (Long.compareUnsigned(Math.abs(increment), range) <= 0 && cacheSize >= 0L) {
                if (cacheSize <= 1L) {
                    return 1L;
                }
                long maxCacheSize = getMaxCacheSize(range, increment);
                if (cacheSize <= maxCacheSize) {
                    return cacheSize;
                }
                if (mayAdjustCacheSize) {
                    return maxCacheSize;
                }
            }
        }
        throw DbException.get(ErrorCode.SEQUENCE_ATTRIBUTES_INVALID_7, getName(), Long.toString(baseValue),
                Long.toString(startValue), Long.toString(minValue), Long.toString(maxValue), Long.toString(increment),
                Long.toString(cacheSize));
    }

    private static long getMaxCacheSize(long range, long increment) {
        if (increment > 0L) {
            if (range < 0) {
                range = Long.MAX_VALUE;
            } else {
                range += increment;
                if (range < 0) {
                    range = Long.MAX_VALUE;
                }
            }
        } else {
            range = -range;
            if (range > 0) {
                range = Long.MIN_VALUE;
            } else {
                range += increment;
                if (range >= 0) {
                    range = Long.MIN_VALUE;
                }
            }
        }
        return range / increment;
    }

    /**
     * Calculates default min value.
     *
     * @param startValue the start value of the sequence.
     * @param increment the increment of the sequence value.
     * @param bounds min and max bounds of data type of the sequence
     * @return min value.
     */
    public static long getDefaultMinValue(Long startValue, long increment, long[] bounds) {
        long v = increment >= 0 ? 1 : bounds[0];
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
     * @param bounds min and max bounds of data type of the sequence
     * @return min value.
     */
    public static long getDefaultMaxValue(Long startValue, long increment, long[] bounds) {
        long v = increment >= 0 ? bounds[1] : -1;
        if (startValue != null && increment < 0 && startValue > v) {
            v = startValue;
        }
        return v;
    }

    public boolean getBelongsToTable() {
        return belongsToTable;
    }

    public TypeInfo getDataType() {
        return dataType;
    }

    public int getEffectivePrecision() {
        TypeInfo dataType = this.dataType;
        switch (dataType.getValueType()) {
        case Value.NUMERIC: {
            int p = (int) dataType.getPrecision();
            int s = dataType.getScale();
            if (p - s > ValueBigint.DECIMAL_PRECISION) {
                return ValueBigint.DECIMAL_PRECISION + s;
            }
            return p;
        }
        case Value.DECFLOAT:
            return Math.min((int) dataType.getPrecision(), ValueBigint.DECIMAL_PRECISION);
        default:
            return (int) dataType.getPrecision();
        }
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

    public Cycle getCycle() {
        return cycle;
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
    public String getCreateSQL() {
        StringBuilder builder = getSQL(new StringBuilder("CREATE SEQUENCE "), DEFAULT_SQL_FLAGS);
        if (dataType.getValueType() != Value.BIGINT) {
            dataType.getSQL(builder.append(" AS "), DEFAULT_SQL_FLAGS);
        }
        builder.append(' ');
        synchronized (this) {
            getSequenceOptionsSQL(builder, writeWithMargin ? margin : baseValue);
        }
        if (belongsToTable) {
            builder.append(" BELONGS_TO_TABLE");
        }
        return builder.toString();
    }

    /**
     * Append the options part of the SQL statement to create the sequence.
     *
     * @param builder the builder
     * @return the builder
     */
    public synchronized StringBuilder getSequenceOptionsSQL(StringBuilder builder) {
        return getSequenceOptionsSQL(builder, baseValue);
    }

    private StringBuilder getSequenceOptionsSQL(StringBuilder builder, long value) {
        builder.append("START WITH ").append(startValue);
        if (value != startValue && cycle != Cycle.EXHAUSTED) {
            builder.append(" RESTART WITH ").append(value);
        }
        if (increment != 1) {
            builder.append(" INCREMENT BY ").append(increment);
        }
        long[] bounds = SequenceOptions.getBounds(dataType);
        if (minValue != getDefaultMinValue(value, increment, bounds)) {
            builder.append(" MINVALUE ").append(minValue);
        }
        if (maxValue != getDefaultMaxValue(value, increment, bounds)) {
            builder.append(" MAXVALUE ").append(maxValue);
        }
        if (cycle == Cycle.CYCLE) {
            builder.append(" CYCLE");
        } else if (cycle == Cycle.EXHAUSTED) {
            builder.append(" EXHAUSTED");
        }
        if (cacheSize != DEFAULT_CACHE_SIZE) {
            if (cacheSize == 1) {
                builder.append(" NO CACHE");
            } else if (cacheSize > DEFAULT_CACHE_SIZE //
                    || cacheSize != getMaxCacheSize(maxValue - minValue, increment)) {
                builder.append(" CACHE ").append(cacheSize);
            }
        }
        return builder;
    }

    /**
     * Get the next value for this sequence. Should not be called directly, use
     * {@link SessionLocal#getNextValueFor(Sequence, org.h2.command.Prepared)} instead.
     *
     * @param session the session
     * @return the next value
     */
    public Value getNext(SessionLocal session) {
        long result;
        boolean needsFlush;
        synchronized (this) {
            if (cycle == Cycle.EXHAUSTED) {
                throw DbException.get(ErrorCode.SEQUENCE_EXHAUSTED, getName());
            }
            result = baseValue;
            long newBase = result + increment;
            needsFlush = increment > 0 ? increment(result, newBase) : decrement(result, newBase);
        }
        if (needsFlush) {
            flush(session);
        }
        return ValueBigint.get(result).castTo(dataType, session);
    }

    private boolean increment(long oldBase, long newBase) {
        boolean needsFlush = false;
        /*
         * If old base is not negative and new base is negative there is an
         * overflow.
         */
        if (newBase > maxValue || (~oldBase & newBase) < 0) {
            newBase = minValue;
            needsFlush = true;
            if (cycle == Cycle.CYCLE) {
                margin = newBase + increment * (cacheSize - 1);
            } else {
                margin = newBase;
                cycle = Cycle.EXHAUSTED;
            }
        } else if (newBase > margin) {
            long newMargin = newBase + increment * (cacheSize - 1);
            if (newMargin > maxValue || (~newBase & newMargin) < 0) {
                /*
                 * Don't cache values near the end of the sequence for
                 * simplicity.
                 */
                newMargin = newBase;
            }
            margin = newMargin;
            needsFlush = true;
        }
        baseValue = newBase;
        return needsFlush;
    }

    private boolean decrement(long oldBase, long newBase) {
        boolean needsFlush = false;
        /*
         * If old base is negative and new base is not negative there is an
         * overflow.
         */
        if (newBase < minValue || (oldBase & ~newBase) < 0) {
            newBase = maxValue;
            needsFlush = true;
            if (cycle == Cycle.CYCLE) {
                margin = newBase + increment * (cacheSize - 1);
            } else {
                margin = newBase;
                cycle = Cycle.EXHAUSTED;
            }
        } else if (newBase < margin) {
            long newMargin = newBase + increment * (cacheSize - 1);
            if (newMargin < minValue || (newBase & ~newMargin) < 0) {
                /*
                 * Don't cache values near the end of the sequence for
                 * simplicity.
                 */
                newMargin = newBase;
            }
            margin = newMargin;
            needsFlush = true;
        }
        baseValue = newBase;
        return needsFlush;
    }

    /**
     * Flush the current value to disk.
     */
    public void flushWithoutMargin() {
        if (margin != baseValue) {
            margin = baseValue;
            flush(null);
        }
    }

    /**
     * Flush the current value, including the margin, to disk.
     *
     * @param session the session
     */
    public void flush(SessionLocal session) {
        if (isTemporary()) {
            return;
        }
        if (session == null || !database.isSysTableLockedBy(session)) {
            // This session may not lock the sys table (except if it has already
            // locked it) because it must be committed immediately, otherwise
            // other threads can not access the sys table.
            final SessionLocal sysSession = database.getSystemSession();
            sysSession.lock();
            try {
                flushInternal(sysSession);
                sysSession.commit(false);
            } finally {
                sysSession.unlock();
            }
        } else {
            session.lock();
            try {
                flushInternal(session);
            } finally {
                session.unlock();
            }
        }
    }

    private void flushInternal(SessionLocal session) {
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
    public void removeChildrenAndResources(SessionLocal session) {
        database.removeMeta(session, getId());
        invalidate();
    }

    public synchronized long getBaseValue() {
        // Use synchronized because baseValue is not volatile
        return baseValue;
    }

    public synchronized long getCurrentValue() {
        return baseValue - increment;
    }

    public void setBelongsToTable(boolean b) {
        this.belongsToTable = b;
    }

    public long getCacheSize() {
        return cacheSize;
    }

}
