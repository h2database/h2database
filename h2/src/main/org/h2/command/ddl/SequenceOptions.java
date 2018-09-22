/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.engine.Session;
import org.h2.expression.Expression;

/**
 * Sequence options.
 */
public class SequenceOptions {

    private Expression start;

    private Expression increment;

    private Expression maxValue;

    private Expression minValue;

    private Boolean cycle;

    private Expression cacheSize;

    private static Long getLong(Session session, Expression expr) {
        return expr != null ? expr.optimize(session).getValue(session).getLong() : null;
    }

    public Long getStartValue(Session session) {
        return getLong(session, start);
    }

    public void setStartValue(Expression start) {
        this.start = start;
    }

    public Long getIncrement(Session session) {
        return getLong(session, increment);
    }

    public void setIncrement(Expression increment) {
        this.increment = increment;
    }

    public Long getMaxValue(Session session) {
        return getLong(session, maxValue);
    }

    public void setMaxValue(Expression maxValue) {
        this.maxValue = maxValue;
    }

    public Long getMinValue(Session session) {
        return getLong(session, minValue);
    }

    public void setMinValue(Expression minValue) {
        this.minValue = minValue;
    }

    public Boolean getCycle() {
        return cycle;
    }

    public void setCycle(Boolean cycle) {
        this.cycle = cycle;
    }

    public Long getCacheSize(Session session) {
        return getLong(session, cacheSize);
    }

    public void setCacheSize(Expression cacheSize) {
        this.cacheSize = cacheSize;
    }

    protected boolean isRangeSet() {
        return start != null || minValue != null || maxValue != null || increment != null;
    }

}
