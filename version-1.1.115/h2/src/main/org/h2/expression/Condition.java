/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.value.Value;
import org.h2.value.ValueBoolean;

/**
 * Represents a condition returning a boolean value, or NULL.
 */
public abstract class Condition extends Expression {

    public int getType() {
        return Value.BOOLEAN;
    }

    public int getScale() {
        return 0;
    }

    public long getPrecision() {
        return ValueBoolean.PRECISION;
    }

    public int getDisplaySize() {
        return ValueBoolean.DISPLAY_SIZE;
    }

}
