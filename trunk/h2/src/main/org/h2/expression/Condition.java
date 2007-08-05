/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.value.Value;

/**
 * @author Thomas
 */
public abstract class Condition extends Expression {

    public int getType() {
        return Value.BOOLEAN;
    }

    public int getScale() {
        return 0;
    }

    public long getPrecision() {
        return 0;
    }

}
