/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

/**
 * A function-like expression with a name.
 */
public interface NamedExpression {

    /**
     * Returns the upper case name.
     */
    String getName();

}
