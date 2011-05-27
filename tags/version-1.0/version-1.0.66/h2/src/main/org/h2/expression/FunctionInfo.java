/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

/**
 * This class contains information about a built-in function.
 */
class FunctionInfo {
    String name;
    int type;
    int dataType;
    int parameterCount;
    boolean nullIfParameterIsNull;
    boolean isDeterministic;
}
