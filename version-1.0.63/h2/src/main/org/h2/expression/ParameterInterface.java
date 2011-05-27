/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;

import org.h2.value.Value;

public interface ParameterInterface {
    void setValue(Value value);
    Value getParamValue() throws SQLException;
    void checkSet() throws SQLException;
}
