/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;

import org.h2.engine.Session;
import org.h2.value.Value;
import org.h2.value.ValueResultSet;

public interface FunctionCall {

    String getName();
    int getParameterCount();
    ValueResultSet getValueForColumnList(Session session, Expression[] nullArgs) throws SQLException;
    int getType();
    Expression optimize(Session session) throws SQLException;
    Value getValue(Session session) throws SQLException;
    Expression[] getArgs();
    String getSQL();

}
