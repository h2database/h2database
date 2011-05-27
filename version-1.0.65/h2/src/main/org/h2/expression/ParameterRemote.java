/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.message.Message;
import org.h2.value.Value;

/**
 * A client side (remote) parameter.
 */
public class ParameterRemote implements ParameterInterface {

    private Value value;
    private int index;

    public ParameterRemote(int index) {
        this.index = index;
    }

    public void setValue(Value value) {
        this.value = value;
    }

    public Value getParamValue() {
        return value;
    }

    public void checkSet() throws SQLException {
        if (value == null) {
            throw Message.getSQLException(ErrorCode.PARAMETER_NOT_SET_1, "#" + (index + 1));
        }
    }

}
