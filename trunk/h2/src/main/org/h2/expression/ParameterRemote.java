/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.message.Message;
import org.h2.value.Transfer;
import org.h2.value.Value;

/**
 * A client side (remote) parameter.
 */
public class ParameterRemote implements ParameterInterface {

    private Value value;
    private int index;
    private int dataType = Value.UNKNOWN;
    private long precision;
    private int scale;
    private int nullable = ResultSetMetaData.columnNullableUnknown;

    public ParameterRemote(int index) {
        this.index = index;
    }

    public void setValue(Value newValue, boolean closeOld) throws SQLException {
        if (closeOld && value != null) {
            value.close();
        }
        value = newValue;
    }

    public Value getParamValue() {
        return value;
    }

    public void checkSet() throws SQLException {
        if (value == null) {
            throw Message.getSQLException(ErrorCode.PARAMETER_NOT_SET_1, "#" + (index + 1));
        }
    }

    public int getType() {
        return value == null ? dataType : value.getType();
    }

    public long getPrecision() {
        return value == null ? precision : value.getPrecision();
    }

    public int getScale() {
        return value == null ? scale : value.getScale();
    }

    public int getNullable() {
        return nullable;
    }

    /**
     * Write the parameter meta data from the transfer object.
     *
     * @param transfer the transfer object
     */
    public void readMetaData(Transfer transfer) throws IOException {
        dataType = transfer.readInt();
        precision = transfer.readLong();
        scale = transfer.readInt();
        nullable = transfer.readInt();
    }

    /**
     * Write the parameter meta data to the transfer object.
     *
     * @param transfer the transfer object
     * @param p the parameter
     */
    public static void writeMetaData(Transfer transfer, ParameterInterface p) throws IOException {
        transfer.writeInt(p.getType());
        transfer.writeLong(p.getPrecision());
        transfer.writeInt(p.getScale());
        transfer.writeInt(p.getNullable());
    }

}
