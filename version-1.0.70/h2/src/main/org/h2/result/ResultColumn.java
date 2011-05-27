/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.io.IOException;

import org.h2.value.Transfer;

/**
 * A result set column of a remote result.
 */
public class ResultColumn {
    String alias;
    String schemaName;
    String tableName;
    String columnName;
    int columnType;
    long precision;
    int scale;
    int displaySize;
    boolean autoIncrement;
    int nullable;

    ResultColumn(Transfer in) throws IOException {
        alias = in.readString();
        schemaName = in.readString();
        tableName = in.readString();
        columnName = in.readString();
        columnType = in.readInt();
        precision = in.readLong();
        scale = in.readInt();
        displaySize = in.readInt();
        autoIncrement = in.readBoolean();
        nullable = in.readInt();
    }

    public static void writeColumn(Transfer out, ResultInterface result, int i) throws IOException {
        out.writeString(result.getAlias(i));
        out.writeString(result.getSchemaName(i));
        out.writeString(result.getTableName(i));
        out.writeString(result.getColumnName(i));
        out.writeInt(result.getColumnType(i));
        out.writeLong(result.getColumnPrecision(i));
        out.writeInt(result.getColumnScale(i));
        out.writeInt(result.getDisplaySize(i));
        out.writeBoolean(result.isAutoIncrement(i));
        out.writeInt(result.getNullable(i));
    }
}
