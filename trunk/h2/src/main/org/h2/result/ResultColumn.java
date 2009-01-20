/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
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

    /**
     * The column alias.
     */
    String alias;

    /**
     * The schema name or null.
     */
    String schemaName;

    /**
     * The table name or null.
     */
    String tableName;

    /**
     * The column name or null.
     */
    String columnName;

    /**
     * The value type of this column.
     */
    int columnType;

    /**
     * The precision.
     */
    long precision;

    /**
     * The scale.
     */
    int scale;

    /**
     * The expected display size.
     */
    int displaySize;

    /**
     * True if this is an autoincrement column.
     */
    boolean autoIncrement;

    /**
     * True if this column is nullable.
     */
    int nullable;

    /**
     * Read an object from the given transfer object.
     *
     * @param in the object from where to read the data
     */
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

    /**
     * Write a result column to the given output.
     *
     * @param out the object to where to write the data
     * @param result the result
     * @param i the column index
     */
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
