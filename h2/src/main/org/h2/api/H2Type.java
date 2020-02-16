/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.api;

import java.sql.SQLType;

import org.h2.value.Value;

/**
 * Data types of H2.
 */
public enum H2Type implements SQLType {

    // Exact numeric data types

    /**
     * The TINYINT data type.
     */
    TINYINT("TINYINT", Value.TINYINT),

    /**
     * The SMALLINT data type.
     */
    SMALLINT("SMALLINT", Value.SMALLINT),

    /**
     * The INTEGER data type.
     */
    INTEGER("INTEGER", Value.INTEGER),

    /**
     * The BIGINT data type.
     */
    BIGINT("BIGINT", Value.BIGINT),

    /**
     * The NUMERIC data type.
     */
    NUMERIC("NUMERIC", Value.NUMERIC),

    // Approximate numeric data types

    /**
     * The REAL data type.
     */
    REAL("REAL", Value.REAL),

    /**
     * The DOUBLE PRECISION data type.
     */
    DOUBLE_PRECISION("DOUBLE PRECISION", Value.DOUBLE),

    // Character strings

    /**
     * The CHAR data type.
     */
    CHAR("CHAR", Value.CHAR),

    /**
     * The VARCHAR data type.
     */
    VARCHAR("VARCHAR", Value.VARCHAR),

    /**
     * The VARCHAR_IGNORECASE data type.
     */
    VARCHAR_IGNORECASE("VARCHAR_IGNORECASE", Value.VARCHAR_IGNORECASE),

    /**
     * The CLOB data type.
     */
    CLOB("CLOB", Value.CLOB),

    // Date-time data types

    /**
     * The DATE data type.
     */
    DATE("DATE", Value.DATE),

    /**
     * The TIME data type.
     */
    TIME("TIME", Value.TIME),

    /**
     * The TIME WITH TIME ZONE data type.
     */
    TIME_WITH_TIME_ZONE("TIME WITH TIME ZONE", Value.TIME_TZ),

    /**
     * The TIMESTAMP data type.
     */
    TIMESTAMP("TIMESTAMP", Value.TIMESTAMP),

    /**
     * The TIMESTAMP WITH TIME ZONE data type.
     */
    TIMESTAMP_WITH_TIME_ZONE("TIMESTAMP WITH TIME ZONE", Value.TIMESTAMP_TZ),

    // Intervals

    /**
     * The INTERVAL YEAR data type.
     */
    INTERVAL_YEAR("INTERVAL YEAR", Value.INTERVAL_YEAR),

    /**
     * The INTERVAL MONTH data type.
     */
    INTERVAL_MONTH("INTERVAL MONTH", Value.INTERVAL_MONTH),

    /**
     * The INTERVAL DAY data type.
     */
    INTERVAL_DAY("INTERVAL DAY", Value.INTERVAL_DAY),

    /**
     * The INTERVAL HOUR data type.
     */
    INTERVAL_HOUR("INTERVAL HOUR", Value.INTERVAL_HOUR),

    /**
     * The INTERVAL MINUTE data type.
     */
    INTERVAL_MINUTE("INTERVAL MINUTE", Value.INTERVAL_MINUTE),

    /**
     * The INTERVAL SECOND data type.
     */
    INTERVAL_SECOND("INTERVAL SECOND", Value.INTERVAL_SECOND),

    /**
     * The INTERVAL YEAR TO MONTH data type.
     */
    INTERVAL_YEAR_TO_MONTH("INTERVAL YEAR TO MONTH", Value.INTERVAL_YEAR_TO_MONTH),

    /**
     * The INTERVAL DAY TO HOUR data type.
     */
    INTERVAL_DAY_TO_HOUR("INTERVAL DAY TO HOUR", Value.INTERVAL_DAY_TO_HOUR),

    /**
     * The INTERVAL DAY TO MINUTE data type.
     */
    INTERVAL_DAY_TO_MINUTE("INTERVAL DAY TO MINUTE", Value.INTERVAL_DAY_TO_MINUTE),

    /**
     * The INTERVAL DAY TO SECOND data type.
     */
    INTERVAL_DAY_TO_SECOND("INTERVAL DAY TO SECOND", Value.INTERVAL_DAY_TO_SECOND),

    /**
     * The INTERVAL HOUR TO MINUTE data type.
     */
    INTERVAL_HOUR_TO_MINUTE("INTERVAL HOUR TO MINUTE", Value.INTERVAL_HOUR_TO_MINUTE),

    /**
     * The INTERVAL HOUR TO SECOND data type.
     */
    INTERVAL_HOUR_TO_SECOND("INTERVAL HOUR TO SECOND", Value.INTERVAL_HOUR_TO_SECOND),

    /**
     * The INTERVAL MINUTE TO SECOND data type.
     */
    INTERVAL_MINUTE_TO_SECOND("INTERVAL MINUTE TO SECOND", Value.INTERVAL_MINUTE_TO_SECOND),

    // Boolean

    /**
     * The BOOLEAN data type
     */
    BOOLEAN("BOOLEAN", Value.BOOLEAN),

    // Binary strings

    /**
     * The VARBINARY data type.
     */
    BINARY("BINARY", Value.BINARY),

    /**
     * The VARBINARY data type.
     */
    VARBINARY("VARBINARY", Value.VARBINARY),

    /**
     * The BLOB data type.
     */
    BLOB("BLOB", Value.BLOB),

    // Collections

    /**
     * The ARRAY data type.
     */
    ARRAY("ARRAY", Value.ARRAY),

    // Row

    /**
     * The ROW data type.
     */
    ROW("ROW", Value.ROW),

    // Result set for table functions

    /**
     * The RESULT_SET data type.
     */
    RESULT_SET("RESULT_SET", Value.RESULT_SET),

    // Other JDBC

    /**
     * The JAVA_OBJECT data type.
     */
    JAVA_OBJECT("JAVA_OBJECT", Value.JAVA_OBJECT),

    // Other non-standard

    /**
     * The ENUM data type.
     */
    ENUM("ENUM", Value.ENUM),

    /**
     * The GEOMETRY data type.
     */
    GEOMETRY("GEOMETRY", Value.GEOMETRY),

    /**
     * The JSON data type.
     */
    JSON("JSON", Value.JSON),

    /**
     * The UUID data type.
     */
    UUID("UUID", Value.UUID),

    ;

    private final String name;

    private Integer valueType;

    private H2Type(String name, int valueType) {
        this.name = name;
        this.valueType = valueType;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getVendor() {
        return "com.h2database";
    }

    /**
     * Returns the vendor specific type number for the data type. The returned
     * value is actual only for the current version of H2.
     *
     * @return the vendor specific data type
     */
    @Override
    public Integer getVendorTypeNumber() {
        return valueType;
    }

}
