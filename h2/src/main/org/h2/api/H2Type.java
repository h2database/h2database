/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.api;

import java.sql.SQLType;

/**
 * Data types of H2.
 */
public enum H2Type implements SQLType {

    // Character strings

    /**
     * The CHAR data type.
     */
    CHAR("CHAR"),

    /**
     * The VARCHAR data type.
     */
    VARCHAR("VARCHAR"),

    /**
     * The CLOB data type.
     */
    CLOB("CLOB"),

    /**
     * The VARCHAR_IGNORECASE data type.
     */
    VARCHAR_IGNORECASE("VARCHAR_IGNORECASE"),

    // Binary strings

    /**
     * The VARBINARY data type.
     */
    BINARY("BINARY"),

    /**
     * The VARBINARY data type.
     */
    VARBINARY("VARBINARY"),

    /**
     * The BLOB data type.
     */
    BLOB("BLOB"),

    // Boolean

    /**
     * The BOOLEAN data type
     */
    BOOLEAN("BOOLEAN"),

    // Exact numeric data types

    /**
     * The TINYINT data type.
     */
    TINYINT("TINYINT"),

    /**
     * The SMALLINT data type.
     */
    SMALLINT("SMALLINT"),

    /**
     * The INTEGER data type.
     */
    INTEGER("INTEGER"),

    /**
     * The BIGINT data type.
     */
    BIGINT("BIGINT"),

    /**
     * The NUMERIC data type.
     */
    NUMERIC("NUMERIC"),

    // Approximate numeric data types

    /**
     * The REAL data type.
     */
    REAL("REAL"),

    /**
     * The DOUBLE PRECISION data type.
     */
    DOUBLE_PRECISION("DOUBLE PRECISION"),

    // Date-time data types

    /**
     * The DATE data type.
     */
    DATE("DATE"),

    /**
     * The TIME data type.
     */
    TIME("TIME"),

    /**
     * The TIME WITH TIME ZONE data type.
     */
    TIME_WITH_TIME_ZONE("TIME WITH TIME ZONE"),

    /**
     * The TIMESTAMP data type.
     */
    TIMESTAMP("TIMESTAMP"),

    /**
     * The TIMESTAMP WITH TIME ZONE data type.
     */
    TIMESTAMP_WITH_TIME_ZONE("TIMESTAMP WITH TIME ZONE"),

    // Intervals

    /**
     * The INTERVAL YEAR data type.
     */
    INTERVAL_YEAR("INTERVAL YEAR"),

    /**
     * The INTERVAL MONTH data type.
     */
    INTERVAL_MONTH("INTERVAL MONTH"),

    /**
     * The INTERVAL DAY data type.
     */
    INTERVAL_DAY("INTERVAL DAY"),

    /**
     * The INTERVAL HOUR data type.
     */
    INTERVAL_HOUR("INTERVAL HOUR"),

    /**
     * The INTERVAL MINUTE data type.
     */
    INTERVAL_MINUTE("INTERVAL MINUTE"),

    /**
     * The INTERVAL SECOND data type.
     */
    INTERVAL_SECOND("INTERVAL SECOND"),

    /**
     * The INTERVAL YEAR TO MONTH data type.
     */
    INTERVAL_YEAR_TO_MONTH("INTERVAL YEAR TO MONTH"),

    /**
     * The INTERVAL DAY TO HOUR data type.
     */
    INTERVAL_DAY_TO_HOUR("INTERVAL DAY TO HOUR"),

    /**
     * The INTERVAL DAY TO MINUTE data type.
     */
    INTERVAL_DAY_TO_MINUTE("INTERVAL DAY TO MINUTE"),

    /**
     * The INTERVAL DAY TO SECOND data type.
     */
    INTERVAL_DAY_TO_SECOND("INTERVAL DAY TO SECOND"),

    /**
     * The INTERVAL HOUR TO MINUTE data type.
     */
    INTERVAL_HOUR_TO_MINUTE("INTERVAL HOUR TO MINUTE"),

    /**
     * The INTERVAL HOUR TO SECOND data type.
     */
    INTERVAL_HOUR_TO_SECOND("INTERVAL HOUR TO SECOND"),

    /**
     * The INTERVAL MINUTE TO SECOND data type.
     */
    INTERVAL_MINUTE_TO_SECOND("INTERVAL MINUTE TO SECOND"),

    // Other JDBC

    /**
     * The JAVA_OBJECT data type.
     */
    JAVA_OBJECT("JAVA_OBJECT"),

    // Other non-standard

    /**
     * The ENUM data type.
     */
    ENUM("ENUM"),

    /**
     * The GEOMETRY data type.
     */
    GEOMETRY("GEOMETRY"),

    /**
     * The JSON data type.
     */
    JSON("JSON"),

    /**
     * The UUID data type.
     */
    UUID("UUID"),

    // Collections

    /**
     * The ARRAY data type.
     */
    ARRAY("ARRAY"),

    // Row

    /**
     * The ROW data type.
     */
    ROW("ROW"),

    // Result set for table functions

    /**
     * The RESULT_SET data type.
     */
    RESULT_SET("RESULT_SET"),

    ;

    private final String name;

    private Integer valueType;

    private H2Type(String name) {
        this.name = name;
        valueType = ordinal() + 1;
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
