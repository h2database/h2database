/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.api;

import java.sql.SQLType;

import org.h2.value.ExtTypeInfoRow;
import org.h2.value.TypeInfo;
import org.h2.value.Value;

/**
 * Data types of H2.
 */
public final class H2Type implements SQLType {

    // Character strings

    /**
     * The CHARACTER data type.
     */
    public static final H2Type CHAR = new H2Type(TypeInfo.getTypeInfo(Value.CHAR), "CHARACTER");

    /**
     * The CHARACTER VARYING data type.
     */
    public static final H2Type VARCHAR = new H2Type(TypeInfo.TYPE_VARCHAR, "CHARACTER VARYING");

    /**
     * The CHARACTER LARGE OBJECT data type.
     */
    public static final H2Type CLOB = new H2Type(TypeInfo.TYPE_CLOB, "CHARACTER LARGE OBJECT");

    /**
     * The VARCHAR_IGNORECASE data type.
     */
    public static final H2Type VARCHAR_IGNORECASE = new H2Type(TypeInfo.TYPE_VARCHAR_IGNORECASE, "VARCHAR_IGNORECASE");

    // Binary strings

    /**
     * The BINARY data type.
     */
    public static final H2Type BINARY = new H2Type(TypeInfo.getTypeInfo(Value.BINARY), "BINARY");

    /**
     * The BINARY VARYING data type.
     */
    public static final H2Type VARBINARY = new H2Type(TypeInfo.TYPE_VARBINARY, "BINARY VARYING");

    /**
     * The BINARY LARGE OBJECT data type.
     */
    public static final H2Type BLOB = new H2Type(TypeInfo.TYPE_BLOB, "BINARY LARGE OBJECT");

    // Boolean

    /**
     * The BOOLEAN data type
     */
    public static final H2Type BOOLEAN = new H2Type(TypeInfo.TYPE_BOOLEAN, "BOOLEAN");

    // Exact numeric data types

    /**
     * The TINYINT data type.
     */
    public static final H2Type TINYINT = new H2Type(TypeInfo.TYPE_TINYINT, "TINYINT");

    /**
     * The SMALLINT data type.
     */
    public static final H2Type SMALLINT = new H2Type(TypeInfo.TYPE_SMALLINT, "SMALLINT");

    /**
     * The INTEGER data type.
     */
    public static final H2Type INTEGER = new H2Type(TypeInfo.TYPE_INTEGER, "INTEGER");

    /**
     * The BIGINT data type.
     */
    public static final H2Type BIGINT = new H2Type(TypeInfo.TYPE_BIGINT, "BIGINT");

    /**
     * The NUMERIC data type.
     */
    public static final H2Type NUMERIC = new H2Type(TypeInfo.TYPE_NUMERIC_FLOATING_POINT, "NUMERIC");

    // Approximate numeric data types

    /**
     * The REAL data type.
     */
    public static final H2Type REAL = new H2Type(TypeInfo.TYPE_REAL, "REAL");

    /**
     * The DOUBLE PRECISION data type.
     */
    public static final H2Type DOUBLE_PRECISION = new H2Type(TypeInfo.TYPE_DOUBLE, "DOUBLE PRECISION");

    // Decimal floating-point type

    /**
     * The DECFLOAT data type.
     */
    public static final H2Type DECFLOAT = new H2Type(TypeInfo.TYPE_DECFLOAT, "DECFLOAT");

    // Date-time data types

    /**
     * The DATE data type.
     */
    public static final H2Type DATE = new H2Type(TypeInfo.TYPE_DATE, "DATE");

    /**
     * The TIME data type.
     */
    public static final H2Type TIME = new H2Type(TypeInfo.TYPE_TIME, "TIME");

    /**
     * The TIME WITH TIME ZONE data type.
     */
    public static final H2Type TIME_WITH_TIME_ZONE = new H2Type(TypeInfo.TYPE_TIME_TZ, "TIME WITH TIME ZONE");

    /**
     * The TIMESTAMP data type.
     */
    public static final H2Type TIMESTAMP = new H2Type(TypeInfo.TYPE_TIMESTAMP, "TIMESTAMP");

    /**
     * The TIMESTAMP WITH TIME ZONE data type.
     */
    public static final H2Type TIMESTAMP_WITH_TIME_ZONE = new H2Type(TypeInfo.TYPE_TIMESTAMP_TZ,
            "TIMESTAMP WITH TIME ZONE");

    // Intervals

    /**
     * The INTERVAL YEAR data type.
     */
    public static final H2Type INTERVAL_YEAR = new H2Type(TypeInfo.getTypeInfo(Value.INTERVAL_YEAR), "INTERVAL_YEAR");

    /**
     * The INTERVAL MONTH data type.
     */
    public static final H2Type INTERVAL_MONTH = new H2Type(TypeInfo.getTypeInfo(Value.INTERVAL_MONTH),
            "INTERVAL_MONTH");

    /**
     * The INTERVAL DAY data type.
     */
    public static final H2Type INTERVAL_DAY = new H2Type(TypeInfo.TYPE_INTERVAL_DAY, "INTERVAL_DAY");

    /**
     * The INTERVAL HOUR data type.
     */
    public static final H2Type INTERVAL_HOUR = new H2Type(TypeInfo.getTypeInfo(Value.INTERVAL_HOUR), "INTERVAL_HOUR");

    /**
     * The INTERVAL MINUTE data type.
     */
    public static final H2Type INTERVAL_MINUTE = new H2Type(TypeInfo.getTypeInfo(Value.INTERVAL_MINUTE),
            "INTERVAL_MINUTE");

    /**
     * The INTERVAL SECOND data type.
     */
    public static final H2Type INTERVAL_SECOND = new H2Type(TypeInfo.getTypeInfo(Value.INTERVAL_SECOND),
            "INTERVAL_SECOND");

    /**
     * The INTERVAL YEAR TO MONTH data type.
     */
    public static final H2Type INTERVAL_YEAR_TO_MONTH = new H2Type(TypeInfo.TYPE_INTERVAL_YEAR_TO_MONTH,
            "INTERVAL_YEAR_TO_MONTH");

    /**
     * The INTERVAL DAY TO HOUR data type.
     */
    public static final H2Type INTERVAL_DAY_TO_HOUR = new H2Type(TypeInfo.getTypeInfo(Value.INTERVAL_DAY_TO_HOUR),
            "INTERVAL_DAY_TO_HOUR");

    /**
     * The INTERVAL DAY TO MINUTE data type.
     */
    public static final H2Type INTERVAL_DAY_TO_MINUTE = new H2Type(TypeInfo.getTypeInfo(Value.INTERVAL_DAY_TO_MINUTE),
            "INTERVAL_DAY_TO_MINUTE");

    /**
     * The INTERVAL DAY TO SECOND data type.
     */
    public static final H2Type INTERVAL_DAY_TO_SECOND = new H2Type(TypeInfo.TYPE_INTERVAL_DAY_TO_SECOND,
            "INTERVAL_DAY_TO_SECOND");

    /**
     * The INTERVAL HOUR TO MINUTE data type.
     */
    public static final H2Type INTERVAL_HOUR_TO_MINUTE = new H2Type( //
            TypeInfo.getTypeInfo(Value.INTERVAL_HOUR_TO_MINUTE), "INTERVAL_HOUR_TO_MINUTE");

    /**
     * The INTERVAL HOUR TO SECOND data type.
     */
    public static final H2Type INTERVAL_HOUR_TO_SECOND = new H2Type(TypeInfo.TYPE_INTERVAL_HOUR_TO_SECOND,
            "INTERVAL_HOUR_TO_SECOND");

    /**
     * The INTERVAL MINUTE TO SECOND data type.
     */
    public static final H2Type INTERVAL_MINUTE_TO_SECOND = new H2Type(
            TypeInfo.getTypeInfo(Value.INTERVAL_MINUTE_TO_SECOND), "INTERVAL_MINUTE_TO_SECOND");

    // Other JDBC

    /**
     * The JAVA_OBJECT data type.
     */
    public static final H2Type JAVA_OBJECT = new H2Type(TypeInfo.TYPE_JAVA_OBJECT, "JAVA_OBJECT");

    // Other non-standard

    /**
     * The ENUM data type.
     */
    public static final H2Type ENUM = new H2Type(TypeInfo.TYPE_ENUM_UNDEFINED, "ENUM");

    /**
     * The GEOMETRY data type.
     */
    public static final H2Type GEOMETRY = new H2Type(TypeInfo.TYPE_GEOMETRY, "GEOMETRY");

    /**
     * The JSON data type.
     */
    public static final H2Type JSON = new H2Type(TypeInfo.TYPE_JSON, "JSON");

    /**
     * The UUID data type.
     */
    public static final H2Type UUID = new H2Type(TypeInfo.TYPE_UUID, "UUID");

    // Collections

    // Use arrayOf() for ARRAY

    // Use row() for ROW

    /**
     * Returns ARRAY data type with the specified component type.
     *
     * @param componentType
     *            the type of elements
     * @return ARRAY data type
     */
    public static H2Type array(H2Type componentType) {
        return new H2Type(TypeInfo.getTypeInfo(Value.ARRAY, -1L, -1, componentType.typeInfo),
                "array(" + componentType.field + ')');
    }

    /**
     * Returns ROW data type with specified types of fields and default names.
     *
     * @param fieldTypes
     *            the type of fields
     * @return ROW data type
     */
    public static H2Type row(H2Type... fieldTypes) {
        int degree = fieldTypes.length;
        TypeInfo[] row = new TypeInfo[degree];
        StringBuilder builder = new StringBuilder("row(");
        for (int i = 0; i < degree; i++) {
            H2Type t = fieldTypes[i];
            row[i] = t.typeInfo;
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(t.field);
        }
        return new H2Type(TypeInfo.getTypeInfo(Value.ROW, -1L, -1, new ExtTypeInfoRow(row)),
                builder.append(')').toString());
    }

    private TypeInfo typeInfo;

    private String field;

    private H2Type(TypeInfo typeInfo, String field) {
        this.typeInfo = typeInfo;
        this.field = "H2Type." + field;
    }

    @Override
    public String getName() {
        return typeInfo.toString();
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
        return typeInfo.getValueType();
    }

    @Override
    public String toString() {
        return field;
    }

}
