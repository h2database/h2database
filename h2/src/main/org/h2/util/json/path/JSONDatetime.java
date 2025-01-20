/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json.path;

import static org.h2.util.HasSQL.DEFAULT_SQL_FLAGS;

import org.h2.message.DbException;
import org.h2.util.json.JSONTarget;
import org.h2.util.json.JSONValue;
import org.h2.value.DataType;
import org.h2.value.Value;

/**
 * JSON datetime.
 *
 * <p>
 * This type of JSON scalar value is only used in SQL/JSON path language.
 * </p>
 */
public final class JSONDatetime extends JSONValue {

    private final Value value;

    public JSONDatetime(Value value) {
        if (!DataType.isDateTimeType(value.getValueType())) {
            throw new IllegalArgumentException("valueType=" + value.getValueType());
        }
        this.value = value;
    }

    @Override
    public void addTo(JSONTarget<?> target) {
        throw DbException.getInvalidValueException("JSON", "SQL/JSON path language datetime value");
    }

    /**
     * Returns the value.
     *
     * @return the value
     */
    public Value getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value.getSQL(DEFAULT_SQL_FLAGS);
    }

}
