/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.api;

import org.h2.value.DataType;
import org.h2.value.Value;

/**
 * Custom data type handler
 * Provides means to plug-in custom data types support
 */
public interface CustomDataTypesHandler {
    /**
     * Get custom data type given its name
     *
     * @param name data type name
     * @return custom data type
     */
    DataType getDataTypeByName(String name);

    /**
     * Get custom data type given its integer id
     *
     * @param type identifier of a data type
     * @return custom data type
     */
    DataType getDataTypeById(int type);

    /**
     * Get order for custom data type given its integer id
     *
     * @param type identifier of a data type
     * @return order associated with custom data type
     */
    int getDataTypeOrder(int type);

    /**
     * Convert the provided source value into value of given target data type
     * Shall implement conversions to and from custom data types.
     *
     * @param source source value
     * @param targetType identifier of target data type
     * @return converted value
     */
    Value convert(Value source, int targetType);
}
