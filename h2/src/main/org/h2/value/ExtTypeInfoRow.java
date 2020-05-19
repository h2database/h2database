/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.util.LinkedHashMap;
import java.util.Map;

import org.h2.command.Parser;

/**
 * Extended parameters of the ROW data type.
 */
public class ExtTypeInfoRow extends ExtTypeInfo {

    private final LinkedHashMap<String, TypeInfo> fields;

    /**
     * Creates new instance of extended parameters of ROW data type.
     *
     * @param fields
     *            fields
     */
    public ExtTypeInfoRow(LinkedHashMap<String, TypeInfo> fields) {
        this.fields = fields;
    }

    /**
     * Returns fields.
     *
     * @return fields
     */
    public LinkedHashMap<String, TypeInfo> getFields() {
        return fields;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        builder.append('(');
        boolean f = false;
        for (Map.Entry<String, TypeInfo> field : fields.entrySet()) {
            if (f) {
                builder.append(", ");
            }
            f = true;
            Parser.quoteIdentifier(builder, field.getKey(), sqlFlags).append(' ');
            field.getValue().getSQL(builder, sqlFlags);
        }
        return builder.append(')');
    }

}
