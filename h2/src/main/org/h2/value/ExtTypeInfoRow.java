/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.message.DbException;
import org.h2.util.ParserUtil;

/**
 * Extended parameters of the ROW data type.
 */
public final class ExtTypeInfoRow extends ExtTypeInfo {

    private final LinkedHashMap<String, TypeInfo> fields;

    private int hash;

    /**
     * Creates new instance of extended parameters of ROW data type.
     *
     * @param fields
     *            fields
     */
    public ExtTypeInfoRow(Typed[] fields) {
        this(fields, fields.length);
    }

    /**
     * Creates new instance of extended parameters of ROW data type.
     *
     * @param fields
     *            fields
     * @param degree
     *            number of fields to use
     */
    public ExtTypeInfoRow(Typed[] fields, int degree) {
        if (degree > Constants.MAX_COLUMNS) {
            throw DbException.get(ErrorCode.TOO_MANY_COLUMNS_1, "" + Constants.MAX_COLUMNS);
        }
        LinkedHashMap<String, TypeInfo> map = new LinkedHashMap<>((int) Math.ceil(degree / .75));
        for (int i = 0; i < degree;) {
            TypeInfo t = fields[i].getType();
            map.put("C" + ++i, t);
        }
        this.fields = map;
    }

    /**
     * Creates new instance of extended parameters of ROW data type.
     *
     * @param fields
     *            fields
     */
    public ExtTypeInfoRow(LinkedHashMap<String, TypeInfo> fields) {
        if (fields.size() > Constants.MAX_COLUMNS) {
            throw DbException.get(ErrorCode.TOO_MANY_COLUMNS_1, "" + Constants.MAX_COLUMNS);
        }
        this.fields = fields;
    }

    /**
     * Returns fields.
     *
     * @return fields
     */
    public Set<Map.Entry<String, TypeInfo>> getFields() {
        return fields.entrySet();
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
            ParserUtil.quoteIdentifier(builder, field.getKey(), sqlFlags).append(' ');
            field.getValue().getSQL(builder, sqlFlags);
        }
        return builder.append(')');
    }

    @Override
    public int hashCode() {
        int h = hash;
        if (h != 0) {
            return h;
        }
        h = 67_378_403;
        for (Map.Entry<String, TypeInfo> entry : fields.entrySet()) {
            h = (h * 31 + entry.getKey().hashCode()) * 37 + entry.getValue().hashCode();
        }
        return hash = h;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj.getClass() != ExtTypeInfoRow.class) {
            return false;
        }
        LinkedHashMap<String, TypeInfo> fields2 = ((ExtTypeInfoRow) obj).fields;
        int degree = fields.size();
        if (degree != fields2.size()) {
            return false;
        }
        for (Iterator<Map.Entry<String, TypeInfo>> i1 = fields.entrySet().iterator(), i2 = fields2.entrySet()
                .iterator(); i1.hasNext();) {
            Map.Entry<String, TypeInfo> e1 = i1.next(), e2 = i2.next();
            if (!e1.getKey().equals(e2.getKey()) || !e1.getValue().equals(e2.getValue())) {
                return false;
            }
        }
        return true;
    }

}
