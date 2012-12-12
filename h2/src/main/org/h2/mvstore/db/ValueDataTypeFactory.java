/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.DataTypeFactory;
import org.h2.store.DataHandler;
import org.h2.value.CompareMode;

/**
 * A data type factory for rows.
 */
public class ValueDataTypeFactory implements DataTypeFactory {

    private final CompareMode compareMode;
    private final DataHandler handler;
    private DataTypeFactory parent;

    ValueDataTypeFactory(CompareMode compareMode, DataHandler handler) {
        this.compareMode = compareMode;
        this.handler = handler;
    }

    @Override
    public void setParent(DataTypeFactory parent) {
        this.parent = parent;
    }

    @Override
    public DataType buildDataType(String s) {
        if (s.startsWith(ValueArrayDataType.PREFIX)) {
            return ValueArrayDataType.fromString(compareMode, handler, s);
        }
        return parent.buildDataType(s);
    }

    @Override
    public String getDataType(Class<?> objectClass) {
        return parent.getDataType(objectClass);
    }

}
