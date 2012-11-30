/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.store;

import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.DataTypeFactory;
/**
 * A data type factory.
 */
public class SampleTypeFactory implements DataTypeFactory {

    private DataTypeFactory parent;

    @Override
    public void setParent(DataTypeFactory parent) {
        this.parent = parent;
    }

    @Override
    public DataType buildDataType(String s) {
        if (s.startsWith(RowDataType.PREFIX)) {
            return RowDataType.fromString(s, this);
        }
        return parent.buildDataType(s);
    }

    @Override
    public String getDataType(Class<?> objectClass) {
        return parent.getDataType(objectClass);
    }

}
