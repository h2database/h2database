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
//        if ("org.h2.test.store.int".equals(s)) {
//            return new IntegerType();
//        } else
        if (s.startsWith(RowType.PREFIX)) {
            return RowType.fromString(s, this);
        }
        return parent.buildDataType(s);
    }

    @Override
    public String getDataType(Class<?> objectClass) {
//        if (objectClass == Integer.class) {
//            return "org.h2.test.store.int";
//        }
        return parent.getDataType(objectClass);
    }

}
