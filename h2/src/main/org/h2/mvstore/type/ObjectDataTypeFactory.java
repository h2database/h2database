/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.mvstore.type;

import org.h2.mvstore.rtree.SpatialDataType;

/**
 * A data type factory.
 */
public class ObjectDataTypeFactory implements DataTypeFactory {

    @Override
    public void setParent(DataTypeFactory parent) {
        // never called for this factory
    }

    @Override
    public DataType buildDataType(String s) {
        if ("s".equals(s)) {
            return SpatialDataType.fromString(s);
        } else if ("o".equals(s)) {
            return new ObjectDataType();
        }
        return null;
    }

    @Override
    public String getDataType(Class<?> objectClass) {
        if (objectClass == SpatialDataType.class) {
            return "s";
        }
        return "o";
    }

}
