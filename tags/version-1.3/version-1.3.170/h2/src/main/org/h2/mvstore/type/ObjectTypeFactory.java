/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.mvstore.type;

import org.h2.mvstore.rtree.SpatialType;

/**
 * A data type factory.
 */
public class ObjectTypeFactory implements DataTypeFactory {

    @Override
    public void setParent(DataTypeFactory parent) {
        // never called for this factory
    }

    @Override
    public DataType buildDataType(String s) {
        if ("s".equals(s)) {
            return SpatialType.fromString(s);
        } else if ("o".equals(s)) {
            return new ObjectType();
        }
        return null;
    }

    @Override
    public String getDataType(Class<?> objectClass) {
        if (objectClass == SpatialType.class) {
            return "s";
        }
        return "o";
    }

}
