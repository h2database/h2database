/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

/**
 * A unique spatial key.
 */
public class SpatialKey {

    public float[] min;
    public float[] max;
    public long id;

    public static SpatialKey create(long id, float... minMax) {
        SpatialKey k = new SpatialKey();
        k.id = id;
        int dimensions = minMax.length / 2;
        k.min = new float[dimensions];
        k.max = new float[dimensions];
        for (int i = 0; i < dimensions; i++) {
            k.min[i] = minMax[i + i];
            k.max[i] = minMax[i + i + 1];
        }
        return k;
    }

    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append(id).append(": (");
        for (int i = 0; i < min.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(min[i]).append('/').append(max[i]);
        }
        return buff.append(")").toString();
    }

}
