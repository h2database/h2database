/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.util.Arrays;


/**
 * A unique spatial key.
 */
public class SpatialKey {

    public long id;
    private float[] minMax;

    /**
     * Create a new key.
     *
     * @param id the id
     * @param minMax min x, max x, min y, max y, and so on
     */
    public SpatialKey(long id, float... minMax) {
        this.id = id;
        this.minMax = minMax;
    }

    public float min(int dim) {
        return minMax[dim + dim];
    }

    public void setMin(int dim, float x) {
        minMax[dim + dim] = x;
    }

    public float max(int dim) {
        return minMax[dim + dim + 1];
    }

    public void setMax(int dim, float x) {
        minMax[dim + dim + 1] = x;
    }

    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append(id).append(": (");
        for (int i = 0; i < minMax.length; i += 2) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(minMax[i]).append('/').append(minMax[i + 1]);
        }
        return buff.append(")").toString();
    }

    public int hashCode() {
        return (int) ((id >>> 32) ^ id);
    }

    public boolean equals(Object other) {
        if (!(other instanceof SpatialKey)) {
            return false;
        }
        SpatialKey o = (SpatialKey) other;
        return Arrays.equals(minMax, o.minMax);
    }

}
