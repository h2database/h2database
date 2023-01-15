/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.rtree;

import java.util.Arrays;

/**
 * Class BasicSpatialImpl.
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
final class DefaultSpatial implements Spatial
{
    private final long id;
    private final float[] minMax;

    /**
     * Create a new key.
     *
     * @param id the id
     * @param minMax min x, max x, min y, max y, and so on
     */
    public DefaultSpatial(long id, float... minMax) {
        this.id = id;
        this.minMax = minMax;
    }

    private DefaultSpatial(long id, DefaultSpatial other) {
        this.id = id;
        this.minMax = other.minMax.clone();
    }

    @Override
    public float min(int dim) {
        return minMax[dim + dim];
    }

    @Override
    public void setMin(int dim, float x) {
        minMax[dim + dim] = x;
    }

    @Override
    public float max(int dim) {
        return minMax[dim + dim + 1];
    }

    @Override
    public void setMax(int dim, float x) {
        minMax[dim + dim + 1] = x;
    }

    @Override
    public Spatial clone(long id) {
        return new DefaultSpatial(id, this);
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public boolean isNull() {
        return minMax.length == 0;
    }

    @Override
    public boolean equalsIgnoringId(Spatial o) {
        return Arrays.equals(minMax, ((DefaultSpatial)o).minMax);
    }
}
