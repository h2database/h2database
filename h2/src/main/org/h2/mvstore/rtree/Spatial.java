/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.rtree;

/**
 * Interface Spatial represents boxes in 2+ dimensional space,
 * where total ordering is not that straight-forward.
 * They can be used as keys for MVRTree.
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public interface Spatial
{
    /**
     * Get the minimum value for the given dimension.
     *
     * @param dim the dimension
     * @return the value
     */
    float min(int dim);

    /**
     * Set the minimum value for the given dimension.
     *
     * @param dim the dimension
     * @param x the value
     */
    void setMin(int dim, float x);

    /**
     * Get the maximum value for the given dimension.
     *
     * @param dim the dimension
     * @return the value
     */
    float max(int dim);

    /**
     * Set the maximum value for the given dimension.
     *
     * @param dim the dimension
     * @param x the value
     */
    void setMax(int dim, float x);

    /**
     * Creates a copy of this Spatial object with different id.
     *
     * @param id for the new Spatial object
     * @return a clone
     */
    Spatial clone(long id);

    /**
     * Get id of this Spatial object
     * @return id
     */
    long getId();

    /**
     * Test whether this object has no value
     * @return true if it is NULL, false otherwise
     */
    boolean isNull();

    /**
     * Check whether two objects are equals, but do not compare the id fields.
     *
     * @param o the other key
     * @return true if the contents are the same
     */
    boolean equalsIgnoringId(Spatial o);
}
