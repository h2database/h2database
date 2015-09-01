/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import com.vividsolutions.jts.geom.Envelope;

/**
 * ValueSpatial is an interface for each Value that represents spatial objects
 * (currently Geometry or georeferenced Raster). It defined a method that is
 * mandatory to define a spatial index for the Value.
 *
 * @author Jules Party
 */
public interface ValueSpatial {

    /**
     * Return the Envelope of the spatial object represented by the Value.
     * 
     */
    public Envelope getEnvelope();

    /**
     * Test if this spatial envelope intersects with the other spatial
     * envelope.
     *
     * @param r the other geometry
     * @return true if the two overlap
     */
    public boolean intersectsBoundingBox(ValueSpatial vs);

}
