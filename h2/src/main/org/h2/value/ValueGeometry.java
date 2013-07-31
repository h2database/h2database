/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.h2.message.DbException;
import org.h2.util.StringUtils;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;

/**
 * Implementation of the GEOMETRY data type.
 * 
 * @author Thomas Mueller
 * @author Noel Grandin
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public class ValueGeometry extends Value {

    /**
     * The value.
     */
    private final Geometry geometry;

    private ValueGeometry(Geometry geometry) {
        this.geometry = geometry;
    }

    /**
     * Get or create a geometry value for the given geometry.
     *
     * @param o the geometry object (of type com.vividsolutions.jts.geom.Geometry)
     * @return the value
     */
    public static ValueGeometry getFromGeometry(Object o) {
        return get((Geometry) o);
    }

    private static ValueGeometry get(Geometry g) {
        return (ValueGeometry) Value.cache(new ValueGeometry(g));
    }

    /**
     * Get or create a geometry value for the given geometry.
     *
     * @param s the WKT representation of the geometry
     * @return the value
     */
    public static ValueGeometry get(String s) {
        return (ValueGeometry) Value.cache(new ValueGeometry(fromWKT(s)));
    }

    /**
     * Get or create a geometry value for the given geometry.
     *
     * @param bytes the WKB representation of the geometry
     * @return the value
     */
    public static ValueGeometry get(byte[] bytes) {
        return (ValueGeometry) Value.cache(new ValueGeometry(fromWKB(bytes)));
    }

    public Geometry getGeometry() {
        return geometry;
    }

    /**
     * Test if this geometry envelope intersects with the other geometry
     * envelope.
     * 
     * @param r the other geometry
     * @return true if the two envelopes overlaps
     */
    public boolean intersectsBoundingBox(ValueGeometry r) {
        // it is useless to cache the envelope as the Geometry object do this already
        return geometry.getEnvelopeInternal().intersects(r.getGeometry().getEnvelopeInternal());
    }

    /**
     * Get the intersection.
     * 
     * @param r the other geometry
     * @return the intersection of this geometry envelope and another geometry envelope
     */
    public ValueGeometry getEnvelopeIntersection(ValueGeometry r) {
        Envelope e1 = geometry.getEnvelopeInternal();
        Envelope e2 = r.getGeometry().getEnvelopeInternal();
        Envelope e3 = e1.intersection(e2);
        // try to re-use the object
        if (e3 == e1) {
            return this;
        } else if (e3 == e2) {
            return r;
        }
        GeometryFactory gf = new GeometryFactory();
        return get(gf.toGeometry(e3));
    }
    
    @Override
    public int getType() {
        return Value.GEOMETRY;
    }

    @Override
    public String getSQL() {
        return StringUtils.quoteStringSQL(toWKT()) + "'::Geometry";
    }

    @Override
    protected int compareSecure(Value v, CompareMode mode) {
        Geometry g = ((ValueGeometry) v).geometry;
        return geometry.compareTo(g);
    }

    @Override
    public String getString() {
        return toWKT();
    }

    @Override
    public long getPrecision() {
        return toWKT().length();
    }

    @Override
    public int hashCode() {
        return geometry.hashCode();
    }

    @Override
    public Object getObject() {
        return geometry;
    }

    @Override
    public byte[] getBytes() {
        return toWKB();
    }

    @Override
    public byte[] getBytesNoCopy() {
        return toWKB();
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setObject(parameterIndex, geometry);
    }

    @Override
    public int getDisplaySize() {
        return toWKT().length();
    }

    @Override
    public int getMemory() {
        return toWKB().length * 20 + 24;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueGeometry && geometry.equals(((ValueGeometry) other).geometry);
    }

    /**
     * Convert the value to the Well-Known-Text format.
     *
     * @return the well-known-text
     */
    public String toWKT() {
        return new WKTWriter().write(geometry);
    }

    /**
     * Convert to Well-Known-Binary format.
     *
     * @return the well-known-binary
     */
    public byte[] toWKB() {
        return new WKBWriter().write(geometry);
    }

    /**
     * Convert a Well-Known-Text to a Geometry object.
     *
     * @param s the well-known-text
     * @return the Geometry object
     */
    private static Geometry fromWKT(String s) {
        try {
            return new WKTReader().read(s);
        } catch (ParseException ex) {
            throw DbException.convert(ex);
        }
    }

    /**
     * Convert a Well-Known-Binary to a Geometry object.
     *
     * @param s the well-known-binary
     * @return the Geometry object
     */
    private static Geometry fromWKB(byte[] bytes) {
        try {
            return new WKBReader().read(bytes);
        } catch (ParseException ex) {
            throw DbException.convert(ex);
        }
    }

}
