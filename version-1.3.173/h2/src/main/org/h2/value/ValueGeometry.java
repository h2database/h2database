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

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;

/**
 * Implementation of the GEOMETRY data type.
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
     * Check whether two values intersect.
     *
     * @param r the second value
     * @return true if they intersect
     */
    public boolean intersects(ValueGeometry r) {
        return geometry.intersects(r.getGeometry());
    }

    /**
     * Get the intersection of two values.
     *
     * @param r the second value
     * @return the intersection
     */
    public Value intersection(ValueGeometry r) {
        return get(geometry.intersection(r.geometry));
    }

    /**
     * Get the union of two values.
     *
     * @param r the second value
     * @return the union
     */
    public Value union(ValueGeometry r) {
        return get(geometry.union(r.geometry));
    }

    @Override
    public int getType() {
        return Value.GEOMETRY;
    }

    @Override
    public String getSQL() {
        return StringUtils.quoteStringSQL(toWKT());
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
        WKTWriter w = new WKTWriter();
        return w.write(geometry);
    }

    /**
     * Convert to value to the Well-Known-Binary format.
     *
     * @return the well-known-binary
     */
    public byte[] toWKB() {
        WKBWriter w = new WKBWriter();
        return w.write(geometry);
    }

    /**
     * Convert a Well-Known-Text to a Geometry object.
     *
     * @param s the well-known-text
     * @return the Geometry object
     */
    private static Geometry fromWKT(String s) {
        WKTReader r = new WKTReader();
        try {
            return r.read(s);
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
        WKBReader r = new WKBReader();
        try {
            return r.read(bytes);
        } catch (ParseException ex) {
            throw DbException.convert(ex);
        }
    }

}
