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
    private final com.vividsolutions.jts.geom.Geometry geometry;

    private ValueGeometry(com.vividsolutions.jts.geom.Geometry geometry) {
        this.geometry = geometry;
    }

    /**
     * Get or create a geometry value for the given geometry.
     *
     * @param g the geometry
     * @return the value
     */
    public static ValueGeometry get(com.vividsolutions.jts.geom.Geometry g) {
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
    
    @Override
    public int getType() {
        return Value.GEOMETRY;
    }

    @Override
    public String getSQL() {
        return "'" + toWKT() + "'";
    }

    @Override
    protected int compareSecure(Value v, CompareMode mode) {
        com.vividsolutions.jts.geom.Geometry g = ((ValueGeometry) v).geometry;
        return this.geometry.compareTo(g);
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
        return this.geometry.hashCode();
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
        return toWKB().length + 24;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueGeometry && geometry.equals(((ValueGeometry) other).geometry);
    }
    
    /**
     * Convert to Well-Known-Text format.
     */
    public String toWKT() {
        WKTWriter w = new WKTWriter();
        return w.write(this.geometry);
    }

    /**
     * Convert to Well-Known-Binary format.
     */
    public byte[] toWKB() {
        WKBWriter w = new WKBWriter();
        return w.write(this.geometry);
    }
    
    /**
     * Convert from Well-Known-Text format.
     */
    private static com.vividsolutions.jts.geom.Geometry fromWKT(String s) {
        WKTReader r = new WKTReader();
        try {
            return r.read(s);
        } catch (ParseException ex) {
            throw DbException.convert(ex);
        }
    }
    
    /**
     * Convert from Well-Known-Binary format.
     */
    private static com.vividsolutions.jts.geom.Geometry fromWKB(byte[] bytes) {
        WKBReader r = new WKBReader();
        try {
            return r.read(bytes);
        } catch (ParseException ex) {
            throw DbException.convert(ex);
        }
    }
}
