/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import org.h2.engine.Mode;
import org.h2.message.DbException;
import org.h2.util.Bits;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.util.geometry.EWKTUtils;
import org.h2.util.geometry.GeometryUtils;
import org.h2.util.geometry.JTSUtils;
import org.locationtech.jts.geom.Geometry;

/**
 * Implementation of the GEOMETRY data type.
 *
 * @author Thomas Mueller
 * @author Noel Grandin
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public class ValueGeometry extends Value {

    /**
     * As conversion from/to WKB cost a significant amount of CPU cycles, WKB
     * are kept in ValueGeometry instance.
     *
     * We always calculate the WKB, because not all WKT values can be
     * represented in WKB, but since we persist it in WKB format, it has to be
     * valid in WKB
     */
    private final byte[] bytes;

    private final int hashCode;

    /**
     * The value. Converted from WKB only on request as conversion from/to WKB
     * cost a significant amount of CPU cycles.
     */
    private Object geometry;

    /**
     * The envelope of the value. Calculated only on request.
     */
    private double[] envelope;

    /**
     * Create a new geometry objects.
     *
     * @param bytes the EWKB bytes
     */
    private ValueGeometry(byte[] bytes) {
        this.bytes = bytes;
        this.hashCode = Arrays.hashCode(bytes);
    }

    /**
     * Get or create a geometry value for the given geometry.
     *
     * @param o the geometry object (of type
     *            org.locationtech.jts.geom.Geometry)
     * @return the value
     */
    public static ValueGeometry getFromGeometry(Object o) {
        return get(JTSUtils.geometry2ewkb((Geometry) o));
    }

    /**
     * Get or create a geometry value for the given geometry.
     *
     * @param s the WKT or EWKT representation of the geometry
     * @return the value
     */
    public static ValueGeometry get(String s) {
        try {
            return get(EWKTUtils.ewkt2ewkb(s));
        } catch (RuntimeException ex) {
            throw DbException.convert(ex);
        }
    }

    /**
     * Get or create a geometry value for the given geometry.
     *
     * @param s the WKT representation of the geometry
     * @param srid the srid of the object
     * @return the value
     */
    public static ValueGeometry get(String s, int srid) {
        // This method is not used in H2, but preserved for H2GIS
        return get(srid == 0 ? s : "SRID=" + srid + ';' + s);
    }

    /**
     * Get or create a geometry value for the given geometry.
     *
     * @param bytes the WKB representation of the geometry
     * @return the value
     */
    public static ValueGeometry get(byte[] bytes) {
        return (ValueGeometry) Value.cache(new ValueGeometry(bytes));
    }

    /**
     * Get a copy of geometry object. Geometry object is mutable. The returned
     * object is therefore copied before returning.
     *
     * @return a copy of the geometry object
     */
    public Object getGeometry() {
        if (geometry == null) {
            try {
                geometry = JTSUtils.ewkb2geometry(bytes);
            } catch (RuntimeException ex) {
                throw DbException.convert(ex);
            }
        }
        return ((Geometry) geometry).copy();
    }

    /**
     * Return the SRID (Spatial Reference Identifier).
     *
     * @return spatial reference identifier
     */
    public int getSRID() {
        if (bytes.length >= 9) {
            boolean bigEndian;
            switch (bytes[0]) {
            case 0:
                bigEndian = true;
                break;
            case 1:
                bigEndian = false;
                break;
            default:
                return 0;
            }
            if ((bytes[bigEndian ? 1 : 4] & 0x20) != 0) {
                int srid = Bits.readInt(bytes, 5);
                if (!bigEndian) {
                    srid = Integer.reverseBytes(srid);
                }
                return srid;
            }
        }
        return 0;
    }

    /**
     * Return an envelope of this geometry. Do not modify the returned value.
     *
     * @return envelope of this geometry
     */
    public double[] getEnvelopeNoCopy() {
        if (envelope == null) {
            envelope = GeometryUtils.getEnvelope(bytes);
        }
        return envelope;
    }

    /**
     * Test if this geometry envelope intersects with the other geometry
     * envelope.
     *
     * @param r the other geometry
     * @return true if the two overlap
     */
    public boolean intersectsBoundingBox(ValueGeometry r) {
        return GeometryUtils.intersects(getEnvelopeNoCopy(), r.getEnvelopeNoCopy());
    }

    /**
     * Get the union.
     *
     * @param r the other geometry
     * @return the union of this geometry envelope and another geometry envelope
     */
    public Value getEnvelopeUnion(ValueGeometry r) {
        return get(GeometryUtils.envelope2wkb(GeometryUtils.union(getEnvelopeNoCopy(), r.getEnvelopeNoCopy())));
    }

    @Override
    public int getType() {
        return Value.GEOMETRY;
    }

    @Override
    public String getSQL() {
        // Using bytes is faster than converting to EWKT.
        return "X'" + StringUtils.convertBytesToHex(getBytesNoCopy()) + "'::Geometry";
    }

    @Override
    public int compareTypeSafe(Value v, CompareMode mode) {
        return Bits.compareNotNullUnsigned(bytes, ((ValueGeometry) v).bytes);
    }

    @Override
    public String getString() {
        return getEWKT();
    }

    @Override
    public long getPrecision() {
        return 0;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public Object getObject() {
        if (DataType.GEOMETRY_CLASS != null) {
            return getGeometry();
        }
        return getEWKT();
    }

    @Override
    public byte[] getBytes() {
        return Utils.cloneByteArray(bytes);
    }

    @Override
    public byte[] getBytesNoCopy() {
        return bytes;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setBytes(parameterIndex, bytes);
    }

    @Override
    public int getDisplaySize() {
        return getEWKT().length();
    }

    @Override
    public int getMemory() {
        return getEWKB().length * 20 + 24;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueGeometry && Arrays.equals(getEWKB(), ((ValueGeometry) other).getEWKB());
    }

    /**
     * Get the value in Extended Well-Known Text format.
     *
     * @return the extended well-known text
     */
    public String getEWKT() {
        return EWKTUtils.ewkb2ewkt(bytes);
    }

    /**
     * Get the value in extended Well-Known Binary format.
     *
     * @return the extended well-known binary
     */
    public byte[] getEWKB() {
        return bytes;
    }

    @Override
    public Value convertTo(int targetType, int precision, Mode mode, Object column, String[] enumerators) {
        if (targetType == Value.JAVA_OBJECT) {
            return this;
        }
        return super.convertTo(targetType, precision, mode, column, null);
    }

}
