/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.geometry;

import static org.h2.util.geometry.GeometryUtils.DIMENSION_SYSTEM_XYM;
import static org.h2.util.geometry.GeometryUtils.DIMENSION_SYSTEM_XYZ;
import static org.h2.util.geometry.GeometryUtils.GEOMETRY_COLLECTION;
import static org.h2.util.geometry.GeometryUtils.LINE_STRING;
import static org.h2.util.geometry.GeometryUtils.M;
import static org.h2.util.geometry.GeometryUtils.MULTI_LINE_STRING;
import static org.h2.util.geometry.GeometryUtils.MULTI_POINT;
import static org.h2.util.geometry.GeometryUtils.MULTI_POLYGON;
import static org.h2.util.geometry.GeometryUtils.POINT;
import static org.h2.util.geometry.GeometryUtils.POLYGON;
import static org.h2.util.geometry.GeometryUtils.X;
import static org.h2.util.geometry.GeometryUtils.Y;
import static org.h2.util.geometry.GeometryUtils.Z;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.util.geometry.EWKBUtils.EWKBTarget;
import org.h2.util.geometry.GeometryUtils.DimensionSystemTarget;
import org.h2.util.geometry.GeometryUtils.Target;
import org.h2.util.json.JSONArray;
import org.h2.util.json.JSONByteArrayTarget;
import org.h2.util.json.JSONBytesSource;
import org.h2.util.json.JSONNull;
import org.h2.util.json.JSONNumber;
import org.h2.util.json.JSONObject;
import org.h2.util.json.JSONString;
import org.h2.util.json.JSONValue;
import org.h2.util.json.JSONValueTarget;

/**
 * GeoJson format support for GEOMETRY data type.
 */
public final class GeoJsonUtils {

    /**
     * 0-based type names of geometries, subtract 1 from type code to get index
     * in this array.
     */
    static final String[] TYPES = { //
            "Point", //
            "LineString", //
            "Polygon", //
            "MultiPoint", //
            "MultiLineString", //
            "MultiPolygon", //
            "GeometryCollection", //
    };

    /**
     * Converter output target that writes a GeoJson.
     */
    public static final class GeoJsonTarget extends Target {

        private final JSONByteArrayTarget output;

        private final int dimensionSystem;

        private int type;

        private boolean inMulti, inMultiLine, wasEmpty;

        /**
         * Creates new GeoJson output target.
         *
         * @param output
         *            output JSON target
         * @param dimensionSystem
         *            dimension system to use
         */
        public GeoJsonTarget(JSONByteArrayTarget output, int dimensionSystem) {
            if (dimensionSystem == DIMENSION_SYSTEM_XYM) {
                throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1,
                        "M (XYM) dimension system is not supported in GeoJson");
            }
            this.output = output;
            this.dimensionSystem = dimensionSystem;
        }

        @Override
        protected void startPoint() {
            type = POINT;
            wasEmpty = false;
        }

        @Override
        protected void startLineString(int numPoints) {
            writeHeader(LINE_STRING);
            if (numPoints == 0) {
                output.endArray();
            }
        }

        @Override
        protected void startPolygon(int numInner, int numPoints) {
            writeHeader(POLYGON);
            if (numPoints == 0) {
                output.endArray();
            } else {
                output.startArray();
            }
        }

        @Override
        protected void startPolygonInner(int numInner) {
            output.startArray();
            if (numInner == 0) {
                output.endArray();
            }
        }

        @Override
        protected void endNonEmptyPolygon() {
            output.endArray();
        }

        @Override
        protected void startCollection(int type, int numItems) {
            writeHeader(type);
            if (type != GEOMETRY_COLLECTION) {
                inMulti = true;
                if (type == MULTI_LINE_STRING || type == MULTI_POLYGON) {
                    inMultiLine = true;
                }
            }
        }

        @Override
        protected Target startCollectionItem(int index, int total) {
            if (inMultiLine) {
                output.startArray();
            }
            return this;
        }

        @Override
        protected void endObject(int type) {
            switch (type) {
            case MULTI_POINT:
            case MULTI_LINE_STRING:
            case MULTI_POLYGON:
                inMultiLine = inMulti = false;
                //$FALL-THROUGH$
            case GEOMETRY_COLLECTION:
                output.endArray();
            }
            if (!inMulti && !wasEmpty) {
                output.endObject();
            }
        }

        private void writeHeader(int type) {
            this.type = type;
            wasEmpty = false;
            if (!inMulti) {
                writeStartObject(type);
            }
        }

        @Override
        protected void addCoordinate(double x, double y, double z, double m, int index, int total) {
            if (type == POINT) {
                if (Double.isNaN(x) && Double.isNaN(y) && Double.isNaN(z) && Double.isNaN(m)) {
                    wasEmpty = true;
                    output.valueNull();
                    return;
                }
                if (!inMulti) {
                    writeStartObject(POINT);
                }
            }
            output.startArray();
            writeDouble(x);
            writeDouble(y);
            if ((dimensionSystem & DIMENSION_SYSTEM_XYZ) != 0) {
                writeDouble(z);
            }
            if ((dimensionSystem & DIMENSION_SYSTEM_XYM) != 0) {
                writeDouble(m);
            }
            output.endArray();
            if (type != POINT && index + 1 == total) {
                output.endArray();
            }
        }

        private void writeStartObject(int type) {
            output.startObject();
            output.member("type");
            output.valueString(TYPES[type - 1]);
            output.member(type != GEOMETRY_COLLECTION ? "coordinates" : "geometries");
            if (type != POINT) {
                output.startArray();
            }
        }

        private void writeDouble(double v) {
            BigDecimal d = BigDecimal.valueOf(GeometryUtils.checkFinite(v));
            // stripTrailingZeros() does not work with 0.0 on Java 7
            output.valueNumber(d.signum() != 0 ? d.stripTrailingZeros() : BigDecimal.ZERO);
        }

    }

    /**
     * Converts EWKB with known dimension system to GeoJson.
     *
     * @param ewkb
     *            geometry object in EWKB format
     * @param dimensionSystem
     *            dimension system of the specified object, may be the same or
     *            smaller than its real dimension system. M dimension system is
     *            not supported.
     * @return GeoJson representation of the specified geometry
     * @throws DbException
     *             on unsupported dimension system
     */
    public static byte[] ewkbToGeoJson(byte[] ewkb, int dimensionSystem) {
        JSONByteArrayTarget output = new JSONByteArrayTarget();
        GeoJsonTarget target = new GeoJsonTarget(output, dimensionSystem);
        EWKBUtils.parseEWKB(ewkb, target);
        return output.getResult();
    }

    /**
     * Converts EWKB with known dimension system to GeoJson.
     *
     * @param json
     *            geometry object in GeoJson format
     * @param srid
     *            the SRID of geometry
     * @return GeoJson representation of the specified geometry
     * @throws DbException
     *             on unsupported dimension system
     */
    public static byte[] geoJsonToEwkb(byte[] json, int srid) {
        JSONValue v = JSONBytesSource.parse(json, new JSONValueTarget());
        DimensionSystemTarget dst = new DimensionSystemTarget();
        parse(v, dst);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        EWKBTarget target = new EWKBTarget(baos, dst.getDimensionSystem());
        target.init(srid);
        parse(v, target);
        return baos.toByteArray();
    }

    private static void parse(JSONValue v, GeometryUtils.Target target) {
        if (v instanceof JSONNull) {
            target.startPoint();
            target.addCoordinate(Double.NaN, Double.NaN, Double.NaN, Double.NaN, 0, 1);
            target.endObject(POINT);
        } else if (v instanceof JSONObject) {
            JSONObject o = (JSONObject) v;
            JSONValue t = o.getFirst("type");
            if (!(t instanceof JSONString)) {
                throw new IllegalArgumentException();
            }
            switch (((JSONString) t).getString()) {
            case "Point":
                parse(o, target, POINT);
                break;
            case "LineString":
                parse(o, target, LINE_STRING);
                break;
            case "Polygon":
                parse(o, target, POLYGON);
                break;
            case "MultiPoint":
                parse(o, target, MULTI_POINT);
                break;
            case "MultiLineString":
                parse(o, target, MULTI_LINE_STRING);
                break;
            case "MultiPolygon":
                parse(o, target, MULTI_POLYGON);
                break;
            case "GeometryCollection":
                parseGeometryCollection(o, target);
                break;
            default:
                throw new IllegalArgumentException();
            }
        } else {
            throw new IllegalArgumentException();
        }
    }

    private static void parse(JSONObject o, Target target, int type) {
        JSONValue t = o.getFirst("coordinates");
        if (!(t instanceof JSONArray)) {
            throw new IllegalArgumentException();
        }
        JSONArray a = (JSONArray) t;
        switch (type) {
        case POINT:
            target.startPoint();
            parseCoordinate(a, target, 0, 1);
            target.endObject(POINT);
            break;
        case LINE_STRING: {
            parseLineString(a, target);
            break;
        }
        case POLYGON: {
            parsePolygon(a, target);
            break;
        }
        case MULTI_POINT: {
            JSONValue[] points = a.getArray();
            int numPoints = points.length;
            target.startCollection(MULTI_POINT, numPoints);
            for (int i = 0; i < numPoints; i++) {
                target.startPoint();
                parseCoordinate(points[i], target, 0, 1);
                target.endObject(POINT);
                target.endCollectionItem(target, MULTI_POINT, i, numPoints);
            }
            target.endObject(MULTI_POINT);
            break;
        }
        case MULTI_LINE_STRING: {
            JSONValue[] strings = a.getArray();
            int numStrings = strings.length;
            target.startCollection(MULTI_LINE_STRING, numStrings);
            for (int i = 0; i < numStrings; i++) {
                JSONValue string = strings[i];
                if (!(string instanceof JSONArray)) {
                    throw new IllegalArgumentException();
                }
                parseLineString((JSONArray) string, target);
                target.endCollectionItem(target, MULTI_LINE_STRING, i, numStrings);
            }
            target.endObject(MULTI_LINE_STRING);
            break;
        }
        case MULTI_POLYGON: {
            JSONValue[] polygons = a.getArray();
            int numPolygons = polygons.length;
            target.startCollection(MULTI_POLYGON, numPolygons);
            for (int i = 0; i < numPolygons; i++) {
                JSONValue string = polygons[i];
                if (!(string instanceof JSONArray)) {
                    throw new IllegalArgumentException();
                }
                parsePolygon((JSONArray) string, target);
                target.endCollectionItem(target, MULTI_POLYGON, i, numPolygons);
            }
            target.endObject(MULTI_POLYGON);
            break;
        }
        default:
            throw new IllegalArgumentException();
        }
    }

    private static void parseGeometryCollection(JSONObject o, Target target) {
        JSONValue t = o.getFirst("geometries");
        if (!(t instanceof JSONArray)) {
            throw new IllegalArgumentException();
        }
        JSONArray a = (JSONArray) t;
        JSONValue[] geometries = a.getArray();
        int numGeometries = geometries.length;
        target.startCollection(GEOMETRY_COLLECTION, numGeometries);
        for (int i = 0; i < numGeometries; i++) {
            JSONValue geometry = geometries[i];
            parse(geometry, target);
            target.endCollectionItem(target, GEOMETRY_COLLECTION, i, numGeometries);
        }
        target.endObject(GEOMETRY_COLLECTION);
    }

    private static void parseLineString(JSONArray a, Target target) {
        JSONValue[] points = a.getArray();
        int numPoints = points.length;
        target.startLineString(numPoints);
        for (int i = 0; i < numPoints; i++) {
            parseCoordinate(points[i], target, i, numPoints);
        }
        target.endObject(LINE_STRING);
    }

    private static void parsePolygon(JSONArray a, Target target) {
        JSONValue[] rings = a.getArray();
        int numRings = rings.length;
        if (numRings == 0) {
            target.startPolygon(0, 0);
        } else {
            JSONValue ring = rings[0];
            if (!(ring instanceof JSONArray)) {
                throw new IllegalArgumentException();
            }
            JSONValue[] points = ((JSONArray) ring).getArray();
            target.startPolygon(numRings - 1, points.length);
            parseRing(points, target);
            for (int i = 1; i < numRings; i++) {
                ring = rings[i];
                if (!(ring instanceof JSONArray)) {
                    throw new IllegalArgumentException();
                }
                points = ((JSONArray) ring).getArray();
                target.startPolygonInner(points.length);
                parseRing(points, target);
            }
            target.endNonEmptyPolygon();
        }
        target.endObject(POLYGON);
    }

    private static void parseRing(JSONValue[] points, Target target) {
        int numPoints = points.length;
        for (int i = 0; i < numPoints; i++) {
            parseCoordinate(points[i], target, i, numPoints);
        }
    }

    private static void parseCoordinate(JSONValue v, Target target, int index, int total) {
        if (v instanceof JSONNull) {
            target.addCoordinate(Double.NaN, Double.NaN, Double.NaN, Double.NaN, 0, 1);
            return;
        }
        if (!(v instanceof JSONArray)) {
            throw new IllegalArgumentException();
        }
        JSONValue[] values = ((JSONArray) v).getArray();
        int length = values.length;
        if (length < 2) {
            throw new IllegalArgumentException();
        }
        target.addCoordinate(readCoordinate(values, X), readCoordinate(values, Y), readCoordinate(values, Z),
                readCoordinate(values, M), index, total);
    }

    private static double readCoordinate(JSONValue[] values, int index) {
        if (index >= values.length) {
            return Double.NaN;
        }
        JSONValue v = values[index];
        if (!(v instanceof JSONNumber)) {
            throw new IllegalArgumentException();
        }
        return ((JSONNumber) v).getBigDecimal().doubleValue();
    }

    private GeoJsonUtils() {
    }

}
