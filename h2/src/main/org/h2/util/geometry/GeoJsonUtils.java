/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.geometry;

import static org.h2.util.geometry.GeometryUtils.DIMENSION_SYSTEM_XYM;
import static org.h2.util.geometry.GeometryUtils.DIMENSION_SYSTEM_XYZ;
import static org.h2.util.geometry.GeometryUtils.GEOMETRY_COLLECTION;
import static org.h2.util.geometry.GeometryUtils.LINE_STRING;
import static org.h2.util.geometry.GeometryUtils.MULTI_LINE_STRING;
import static org.h2.util.geometry.GeometryUtils.MULTI_POINT;
import static org.h2.util.geometry.GeometryUtils.MULTI_POLYGON;
import static org.h2.util.geometry.GeometryUtils.POINT;
import static org.h2.util.geometry.GeometryUtils.POLYGON;

import java.math.BigDecimal;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.util.geometry.GeometryUtils.Target;
import org.h2.util.json.JSONStringTarget;

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

        private final JSONStringTarget output;

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
        public GeoJsonTarget(JSONStringTarget output, int dimensionSystem) {
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
            output.valueSeparator();
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
            if (index != 0) {
                output.valueSeparator();
            }
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
            if (index > 0) {
                output.valueSeparator();
            }
            output.startArray();
            writeDouble(x);
            output.valueSeparator();
            writeDouble(y);
            if ((dimensionSystem & DIMENSION_SYSTEM_XYZ) != 0) {
                output.valueSeparator();
                writeDouble(z);
            }
            if ((dimensionSystem & DIMENSION_SYSTEM_XYM) != 0) {
                output.valueSeparator();
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
            output.valueSeparator();
            output.member(type != GEOMETRY_COLLECTION ? "coordinates" : "geometries");
            if (type != POINT) {
                output.startArray();
            }
        }

        private void writeDouble(double v) {
            output.valueNumber(BigDecimal.valueOf(GeometryUtils.checkFinite(v)).stripTrailingZeros());
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
    public static String ewkbToGeoJson(byte[] ewkb, int dimensionSystem) {
        JSONStringTarget output = new JSONStringTarget();
        GeoJsonTarget target = new GeoJsonTarget(output, dimensionSystem);
        EWKBUtils.parseEWKB(ewkb, target);
        return output.getString();
    }

    private GeoJsonUtils() {
    }

}
