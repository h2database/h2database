/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteOrder;

import org.h2.message.DbException;
import org.h2.store.DataHandler;

import com.vividsolutions.jts.geom.Envelope;
import org.h2.util.Utils;

/**
 *
 * @author Thomas Crevoisier
 * @author Jules Party
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public class ValueRaster extends ValueLob implements ValueSpatial {

    /**
     * Get or create a raster value for the given byte array.
     *
     * @param bytesNoCopy the byte array
     * @return the value
     */
    public static Value get(byte[] bytesNoCopy) {
        InputStream bytesStream = new ByteArrayInputStream(bytesNoCopy);
        long len = bytesNoCopy.length;
        return org.h2.value.ValueRaster.createGeoRaster(bytesStream, len, null);
    }


    /**
     * Create a raster from a value lob.
     * 
     * @param v the ValueLob containing the data of the raster
     */
    private ValueRaster(ValueLob v){
        super(v.type , v.handler, v.fileName, v.tableId, v.objectId, v.linked, v.precision, v.compressed);
        small = v.small;
        hash = v.hash;
    }
    
    /**
     * Create an empty Raster with given parameters.
     *
     * @param width the width of the Raster (in pixels)
     * @param height the height of the Raster (in pixels)
     *
     * @return an empty Raster of given dimension.
     */
    public static org.h2.value.ValueRaster createEmptyGeoRaster(double scaleX, double scaleY,
            double ipX, double ipY, double skewX, double skewY, long srid,
            int width, int height) {
        String hexaRast = "0000000000";
        hexaRast += Utils.doubleToHex(scaleX);
        hexaRast += Utils.doubleToHex(scaleY);
        hexaRast += Utils.doubleToHex(ipX);
        hexaRast += Utils.doubleToHex(ipY);
        hexaRast += Utils.doubleToHex(skewX);
        hexaRast += Utils.doubleToHex(skewY);
        hexaRast += Utils.uint32ToHex(srid);
        hexaRast += Utils.uint16ToHex(width);
        hexaRast += Utils.uint16ToHex(height);
        byte[] bytes = Utils.hexStringToByteArray(hexaRast);
        InputStream bytesStream = new ByteArrayInputStream(bytes);
        long len = bytes.length;
        return org.h2.value.ValueRaster.createGeoRaster(bytesStream, len, null);
    }

    /**
     * @return Raster metadata
     */
    public RasterMetaData getMetaData() {
        return RasterMetaData.fetchMetaData(getInputStream());
    }

    /**
     * Create a Raster from a given byte input stream
     * 
     * @param in the InputStream to build the Raster from
     * 
     * @return the ValueGeoRaster created
     */
    public static org.h2.value.ValueRaster createGeoRaster(InputStream in, long length, DataHandler handler){
        org.h2.value.ValueRaster raster = new org.h2.value.ValueRaster(ValueLob.createBlob(in, length, handler));
        return raster;
    }

    /**
     * Create an envelope based on the inputstream of the raster
     *
     * @return the envelope of the raster
     */
    @Override
    public Envelope getEnvelope(){
        InputStream input = getInputStream();
        RasterMetaData metaData = RasterMetaData.fetchMetaData(input);
        return metaData.getEnvelope();
    }

    @Override
    public int getType() {
        return Value.RASTER;
    }

    @Override
    public boolean intersectsBoundingBox(ValueSpatial vs) {
        return getEnvelope().intersects(vs.getEnvelope());
    }

    public static class RasterMetaData {
        public final int version;
        public final int numBands;
        public final double scaleX;
        public final double scaleY;
        public final double ipX;
        public final double ipY;
        public final double skewX;
        public final double skewY;
        public final long srid;
        public final int width;
        public final int height;

        public RasterMetaData(int version, int numBands, double scaleX, double scaleY, double ipX, double ipY,
                              double skewX, double skewY, long srid, int width, int height) {
            this.version = version;
            this.numBands = numBands;
            this.scaleX = scaleX;
            this.scaleY = scaleY;
            this.ipX = ipX;
            this.ipY = ipY;
            this.skewX = skewX;
            this.skewY = skewY;
            this.srid = srid;
            this.width = width;
            this.height = height;
        }

        public static RasterMetaData fetchMetaData(InputStream raster) {
            try {
                byte[] buffer = new byte[8];
                try {
                    // Retrieve the endian value
                    raster.read(buffer, 0, 1);
                    ByteOrder endian = buffer[0]==1 ? ByteOrder.LITTLE_ENDIAN  : ByteOrder.BIG_ENDIAN;

                    // Skip the bytes related to the version and the number of bands

                    raster.read(buffer, 0, 2);
                    int version = Utils.readUnsignedShort(buffer, 0, endian);
                    raster.read(buffer, 0, 2);
                    int numBands = Utils.readUnsignedShort(buffer, 0, endian);

                    // Retrieve scale values
                    raster.read(buffer, 0, 8);
                    double scaleX = Utils.readDouble(buffer, 0, endian);

                    raster.read(buffer, 0, 8);
                    double scaleY = Utils.readDouble(buffer, 0, endian);

                    // Retrieve ip values
                    raster.read(buffer, 0, 8);
                    double ipX = Utils.readDouble(buffer, 0, endian);

                    raster.read(buffer, 0, 8);
                    double ipY = Utils.readDouble(buffer, 0, endian);

                    // Retrieve skew values
                    raster.read(buffer, 0, 8);
                    double skewX = Utils.readDouble(buffer, 0, endian);

                    raster.read(buffer, 0, 8);
                    double skewY = Utils.readDouble(buffer, 0, endian);

                    // Retrieve the srid value
                    raster.read(buffer, 0, 4);
                    long srid = Utils.readInt(buffer, 0, endian);

                    // Retrieve width and height values
                    raster.read(buffer, 0, 2);
                    int width = Utils.readUnsignedShort(buffer, 0, endian);

                    raster.read(buffer, 0, 2);
                    int height =  Utils.readUnsignedShort(buffer, 0, endian);

                    return new RasterMetaData(version, numBands, scaleX, scaleY, ipX, ipY, skewX, skewY, srid, width, height);

                } finally {
                    raster.close();
                }
            } catch (IOException ex) {
                throw DbException.throwInternalError("H2 is unable to read the raster. " + ex.getMessage());
            }
        }

        public Envelope getEnvelope() {

            // Calculate the four points of the envelope and keep max and min values for x and y
            double xMax = ipX;
            double yMax = ipY;
            double xMin = ipX;
            double yMin = ipY;

            xMax = Math.max(xMax, ipX + width * scaleX);
            xMin = Math.min(xMin, ipX + width * scaleX);
            yMax = Math.max(yMax, ipY + width * scaleY);
            yMin = Math.min(yMin, ipY + width * scaleY);

            xMax = Math.max(xMax, ipX + height * skewX);
            xMin = Math.min(xMin, ipX + height * skewX);
            yMax = Math.max(yMax, ipY + height * skewY);
            yMin = Math.min(yMin, ipY + height * skewY);

            xMax = Math.max(xMax, ipX + width * scaleX + height * skewX);
            xMin = Math.min(xMin, ipX + width * scaleX + height * skewX);
            yMax = Math.max(yMax, ipY + width * scaleY + height * skewY);
            yMin = Math.min(yMin, ipY + width * scaleY + height * skewY);

            return new Envelope(xMax, xMin, yMax, yMin);
        }


        /**
         * Read the first bytes of the raster to get the pixels' type used in this band
         *
         * @return the pixels' type of this raster
         */
        public static String getPixelType(int pixelType) {
            // Retrieve pixel type
            switch (pixelType) {
                case 0:
                    return "1BB"; /* 1-bit boolean */
                case 1:
                    return "2BUI"; /* 2-bit unsigned integer*/
                case 2:
                    return "4BUI"; /* 4-bit unsigned integer */
                case 3:
                    return "8BSI"; /* 8-bit signed integer */
                case 4:
                    return "8BUI"; /* 8-bit unsigned integer */
                case 5:
                    return "16BSI"; /* 16-bit signed integer */
                case 6:
                    return "16BUI"; /* 16-bit unsigned integer */
                case 7:
                    return "32BSI"; /* 32-bit signed integer */
                case 8:
                    return "32BUI"; /* 32-bit unsigned integer */
                case 10:
                    return "32BF"; /* float */
                case 11:
                    return "64BF"; /* double */
                default:
                    return "Unknown";
            }
        }
    }
}
