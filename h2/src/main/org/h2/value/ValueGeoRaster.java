/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import com.vividsolutions.jts.io.ByteOrderValues;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.h2.message.DbException;
import org.h2.store.DataHandler;

import com.vividsolutions.jts.geom.Envelope;

/**
 *
 * @author Thomas Crevoisier, Jules Party
 */
public class ValueGeoRaster extends ValueLob implements ValueSpatial {

    /**
     * Get or create a georaster value for the given byte array.
     *
     * @param bytesNoCopy the byte array
     * @return the value
     */
    public static Value get(byte[] bytesNoCopy) {
        InputStream bytesStream = new ByteArrayInputStream(bytesNoCopy);
        long len = bytesNoCopy.length;
        return ValueGeoRaster.createGeoRaster(bytesStream, len, null);
    }


    /**
     * Create a GeoRaster from a value lob.
     * 
     * @param v the ValueLob containing the data of the raster
     */
    private ValueGeoRaster (ValueLob v){
        super(v.type , v.handler, v.fileName, v.tableId, v.objectId, v.linked, v.precision, v.compressed);
        small = v.small;
        hash = v.hash;
    }
    
    /**
     * Create an empty GeoRaster with given parameters.
     *
     * @param width the width of the GeoRaster (in pixels)
     * @param height the height of the GeoRaster (in pixels)
     *
     * @return an empty GeoRaster of given dimension.
     */
    public static ValueGeoRaster createEmptyGeoRaster(double scaleX, double scaleY,
            double ipX, double ipY, double skewX, double skewY, long srid,
            int width, int height) {
        String hexaRast = "0000000000";
        hexaRast += doubleToHex(scaleX);
        hexaRast += doubleToHex(scaleY);
        hexaRast += doubleToHex(ipX);
        hexaRast += doubleToHex(ipY);
        hexaRast += doubleToHex(skewX);
        hexaRast += doubleToHex(skewY);
        hexaRast += uint32ToHex(srid);
        hexaRast += uint16ToHex(width);
        hexaRast += uint16ToHex(height);
        byte[] bytes = hexStringToByteArray(hexaRast);
        InputStream bytesStream = new ByteArrayInputStream(bytes);
        long len = bytes.length;
        return ValueGeoRaster.createGeoRaster(bytesStream, len, null);
    }

    /**
     * Create a GeoRaster from a given byte input stream
     * 
     * @param in the InputStream to build the GeoRaster from
     * 
     * @return the ValueGeoRaster created
     */
    public static ValueGeoRaster createGeoRaster(InputStream in, long length, DataHandler handler){
        ValueGeoRaster geoRaster = new ValueGeoRaster(ValueLob.createBlob(in, length, handler));
        return geoRaster;
    }

    /**
     * Read the first bytes of the raster to get its number of bands.
     * 
     * @return the number of bands of this raster
     */
    public double getNumBands() {
        int numBands = 0;
        InputStream input = getInputStream();
        byte[] buffer = new byte[8];

        try {
            // Retrieve the endian value
            input.read(buffer, 0, 1);
            int endian = buffer[0]==1 ? ByteOrderValues.LITTLE_ENDIAN  : ByteOrderValues.BIG_ENDIAN;

            // Skip byte to numBands
            input.skip(4);

            // Retrieve numBands
            input.read(buffer, 0, 8);
            numBands = getUnsignedInt16(buffer, endian);
        } catch (IOException ex) {
            throw DbException.throwInternalError("H2 is unable to read the raster. " + ex.getMessage());
        }
        return numBands;
    }

    /**
     * Read the first bytes of the raster to get its scale along the X axe.
     * 
     * @return the scale of the georeferenced raster along the X axe
     */
    public double getScaleX() {
        double scaleX = 0;
        InputStream input = getInputStream();
        byte[] buffer = new byte[8];

        try {
            // Retrieve the endian value
            input.read(buffer, 0, 1);
            int endian = buffer[0]==1 ? ByteOrderValues.LITTLE_ENDIAN  : ByteOrderValues.BIG_ENDIAN;

            // Skip byte to scaleX
            input.skip(4);

            // Retrieve scaleX
            input.read(buffer, 0, 8);
            scaleX = ByteOrderValues.getDouble(buffer, endian);
        } catch (IOException ex) {
            throw DbException.throwInternalError("H2 is unable to read the raster. " + ex.getMessage());
        }
        return scaleX;
    }

    /**
     * Read the first bytes of the raster to get its scale along the Y axe.
     * 
     * @return the scale of the georeferenced raster along the Y axe
     */
    public double getScaleY() {
        double scaleY = 0;
        InputStream input = getInputStream();
        byte[] buffer = new byte[8];

        try {
            // Retrieve the endian value
            input.read(buffer, 0, 1);
            int endian = buffer[0]==1 ? ByteOrderValues.LITTLE_ENDIAN  : ByteOrderValues.BIG_ENDIAN;

            // Skip byte to scaleY
            input.skip(12);

            // Retrieve scaleY
            input.read(buffer, 0, 8);
            scaleY = ByteOrderValues.getDouble(buffer, endian);
        } catch (IOException ex) {
            throw DbException.throwInternalError("H2 is unable to read the raster. " + ex.getMessage());
        }
        return scaleY;
    }

    /**
     * Read the first bytes of the raster to get its SRID.
     * 
     * @return the srid of the georeferenced raster
     */
    public long getSRID() {
        long srid = 0;
        InputStream input = getInputStream();
        byte[] buffer = new byte[4];

        try {
            // Retrieve the endian value
            input.read(buffer, 0, 1);
            int endian = buffer[0]==1 ? ByteOrderValues.LITTLE_ENDIAN  : ByteOrderValues.BIG_ENDIAN;

            // Skip byte to the srid
            input.skip(52);

            // Retrieve the srid value
            input.read(buffer, 0, 4);
            srid = getUnsignedInt32(buffer, endian);
        } catch (IOException ex) {
            throw DbException.throwInternalError("H2 is unable to read the raster. " + ex.getMessage());
        }
        return srid;
    }

    /**
     * Read the first bytes of the raster to get the width of this raster.
     * 
     * @return the width of this raster
     */
    public int getWidth() {
        int width = 0;
        InputStream input = getInputStream();
        byte[] buffer = new byte[2];

        try {
            // Retrieve the endian value
            input.read(buffer, 0, 1);
            int endian = buffer[0]==1 ? ByteOrderValues.LITTLE_ENDIAN  : ByteOrderValues.BIG_ENDIAN;

            // Skip byte to width
            input.skip(56);

            // Retrieve width
            input.read(buffer, 0, 2);
            width = getUnsignedInt16(buffer, endian);
        } catch (IOException ex) {
            throw DbException.throwInternalError("H2 is unable to read the raster. " + ex.getMessage());
        }
        return width;
    }

    /**
     * Read the first bytes of the raster to get the height of this raster.
     * 
     * @return the height of this raster
     */
    public int getHeight() {
        int height = 0;
        InputStream input = getInputStream();
        byte[] buffer = new byte[2];

        try {
            // Retrieve the endian value
            input.read(buffer, 0, 1);
            int endian = buffer[0]==1 ? ByteOrderValues.LITTLE_ENDIAN  : ByteOrderValues.BIG_ENDIAN;

            // Skip byte to height
            input.skip(58);

            // Retrieve height
            input.read(buffer, 0, 2);
            height = getUnsignedInt16(buffer, endian);
        } catch (IOException ex) {
            throw DbException.throwInternalError("H2 is unable to read the raster. " + ex.getMessage());
        }
        return height;
    }

    /**
     * Read the first bytes of the raster to get the pixels' type used in this
     * raster.
     *
     * @return the pixels' type of this raster
     */
    public String getPixelType() {
        InputStream input = getInputStream();
        byte[] buffer = new byte[2];

        try {
            // Retrieve the endian value
            input.read(buffer, 0, 1);
            int endian = buffer[0]==1 ? ByteOrderValues.LITTLE_ENDIAN  : ByteOrderValues.BIG_ENDIAN;

            // Skip byte to the pixel type
            input.skip(60);

            // Retrieve pixel type
            input.read(buffer, 0, 1);
            int pixType = (buffer[0] & 0x0f);
            switch (pixType) {
                case 0:
                    return "1BB";
                case 1:
                    return "2BUI";
                case 2:
                    return "4BUI";
                case 3:
                    return "8BSI";
                case 4:
                    return "8BUI";
                case 5:
                    return "16BSI";
                case 6:
                    return "16BUI";
                case 7:
                    return "32BSI";
                case 8:
                    return "32BUI";
                case 10:
                    return "32BF";
                case 11:
                    return "64BF";
                default:
                    return "Unknown";
            }
        } catch (IOException ex) {
            throw DbException.throwInternalError("H2 is unable to read the raster. " + ex.getMessage());
        }
    }

    /**
     * Create an envelope based on the inputstream of the georaster
     *
     * @return the envelope of the georaster
     */
    @Override
    public Envelope getEnvelope(){
        InputStream input = getInputStream();

        try {
            byte[] buffer = new byte[8];

            // Retrieve the endian value
            input.read(buffer, 0, 1);
            int endian = buffer[0]==1 ? ByteOrderValues.LITTLE_ENDIAN  : ByteOrderValues.BIG_ENDIAN;
            
            // Skip the bytes related to the version and the number of bands
            input.skip(4);
            
            // Retrieve scale values
            input.read(buffer,0,8);
            double scaleX = ByteOrderValues.getDouble(buffer, endian);
            
            input.read(buffer,0,8);
            double scaleY = ByteOrderValues.getDouble(buffer, endian);
            
            // Retrieve ip values
            input.read(buffer,0,8);
            double ipX = ByteOrderValues.getDouble(buffer, endian);
            
            input.read(buffer,0,8);
            double ipY = ByteOrderValues.getDouble(buffer, endian);
            
            // Retrieve skew values
            input.read(buffer,0,8);
            double skewX = ByteOrderValues.getDouble(buffer, endian);
            
            input.read(buffer,0,8);
            double skewY = ByteOrderValues.getDouble(buffer, endian);
            
            // Retrieve the srid value
            input.read(buffer,0,4);
            long srid = getUnsignedInt32(buffer, endian);
            
            // Retrieve width and height values
            input.read(buffer,0,2);
            short width = getShort(buffer, endian);
            
            input.read(buffer,0,2);
            short height = getShort(buffer, endian);

            // Calculate the four points of the envelope and keep max and min values for x and y
            double xMax = ipX;
            double yMax = ipY;
            double xMin = ipX;
            double yMin = ipY;

            xMax = Math.max(xMax,ipX + width*scaleX);
            xMin = Math.min(xMin,ipX + width*scaleX);
            yMax = Math.max(yMax,ipY + width*scaleY);
            yMin = Math.min(yMin,ipY + width*scaleY);

            xMax = Math.max(xMax,ipX + height*skewX);
            xMin = Math.min(xMin,ipX + height*skewX);
            yMax = Math.max(yMax,ipY + height*skewY);
            yMin = Math.min(yMin,ipY + height*skewY);

            xMax = Math.max(xMax,ipX + width*scaleX + height*skewX);
            xMin = Math.min(xMin,ipX + width*scaleX + height*skewX);
            yMax = Math.max(yMax,ipY + width*scaleY + height*skewY);
            yMin = Math.min(yMin,ipY + width*scaleY + height*skewY);

            return new Envelope(xMax, xMin, yMax, yMin);

        } catch (IOException ex) {
            throw DbException.throwInternalError("H2 is unable to read the raster. " + ex.getMessage());
        }
    }

    /**
     * Convert an given array of bytes into a short int by precising the value of endian
     * 
     * @param buff the array of bytes to convert
     * @param endian 2 for little endian and 1 for big endian
     * 
     * @return short the result of the conversion
     */
    public static short getShort(byte[] buff, int endian){
        if(endian==1){
            return (short) (((buff[0] & 0xff) << 8)|((buff[1] & 0xff)));
        }else{
            return (short) (((buff[1] & 0xff) << 8)|((buff[0] & 0xff)));
        }
    }

    /**
     * Convert an given array of bytes into a unsigned int 32 bits (represented
     * by a long in Java) by precising the value of endian
     *
     * @param buff the array of bytes to convert
     * @param endian 2 for little endian and 1 for big endian
     *
     * @return long the result of the conversion
     */
    public static long getUnsignedInt32(byte[] buff, int endian) {
        long result = ByteOrderValues.getInt(buff, endian);
        if (result < 0) {
            result = result + 2*(Integer.MAX_VALUE+1);
        }
        return result;
    }

    /**
     * Convert an given array of bytes into a unsigned int 16 bits (represented
     * by an int in Java) by precising the value of endian
     *
     * @param buff the array of bytes to convert
     * @param endian 2 for little endian and 1 for big endian
     *
     * @return long the result of the conversion
     */
    private static int getUnsignedInt16(byte[] buff, int endian) {
        int result = getShort(buff, endian);
        if (result < 0) {
            result = result + 2*(Short.MAX_VALUE+1);
        }
        return result;
    }

    /**
     * Convert a double into its hexadecimal representation (with big endian
     * convention).
     *
     * @param value the double to convert
     * @return hexadecimal String representation of the double, with a fix length of 16
     */
    private static String doubleToHex(double value) {
        long valueAsLong = Double.doubleToRawLongBits(value);
        String hexaValue = Long.toHexString(valueAsLong);
        while (hexaValue.length() < 16) {
            hexaValue = "0" + hexaValue;
        }
        return hexaValue;
    }

    /**
     * Convert a long reprensenting an unsigned integer 32 bits into its
     * hexadecimal representation (with big endian convention).
     *
     * @param value the long (uint32) to convert
     * @return hexadecimal String representation of the uint32, with a fix
     * length of 8
     */
    private static String uint32ToHex(long value) {
        if (value < 0 || value > 2 * ((long) Integer.MAX_VALUE) + 1) {
            throw DbException.throwInternalError("Error in argument : " + value + " is not a valid unsigned integer 32 bits. It should be include between 0 and " + 2 * Integer.MAX_VALUE + 1 + ".");
        }
        String hexaValue = Long.toHexString(value);
        while (hexaValue.length() < 8) {
            hexaValue = "0" + hexaValue;
        }
        return hexaValue;
    }

    /**
     * Convert an int reprensenting an unsigned integer 16 bits into its
     * hexadecimal representation (with big endian convention).
     *
     * @param value the long (uint32) to convert
     * @return hexadecimal String representation of the uint32, with a fix
     * length of 4
     */
    private static String uint16ToHex(int value) {
        if (value < 0 || value > 2 * ((int) Short.MAX_VALUE) + 1) {
            throw DbException.throwInternalError("Error in argument : " + value + " is not a valid unsigned integer 16 bits value. It should be include between 0 and " + 2*Short.MAX_VALUE + 1 + ".");
        }
        String hexaValue = Long.toHexString(value);
        while (hexaValue.length() < 4) {
            hexaValue = "0" + hexaValue;
        }
        return hexaValue;
    }

    /**
     * Convert a haxadecimal String into the equivalent byte array.
     *
     * @param s the string to tansform
     * @return the equivalent byte array
     */
    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        if (len % 2 != 0) {
            throw DbException.throwInternalError("The length of an hexadecimal string must be an even number.");
        }
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                                + Character.digit(s.charAt(i+1), 16));
        }
        return data;
    }

    @Override
    public int getType() {
        return Value.GEORASTER;
    }

    @Override
    public boolean intersectsBoundingBox(ValueSpatial vs) {
        return getEnvelope().intersects(vs.getEnvelope());
    }
}
