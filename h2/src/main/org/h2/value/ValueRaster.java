/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.io.*;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

import com.vividsolutions.jts.geom.*;
import org.h2.message.DbException;
import org.h2.store.DataHandler;

import org.h2.util.Utils;
import org.h2.util.ValueImageInputStream;
import org.h2.util.WKBRasterWrapper;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

/**
 * @author Thomas Crevoisier
 * @author Jules Party
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public class ValueRaster extends ValueLob
        implements ValueSpatial, Value.ValueRasterMarker {
    private static final int RASTER_METADATA_SIZE = 61;
    public static final int LAST_WKB_VERSION = 0;
    private RasterMetaData cachedMetaData;

    /**
     * Get or create a raster value for the given byte array.
     *
     * @param bytesNoCopy the byte array
     * @return the value
     */
    public static ValueRaster get(byte[] bytesNoCopy) {
        InputStream bytesStream = new ByteArrayInputStream(bytesNoCopy);
        long len = bytesNoCopy.length;
        return createGeoRaster(bytesStream, len, null);
    }

    public static ValueRaster getFromImage(ValueLobDb value,double upperLeftX,
            double upperLeftY,double scaleX,double scaleY,double skewX,
            double skewY,int srid) throws IOException {
        ImageInputStream imageInputStream = new ValueImageInputStream(value);
        // Fetch ImageRead using ImageIO API
        Iterator<ImageReader> itReader = ImageIO.getImageReaders(
                imageInputStream);
        if(itReader != null && itReader.hasNext()) {
            ImageReader read = itReader.next();
            imageInputStream.seek(0);
            read.setInput(imageInputStream);
            Raster raster;
            if(read.canReadRaster()) {
                raster = read.readRaster(read.getMinIndex(), null);
            } else {
                // Not memory efficient as creating BufferedImage will(may?)
                // generate a copy of image data in memory
                raster = read.read(read.getMinIndex()).getRaster();
            }
            RasterBandMetaData[] bands = new RasterBandMetaData[raster
                    .getNumBands()];
            PixelType pixelType;
            switch (raster.getDataBuffer().getDataType()) {
                case DataBuffer.TYPE_BYTE:
                    pixelType = PixelType.PT_8BUI;
                    break;
                case DataBuffer.TYPE_SHORT:
                    pixelType = PixelType.PT_16BSI;
                    break;
                case DataBuffer.TYPE_USHORT:
                    pixelType = PixelType.PT_16BUI;
                    break;
                case DataBuffer.TYPE_INT:
                    pixelType = PixelType.PT_32BSI;
                    break;
                case DataBuffer.TYPE_FLOAT:
                    pixelType = PixelType.PT_32BF;
                    break;
                case DataBuffer.TYPE_DOUBLE:
                    pixelType = PixelType.PT_64BF;
                    break;
                default:
                    pixelType = PixelType.PT_32BSI;
            }
            for(int idBand = 0; idBand < bands.length; idBand++) {
                // TODO NODATA value
                bands[idBand] = new RasterBandMetaData(0, pixelType, true, 0);
            }
            RasterMetaData rasterMetaData = new RasterMetaData(ValueRaster
                    .LAST_WKB_VERSION, raster.getNumBands(), scaleX, scaleY,
                    upperLeftX, upperLeftY, skewX, skewY, srid, raster
                    .getWidth(), raster.getHeight(), bands);


            return ValueRaster.createGeoRaster(
                    new WKBRasterWrapper(raster, rasterMetaData)
                            .toWKBRasterStream(), -1, value.getDataHandler());
        }
        return null;
    }


    /**
     * Create a raster from a value lob.
     *
     * @param v the ValueLob containing the data of the raster
     */
    private ValueRaster(ValueLob v) {
        super(v.type, v.handler, v.fileName, v.tableId, v.objectId, v.linked,
                v.precision, v.compressed);
        small = v.small;
        hash = v.hash;
    }

    /**
     * Create an empty Raster with given parameters.
     *
     * @param width  the width of the Raster (in pixels)
     * @param height the height of the Raster (in pixels)
     * @return an empty Raster of given dimension.
     */
    public static org.h2.value.ValueRaster createEmptyGeoRaster(int numbands,
            double scaleX, double scaleY, double ipX, double ipY, double skewX,
            double skewY, int srid, int width, int height) {
        RasterMetaData rasterMetaData =
                new RasterMetaData(LAST_WKB_VERSION, numbands, scaleX, scaleY,
                        ipX, ipY, skewX, skewY, srid, width, height);
        ByteArrayOutputStream byteArrayOutputStream =
                new ByteArrayOutputStream(RASTER_METADATA_SIZE);
        try {
            rasterMetaData.writeRasterHeader(byteArrayOutputStream,
                    ByteOrder.BIG_ENDIAN);
            return createGeoRaster(new ByteArrayInputStream(
                            byteArrayOutputStream.toByteArray()),
                    byteArrayOutputStream.size(), null);
        } catch (IOException ex) {
            throw DbException.throwInternalError("H2 is unable to create the " +
                    "raster. " + ex.getMessage());
        }
    }

    /**
     * @return Raster metadata
     */
    @Override
    public RasterMetaData getMetaData() throws IOException {
        if (cachedMetaData == null) {
            cachedMetaData =
                    RasterMetaData.fetchMetaData(getInputStream(), true);
        }
        return cachedMetaData;
    }

    /**
     * Create a Raster from a given byte input stream
     *
     * @param in the InputStream to build the Raster from
     * @return the ValueGeoRaster created
     */
    public static org.h2.value.ValueRaster createGeoRaster(InputStream in,
            long length, DataHandler handler) {
        org.h2.value.ValueRaster raster = new org.h2.value.ValueRaster(
                ValueLob.createBlob(in, length, handler));
        return raster;
    }

    /**
     * Create an envelope based on the inputstream of the raster
     *
     * @return the envelope of the raster
     */
    @Override
    public Envelope getEnvelope() {
        InputStream input = getInputStream();
        try {
            RasterMetaData metaData = RasterMetaData.fetchMetaData(input);
            return metaData.getEnvelope();
        } catch (IOException ex) {
            throw DbException.throwInternalError("H2 is unable to read the " +
                    "raster. " + ex.getMessage());
        }
    }

    @Override
    public int getType() {
        return Value.RASTER;
    }

    @Override
    public boolean intersectsBoundingBox(ValueSpatial vs) {
        return getEnvelope().intersects(vs.getEnvelope());
    }

    /**
     * Raster band pixel type
     */
    public static enum PixelType {
        PT_1BB(0, 1),     /* 1-bit boolean            */
        PT_2BUI(1, 1),    /* 2-bit unsigned integer   */
        PT_4BUI(2, 1),    /* 4-bit unsigned integer   */
        PT_8BSI(3, 1),    /* 8-bit signed integer     */
        PT_8BUI(4, 1),    /* 8-bit unsigned integer   */
        PT_16BSI(5, Short.SIZE / Byte.SIZE),   /* 16-bit signed integer    */
        PT_16BUI(6, Short.SIZE / Byte.SIZE),   /* 16-bit unsigned integer  */
        PT_32BSI(7, Integer.SIZE / Byte.SIZE),   /* 32-bit signed integer    */
        PT_32BUI(8, Integer.SIZE / Byte.SIZE),   /* 32-bit unsigned integer  */
        PT_32BF(10, Integer.SIZE / Byte.SIZE),   /* 32-bit float             */
        PT_64BF(11, Double.SIZE / Byte.SIZE);   /* 64-bit float             */
        /**
         * Pixel type identifier
         */
        public final int value;

        /**
         * Pixel size in bytes.
         */
        public final int pixelSize;

        final static Map<Integer, PixelType> mapIntToEnum =
                new HashMap<Integer, PixelType>();

        static {
            for (PixelType pixelType : values()) {
                mapIntToEnum.put(pixelType.value, pixelType);
            }
        }

        private PixelType(int value, int pixelSize) {
            this.value = value;
            this.pixelSize = pixelSize;
        }

        /**
         * Create PixelType from pixel type identifier
         *
         * @param pixelTypeIdentifier Pixel type identifier from WKB Raster
         *                            Format
         * @return PixelType enum or null if it does not exists.
         */
        public static PixelType cast(int pixelTypeIdentifier) {
            return mapIntToEnum.get(pixelTypeIdentifier);
        }
    }

    public static class RasterBandMetaData {
        public static final int BANDTYPE_PIXTYPE_MASK = 0x0F;
        public static final int BANDTYPE_FLAG_OFFDB = (1 << 7);
        public static final int BANDTYPE_FLAG_HASNODATA = (1 << 6);

        public final PixelType pixelType;
        public final boolean hasNoData;
        public final boolean offDB; /* If True, then external band id and
        path are defined */
        public final double noDataValue;
        public final int externalBandId;
        public final String externalPath;
        public long offset;
        public long offsetPixel;

        public RasterBandMetaData(double noDataValue, PixelType pixelType,
                boolean hasNoData, long offset) {
            this.noDataValue = noDataValue;
            this.pixelType = pixelType;
            this.hasNoData = hasNoData;
            this.offDB = false;
            this.externalBandId = -1;
            this.externalPath = "";
            setOffset(offset);
        }

        public RasterBandMetaData(double noDataValue, PixelType pixelType,
                boolean hasNoData, int externalBandId, String externalPath,
                long offset) {
            this.pixelType = pixelType;
            this.hasNoData = hasNoData;
            this.noDataValue = noDataValue;
            this.externalBandId = externalBandId;
            this.externalPath = externalPath;
            this.offDB = true;
            setOffset(offset);
        }

        public void setOffset(long offset) {
            this.offset = offset;
            if(offDB) {
                this.offsetPixel = offset + 1 + pixelType.pixelSize + 1 +
                        externalPath.getBytes().length;
            } else {
                this.offsetPixel = offset + 1 + pixelType.pixelSize;
            }
        }
    }

    /**
     * Raster MetaData
     */
    public static class RasterMetaData {
        public final int version;
        public final int numBands;
        public final double scaleX;
        public final double scaleY;
        public final double ipX;
        public final double ipY;
        public final double skewX;
        public final double skewY;
        public final int srid;
        public final int width;
        public final int height;
        public final RasterBandMetaData[] bands;

        public RasterMetaData(int version, int numBands, double scaleX,
                double scaleY, double ipX, double ipY, double skewX,
                double skewY, int srid, int width, int height) {
            this(version, numBands, scaleX, scaleY, ipX, ipY, skewX, skewY,
                    srid, width, height, new RasterBandMetaData[0]);
        }

        public RasterMetaData(int version, int numBands, double scaleX,
                double scaleY, double ipX, double ipY, double skewX,
                double skewY, int srid, int width, int height,
                RasterBandMetaData[] bands) {
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
            this.bands = bands;
        }

        /**
         * Compute bytes to skip in InputStream in order to read the
         * specified band
         *
         * @param band Band id [0-numBands[
         * @return bytes to skip
         */
        public long getStreamOffset(int band) {
            return getStreamOffset(band, 0, 0);
        }

        /**
         * Compute bytes to skip in InputStream in order to read the
         * specified pixel.
         *
         * @param band Band id [0-numBands[
         * @return bytes to skip
         */
        public long getStreamOffset(int band, int x, int y) {
            if (band >= numBands) {
                throw new IllegalArgumentException("Band number out of range");
            }
            final RasterBandMetaData metaData = bands[band];
            return metaData.offsetPixel + metaData.pixelType.pixelSize *
                    width * y + x;
        }

        /**
         * Translate the pixel row,column into map coordinate
         *
         * @param x Column
         * @param y Row
         * @return Pixel world coordinate
         */
        public Coordinate getPixelCoordinate(int x, int y) {
            return new Coordinate(scaleX * x + skewX * y + ipX, scaleY * y +
                    skewY * x + ipY);
        }

        public static RasterMetaData fetchMetaData(InputStream raster)
                throws IOException {
            return fetchMetaData(raster, true);
        }

        public static RasterMetaData fetchMetaData(InputStream raster,
                boolean fetchBandsMetaData) throws IOException {
            try {
                byte[] buffer = new byte[RASTER_METADATA_SIZE];
                AtomicInteger cursor = new AtomicInteger(0);
                if (raster.read(buffer, 0, buffer.length) != buffer.length) {
                    throw DbException.throwInternalError(
                            "H2 is unable to " + "read the raster. ");
                }
                ByteOrder endian = buffer[cursor.getAndAdd(1)] == 1 ?
                        ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
                int version = Utils.readUnsignedShort(buffer,
                        cursor.getAndAdd(Short.SIZE / Byte.SIZE), endian);
                if (version > LAST_WKB_VERSION) {
                    throw DbException.throwInternalError("H2 is does not " +
                            "support raster version " + version + " raster.");
                }
                int numBands = Utils.readUnsignedShort(buffer,
                        cursor.getAndAdd(Short.SIZE / Byte.SIZE), endian);

                // Retrieve scale values
                double scaleX = Utils.readDouble(buffer,
                        cursor.getAndAdd(Double.SIZE / Byte.SIZE), endian);
                double scaleY = Utils.readDouble(buffer,
                        cursor.getAndAdd(Double.SIZE / Byte.SIZE), endian);

                // Retrieve insertion point values
                double ipX = Utils.readDouble(buffer,
                        cursor.getAndAdd(Double.SIZE / Byte.SIZE), endian);
                double ipY = Utils.readDouble(buffer,
                        cursor.getAndAdd(Double.SIZE / Byte.SIZE), endian);

                // Retrieve XY offset values
                double skewX = Utils.readDouble(buffer,
                        cursor.getAndAdd(Double.SIZE / Byte.SIZE), endian);
                double skewY = Utils.readDouble(buffer,
                        cursor.getAndAdd(Double.SIZE / Byte.SIZE), endian);

                // Retrieve the srid value
                int srid = Utils.readInt(buffer,
                        cursor.getAndAdd(Integer.SIZE / Byte.SIZE), endian);

                // Retrieve width and height values
                int width = Utils.readUnsignedShort(buffer,
                        cursor.getAndAdd(Short.SIZE / Byte.SIZE), endian);
                int height = Utils.readUnsignedShort(buffer,
                        cursor.getAndAdd(Short.SIZE / Byte.SIZE), endian);

                RasterBandMetaData[] bands = new RasterBandMetaData[numBands];
                int idBand = 0;
                if (numBands > 0 && fetchBandsMetaData) {
                    while (idBand < numBands) {
                        final int bandOffset = cursor.get();
                        // Read Flag
                        if (1 != raster.read(buffer, 0, 1)) {
                            throw DbException.throwInternalError("H2 is " +
                                    "unable to read the " + "raster's band. ");
                        }
                        cursor.addAndGet(1);
                        byte flag = buffer[0];
                        final PixelType pixelType = PixelType.cast(flag &
                                RasterBandMetaData.BANDTYPE_PIXTYPE_MASK);
                        if (pixelType == null) {
                            throw DbException.throwInternalError("H2 is " +
                                    "unable to read the " + "raster's band " +
                                    "pixel type. ");
                        }
                        final boolean hasNoData = (flag &
                                RasterBandMetaData.BANDTYPE_FLAG_HASNODATA) !=
                                0;
                        final boolean offDB = (flag &
                                RasterBandMetaData.BANDTYPE_FLAG_OFFDB) != 0;

                        // Read NODATA value
                        final double noData;
                        cursor.addAndGet(pixelType.pixelSize);
                        if (pixelType.pixelSize !=
                                raster.read(buffer, 0, pixelType.pixelSize)) {
                            throw DbException.throwInternalError("H2 is " +
                                    "unable to read the " + "raster's nodata." +
                                    " ");
                        }
                        cursor.getAndAdd(pixelType.pixelSize);
                        switch (pixelType) {
                            case PT_1BB:
                                noData = Utils.readUnsignedByte(buffer, 0) &
                                        0x01;
                                break;
                            case PT_2BUI:
                                noData = Utils.readUnsignedByte(buffer, 0) &
                                        0x03;
                                break;
                            case PT_4BUI:
                                noData = Utils.readUnsignedByte(buffer, 0) &
                                        0x0F;
                                break;
                            case PT_8BSI:
                                noData = buffer[0];
                                break;
                            case PT_8BUI:
                                noData = Utils.readUnsignedByte(buffer, 0);
                                break;
                            case PT_16BSI:
                                noData = Utils.readShort(buffer, 0, endian);
                                break;
                            case PT_16BUI:
                                noData = Utils.readUnsignedShort(buffer, 0,
                                        endian);
                                break;
                            case PT_32BSI:
                                noData = Utils.readInt(buffer, 0, endian);
                                break;
                            case PT_32BUI:
                                noData = Utils.readUnsignedInt32(buffer, 0,
                                        endian);
                                break;
                            case PT_32BF:
                                noData = Float.intBitsToFloat(
                                        Utils.readInt(buffer, 0, endian));
                                break;
                            case PT_64BF:
                                noData = Utils.readDouble(buffer, 0, endian);
                                break;
                            default:
                                throw DbException.throwInternalError("H2 is " +
                                        "unable to read the " + "raster's " +
                                        "nodata. ");
                        }
                        if (offDB) {
                            // Read external band id
                            if (1 != raster.read(buffer, 0, 1)) {
                                throw DbException.throwInternalError("H2 is " +
                                        "unable to read the " + "raster's " +
                                        "band. ");
                            }
                            int bandId = buffer[0];
                            // read path
                            Scanner scanner = new Scanner(raster);
                            String path = scanner.next();
                            cursor.addAndGet(1 + path.getBytes().length);
                            RasterBandMetaData rasterBandMetaData =
                                    new RasterBandMetaData(noData, pixelType,
                                            hasNoData, bandId, path,
                                            bandOffset);
                            bands[idBand++] = rasterBandMetaData;
                        } else {
                            RasterBandMetaData rasterBandMetaData =
                                    new RasterBandMetaData(noData, pixelType,
                                            hasNoData, bandOffset);
                            bands[idBand++] = rasterBandMetaData;
                            // Skip remaining pixel until next band
                            int skipLength =
                                    width * height * pixelType.pixelSize;
                            cursor.addAndGet(skipLength);
                            if (skipLength != raster.skip(skipLength)) {
                                throw DbException.throwInternalError("H2 is " +
                                        "unable to read the " + "raster's " +
                                        "band. ");
                            }
                        }
                    }
                }
                return new RasterMetaData(version, numBands, scaleX, scaleY,
                        ipX, ipY, skewX, skewY, srid, width, height, bands);
            } finally {
                raster.close();
            }
        }

        public void writeRasterHeader(OutputStream stream, ByteOrder endian)
                throws IOException {
            byte[] buffer = new byte[RASTER_METADATA_SIZE];
            AtomicInteger cursor = new AtomicInteger(0);

            // Write byte order
            buffer[cursor.getAndAdd(1)] =
                    (byte) (endian == ByteOrder.LITTLE_ENDIAN ? 1 : 0);

            Utils.writeUnsignedShort(buffer,
                    cursor.getAndAdd(Short.SIZE / Byte.SIZE), version, endian);
            Utils.writeUnsignedShort(buffer,
                    cursor.getAndAdd(Short.SIZE / Byte.SIZE), numBands, endian);

            // Write scale values
            Utils.writeDouble(buffer, cursor.getAndAdd(Double.SIZE / Byte.SIZE),
                    scaleX, endian);
            Utils.writeDouble(buffer, cursor.getAndAdd(Double.SIZE / Byte.SIZE),
                    scaleY, endian);

            // Write insertion point values
            Utils.writeDouble(buffer, cursor.getAndAdd(Double.SIZE / Byte.SIZE),
                    ipX, endian);
            Utils.writeDouble(buffer, cursor.getAndAdd(Double.SIZE / Byte.SIZE),
                    ipY, endian);

            // Write XY offset values
            Utils.writeDouble(buffer, cursor.getAndAdd(Double.SIZE / Byte.SIZE),
                    skewX, endian);
            Utils.writeDouble(buffer, cursor.getAndAdd(Double.SIZE / Byte.SIZE),
                    skewY, endian);

            // Write the srid value
            Utils.writeInt(buffer, cursor.getAndAdd(Integer.SIZE / Byte.SIZE),
                    srid, endian);

            // Retrieve width and height values
            Utils.writeUnsignedShort(buffer,
                    cursor.getAndAdd(Short.SIZE / Byte.SIZE), width, endian);
            Utils.writeUnsignedShort(buffer,
                    cursor.getAndAdd(Short.SIZE / Byte.SIZE), height, endian);

            stream.write(buffer);
        }


        public void writeRasterBandHeader(OutputStream stream,int band,
                ByteOrder endian)
        throws IOException {
            RasterBandMetaData bandMetaData = bands[band];
            // Write flag
            stream.write(bandMetaData.pixelType.value |
                    (bandMetaData.offDB ?
                            RasterBandMetaData.BANDTYPE_FLAG_OFFDB : 0) |
                    ((bandMetaData.hasNoData ?
                            RasterBandMetaData.BANDTYPE_FLAG_HASNODATA : 0)));
            // Write novalue
            byte[] buffer = new byte[8];
            switch (bandMetaData.pixelType) {
                case PT_1BB:
                    buffer[0] = (byte)(bandMetaData.noDataValue != 0 ? 1 : 0);
                    break;
                case PT_2BUI:
                case PT_4BUI:
                case PT_8BUI:
                    buffer[0] = (byte)((int)bandMetaData.noDataValue & 0xFF);
                    break;
                case PT_8BSI:
                    buffer[0] = (byte)((int)bandMetaData.noDataValue);
                case PT_16BSI:
                    Utils.writeUnsignedShort(buffer, 0,
                            (int) bandMetaData.noDataValue, endian);
                case PT_16BUI:
                    Utils.writeUnsignedShort(buffer, 0, (int) bandMetaData.noDataValue,
                            endian);
                    break;
                case PT_32BSI:
                    Utils.writeInt(buffer, 0, (int)bandMetaData.noDataValue,
                            endian);
                    break;
                case PT_32BUI:
                    Utils.writeUnsignedInt(buffer, 0,
                            (long) (bandMetaData.noDataValue), endian);
                    break;
                case PT_32BF:
                    Utils.writeInt(buffer, 0, Float.floatToIntBits(
                            (float)bandMetaData.noDataValue), endian);
                    break;
                case PT_64BF:
                    Utils.writeDouble(buffer, 0, bandMetaData.noDataValue,
                            endian);
                    break;
                default:
                    throw DbException.throwInternalError("H2 is " +
                            "unable to read the " + "raster's " +
                            "nodata. ");
            }
            stream.write(buffer, 0, bandMetaData.pixelType.pixelSize);
            if(bandMetaData.offDB) {
                stream.write((byte)bandMetaData.externalBandId);
                if(bandMetaData.externalPath.endsWith("\0")) {
                    stream.write(bandMetaData.externalPath.getBytes());
                } else {
                    // must be a null terminated string
                    stream.write((bandMetaData.externalPath + "\0").getBytes());
                }
            }
        }

        /**
         * @return The envelope of the raster, take account of the rotation
         * of the raster
         */
        public Polygon convexHull() {
            GeometryFactory geometryFactory =
                    new GeometryFactory(new PrecisionModel(), srid);
            Coordinate bottomLeft = getPixelCoordinate(0, 0);
            Coordinate bottomRight = getPixelCoordinate(width, 0);
            Coordinate topRight = getPixelCoordinate(width, height);
            Coordinate topLeft = getPixelCoordinate(0, height);
            return geometryFactory.createPolygon(
                    new Coordinate[]{bottomLeft, bottomRight, topRight, topLeft,
                            bottomLeft});
        }

        /**
         * @return The envelope of the raster. This envelope is larger than
         * the convex hull as
         */
        public Envelope getEnvelope() {
            Envelope env = new Envelope(getPixelCoordinate(0, 0));
            env.expandToInclude(getPixelCoordinate(width, 0));
            env.expandToInclude(getPixelCoordinate(width, height));
            env.expandToInclude(getPixelCoordinate(0, height));
            return env;
        }
    }
}
