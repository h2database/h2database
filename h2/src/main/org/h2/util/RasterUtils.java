/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FilePathDisk;
import org.h2.util.imageio.RenderedImageReader;
import org.h2.util.imageio.WKBRasterReader;
import org.h2.util.imageio.WKBRasterReaderSpi;
import org.h2.value.Value;
import org.h2.value.ValueLobDb;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.ImageOutputStream;
import java.awt.geom.AffineTransform;
import java.awt.geom.NoninvertibleTransformException;
import java.awt.geom.Point2D;
import java.awt.image.RenderedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Utility on Raster format
 * @author Nicolas Fortin, CNRS
 * @author Erwan Bocher, CNRS
 */
public class RasterUtils {
    public static final int RASTER_METADATA_SIZE = 61;
    public static final int LAST_WKB_VERSION = 0;

    /**
     * Utility class
     */
    private RasterUtils() {
    }

    /**
     * Create an empty raster without bands
     * @param metaSource Copy metadata from this source (does not read band
     *                   count)
     * @return Raster value.
     * @throws IOException An error occured while creating data.
     */
    public static Value makeEmptyRaster(RasterMetaData metaSource) throws IOException {
        RasterMetaData meta =
                new RasterMetaData(metaSource.width, metaSource.height,
                        metaSource.srid,
                        metaSource.scaleX, metaSource.scaleY, metaSource.ipX,
                        metaSource.ipY, metaSource.skewX, metaSource.skewY);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        meta.writeRasterHeader(out, ByteOrder.BIG_ENDIAN);
        return ValueLobDb.createSmallLob(Value.RASTER, out.toByteArray());
    }

    /**
     * Convert a raster into an Image using ImageIO ImageWriter.
     * @param value Raster
     * @param suffix Image suffix ex:png
     * @param session of the current connection
     * @return Image stream
     * @throws IOException
     */
    public static Value asImage(Value value, String suffix,
            Session session) throws
            IOException {
        Iterator<ImageWriter> itWriter = ImageIO.getImageWritersBySuffix
                (suffix);
        if(itWriter != null && itWriter.hasNext()) {
            ImageWriter imageWriter = itWriter.next();
            ImageReader wkbReader = new WKBRasterReader(new WKBRasterReaderSpi());
            wkbReader.setInput(new ImageInputStreamWrapper(
                    new ImageInputStreamWrapper.ValueStreamProvider(value),
                    session));
            // Create pipe for streaming data between two thread
            final PipedInputStream in = new PipedInputStream();
            final Task task = new TransferStreamTask(session, in);
            TaskPipedOutputStream outputStream = new TaskPipedOutputStream
                    (in, task, session);
            // Init ImageIO cache directory it does not exists
            File cacheDir = ImageIO.getCacheDirectory();
            if(cacheDir == null || !cacheDir.exists()) {
                FilePath cacheFile = FilePathDisk.get("imageio").createTempFile
                        ("imageio", true, true);
                ImageIO.setCacheDirectory(new File(cacheFile.getParent().toString()));
            }
            ImageOutputStream imageOutputStream = ImageIO
                    .createImageOutputStream(outputStream);
            try {
                imageWriter
                        .setOutput(imageOutputStream);
                task.execute();
                if (imageWriter.canWriteRasters()) {
                    imageWriter.write(new IIOImage(
                            wkbReader.readRaster(wkbReader.getMinIndex(), null),
                            null, null));
                } else {
                    imageWriter.write(new IIOImage(wkbReader.read(wkbReader.getMinIndex()),
                            null, null));
                }
            } finally {
                outputStream.close();
            }
            return (Value)task.get();
        }
        throw DbException.throwInternalError("Unsupported format: "+suffix);
    }

    private static class TaskPipedOutputStream extends PipedOutputStream {
        private Task task;
        private Session session;
        private long lastPositionChecked = 0;
        private long position = 0;
        private static final long CHECK_CANCEL_POSITION_SHIFT = 100000;

        public TaskPipedOutputStream(PipedInputStream snk, Task task, Session
                session)
                throws IOException {
            super(snk);
            this.session = session;
            this.task = task;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            super.write(b, off, len);
            position += len;
            // Check for cancel
            if(session != null && lastPositionChecked +
                    CHECK_CANCEL_POSITION_SHIFT < position) {
                session.checkCanceled();
                lastPositionChecked = position;
            }
        }

        @Override
        public void close() throws IOException {
            super.close();
            try {
                task.get();
            } catch (Exception e) {
                throw DbException.convertToIOException(e);
            }
        }
    }

    private static class TransferStreamTask extends Task {
        private Session session;
        private InputStream inputStream;

        public TransferStreamTask(Session session, InputStream inputStream) {
            this.session = session;
            this.inputStream = inputStream;
        }

        @Override
        public void call() throws Exception {
            try {
                if(session != null) {
                        Value v = session.getDataHandler().getLobStorage()
                                .createBlob(inputStream, -1);
                        session.addTemporaryLob(v);
                        result = v;
                } else {
                    result = ValueLobDb.createSmallLob(Value.BLOB, IOUtils
                            .readBytesAndClose(inputStream, -1));
                }
            } finally {
                inputStream.close();
            }
        }
    }

    /**
     * Convert an Image stream into a Raster
     * @param value Image stream
     * @param scaleX Pixel width in geographical units
     * @param scaleY Pixel height in geographical units     * 
     * @param ipX X ordinate of upper-left  pixel's upper-left corner in geographical units
     * @param ipY Y ordinate of upper-left pixel's upper-left corner in geographical units
     * @param skewX Rotation about X-axis
     * @param skewY Rotation about Y-axis
     * @param srid Spatial reference identifier
     * @param session Session or null
     * @return The raster value
     * @throws IOException
     */
    public static Value getFromImage(Value value, double scaleX, double scaleY,double ipX,
            double ipY,  double skewX,
            double skewY, int srid, Session session)
            throws IOException {
        ImageInputStream imageInputStream = new ImageInputStreamWrapper(
                new ImageInputStreamWrapper.ValueStreamProvider(value),
                session);
        // Fetch ImageRead using ImageIO API
        Iterator<ImageReader> itReader =
                ImageIO.getImageReaders(imageInputStream);
        if (itReader != null && itReader.hasNext()) {
            ImageReader read = itReader.next();
            imageInputStream.seek(0);
            read.setInput(imageInputStream);
            RenderedImage image = new RenderedImageReader(read);
            if (session != null) {
                return session.getDataHandler().getLobStorage().createRaster
                        (GeoRasterRenderedImage
                                .create(image, scaleX, scaleY, ipX,
                                        ipY, skewX, skewY, srid,
                                        Double.NaN).asWKBRaster(), -1);
            } else {
                return ValueLobDb.createSmallLob(Value.RASTER,
                        IOUtils.readBytesAndClose(GeoRasterRenderedImage
                                .create(image, scaleX, scaleY, ipX,
                                        ipY, skewX, skewY, srid,
                                        Double.NaN).asWKBRaster(), -1));
            }
        }
        throw DbException.throwInternalError("Unsupported image format");
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
         * @return Pixel type string representation
         */
        public String getPixeTypeName() {
            return toString().substring(3);
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

    /**
     * Metadata of a band
     */
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

        /**
         * @param noDataValue Pixel value that represent nodata
         * @param pixelType Pixel type
         * @param hasNodataValue If true, stored nodata value is a true nodata value. 
         * Otherwise the value stored as a nodata value should be ignored.
         * @param offset Position of the first byte of this band
         */
        public RasterBandMetaData(double noDataValue, PixelType pixelType,
                boolean hasNodataValue, long offset) {
            this.noDataValue = noDataValue;
            this.pixelType = pixelType;
            this.hasNoData = hasNodataValue;
            this.offDB = false;
            this.externalBandId = -1;
            this.externalPath = "";
            setOffset(offset);
        }

        /**
         * 
         * @param noDataValue Pixel value that represent nodata
         * @param pixelType Pixel type
         * @param hasNodataValue If true, stored nodata value is a true nodata value. 
         * Otherwise the value stored as a nodata value should be ignored.
         * @param externalBandId band id to use from the set available 
         * in the external file 
         * @param externalPath isOffline flag set, null-terminated path to data file         * 
         * @param offset Position of the first byte of this band
         */
        public RasterBandMetaData(double noDataValue, PixelType pixelType,
                boolean hasNodataValue, int externalBandId, String externalPath,
                long offset) {
            this.pixelType = pixelType;
            this.hasNoData = hasNodataValue;
            this.noDataValue = noDataValue;
            this.externalBandId = externalBandId;
            this.externalPath = externalPath;
            this.offDB = true;
            setOffset(offset);
        }

        /** Return the bytes length of the band
         * @param imageWith Number of pixel columns
         * @param imageHeight Number of pixel rows
         * @return Band Bytes length
         */
        public long getLength(int imageWith, int imageHeight) {
            if(offDB) {
                return 1 + pixelType.pixelSize + 1 +
                        externalPath.getBytes().length;
            } else {
                return 1 + pixelType.pixelSize + imageWith *
                        imageHeight * pixelType.pixelSize;
            }
        }

        /**
         * Set the position of the first byte of this band
         * @param offset 
         */
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
        public final ByteOrder endian;
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

    
        /**
         * Build a RasterMetaData
         * 
         * @param width Number of pixel columns
         * @param height Number of pixel rows         * 
         * @param srid Spatial reference identifier 
         * @param scaleX Pixel width in geographical units 
         * @param scaleY Pixel height in geographical units
         * @param ipX X ordinate of upper-left  pixel's upper-left corner in geographical units
         * @param ipY Y ordinate of upper-left pixel's upper-left corner in geographical units
         * @param skewX Rotation about X-axis
         * @param skewY Rotation about Y-axis
         */
        public RasterMetaData(int width, int height,  int srid, double scaleX,
                double scaleY, double ipX, double ipY, double skewX,
                double skewY) {
            this(LAST_WKB_VERSION, 0, width,  height,srid, scaleX, scaleY, ipX, ipY, skewX, skewY,
                     new RasterBandMetaData[0]);
        }

        /**
         * Build a RasterMetaData
         * 
         * @param version Format version (0 for this structure)       * 
         * @param width Number of pixel columns
         * @param height Number of pixel rows 
         * @param numBands Number of bands         * 
         * @param srid Spatial reference identifier 
         * @param scaleX Pixel width in geographical units 
         * @param scaleY Pixel height in geographical units
         * @param ipX X ordinate of upper-left  pixel's upper-left corner in geographical units
         * @param ipY Y ordinate of upper-left pixel's upper-left corner in geographical units
         * @param skewX Rotation about X-axis
         * @param skewY Rotation about Y-axis
         */
        public RasterMetaData(int version, int numBands, int width, int height, int srid,double scaleX,
                double scaleY, double ipX, double ipY, double skewX,
                double skewY) {
            this(version, numBands, width, height, srid,scaleX, scaleY, ipX, ipY, skewX, skewY,
                     new RasterBandMetaData[0]);
        }

        /**
         * Build a RasterMetaData
         * 
         * @param version Format version (0 for this structure)       * 
         * @param width Number of pixel columns
         * @param height Number of pixel rows 
         * @param numBands Number of bands         * 
         * @param srid Spatial reference identifier 
         * @param scaleX Pixel width in geographical units 
         * @param scaleY Pixel height in geographical units
         * @param ipX X ordinate of upper-left  pixel's upper-left corner in geographical units
         * @param ipY Y ordinate of upper-left pixel's upper-left corner in geographical units
         * @param skewX Rotation about X-axis
         * @param skewY Rotation about Y-axis
         * @param bands RasterBandMetaData
         */
        public RasterMetaData(int version, int numBands, int width, int height,int srid,double scaleX,
                double scaleY, double ipX, double ipY, double skewX,
                double skewY,  RasterBandMetaData[] bands) {
            this(ByteOrder.BIG_ENDIAN, version, numBands, width, height, srid,scaleX, scaleY,
                    ipX, ipY, skewX, skewY,  bands); 
        }

        /**
         * Build a RasterMetaData
         * 
         * @param byteOrder 1:ndr/little endian 0:xdr/big endian 
         * @param version Format version (0 for this structure)       * 
         * @param width Number of pixel columns
         * @param height Number of pixel rows 
         * @param numBands Number of bands         * 
         * @param srid Spatial reference identifier 
         * @param scaleX Pixel width in geographical units 
         * @param scaleY Pixel height in geographical units
         * @param ipX X ordinate of upper-left  pixel's upper-left corner in geographical units
         * @param ipY Y ordinate of upper-left pixel's upper-left corner in geographical units
         * @param skewX Rotation about X-axis
         * @param skewY Rotation about Y-axis
         * @param bands RasterBandMetaData
         */
        public RasterMetaData(ByteOrder byteOrder, int version, int numBands,
                int width, int height,int srid,double scaleX, double scaleY, double ipX, double ipY,
                double skewX, double skewY, RasterBandMetaData[] bands) {
            this.endian = byteOrder;
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

        @Override
        public String toString() {
            return String.format(Locale.ENGLISH,"w:%d h:%d bands:%d srid:%d x:%g y:%g " +
                            "scalex:%g scaley:%g skewx:%g skewy:%g",
                    width, height, numBands,
                    srid, ipX, ipY,
                    scaleX, scaleY, skewX,
                    skewY);
        }

        /**
         * @return Raster stream length
         */
        public long getTotalLength() {
            long length = RasterUtils.RASTER_METADATA_SIZE;
            for(int idBand = 0; idBand < numBands; idBand++) {
                length += bands[idBand].getLength(width, height);
            }
            return length;
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
         * @param x offset
         * @param y offset
         * @return bytes to skip
         */
        public long getStreamOffset(int band, int x, int y) {
            return getStreamOffset(band, width * y + x);
        }

        /**
         * Compute bytes to skip in InputStream in order to read the
         * specified pixel.
         *
         * @param band Band id [0-numBands[
         * @param pixelIndex Pixel position (width * y + x)
         * @return bytes to skip
         */
        public long getStreamOffset(int band, int pixelIndex) {
            if (band >= numBands) {
                throw new IllegalArgumentException("Band number out of range");
            }
            final RasterBandMetaData metaData = bands[band];
            return metaData.offsetPixel + metaData.pixelType.pixelSize *
                    pixelIndex;
        }

        /**
         * @return The raster transformation matrix using awt API.
         */
        public AffineTransform getTransform() {
            return new AffineTransform(scaleX, skewY, skewX, scaleY, ipX, ipY);
        }

        /**
         * Compute row-column position from world coordinate
         * @param coordinate world coordinate.
         * @return raster row-column (0-based)
         */
        public int[] getPixelFromCoordinate(Point2D.Double coordinate) {
            try {
                AffineTransform inv = getTransform().createInverse();
                Point2D pt = inv.transform(coordinate, null);
                return new int[]{(int)pt.getX(), (int)pt.getY()};
            } catch (NoninvertibleTransformException ex) {
                return null;
            }
        }


        /**
         * Translate the pixel row,column into map coordinate
         *
         * @param x Column (0-based)
         * @param y Row (0-based)
         * @return Pixel world coordinate
         */
        public Coordinate getPixelCoordinate(int x, int y) {
            AffineTransform pixelTransform = getTransform();
            Point2D pt = pixelTransform.transform(new Point2D.Double(x, y), null);
            return new Coordinate(pt.getX(), pt.getY());
        }

        /**
         * Fetch raster metadata
         * 
         * @param raster Raster binary data
         * @return Complete raster and bands metadata.
         * @throws IOException
         */
        public static RasterMetaData fetchMetaData(InputStream raster)
                throws IOException {
            return fetchMetaData(raster, true);
        }

        /**
         * Fetch raster metadata
         * 
         * @param raster InputStream. The caller is responsible for closing
         *               the stream
         * @param fetchBandsMetaData If true, each band metadata is read.
         * @return Metadata of raster provided
         * @throws IOException Throw an exception in case of wrong metadata
         * format
         */
        public static RasterMetaData fetchMetaData(InputStream raster,
            boolean fetchBandsMetaData) throws IOException {
            byte[] buffer = new byte[RASTER_METADATA_SIZE];
            AtomicLong cursor = new AtomicLong(0);
            if (IOUtils.readFully(raster,buffer, buffer.length)
                    != buffer
                    .length) {
                throw new IOException(
                        "H2 is unable to " + "read the raster. ");
            }
            ByteOrder endian = buffer[(int)cursor.getAndAdd(1)] == 1 ?
                    ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
            int version = Utils.readUnsignedShort(buffer,
                    (int)cursor.getAndAdd(Short.SIZE / Byte.SIZE), endian);
            if (version > LAST_WKB_VERSION) {
                throw new IOException("H2 " +
                        "is does not " +
                        "support raster version " + version + " raster.");
            }
            int numBands = Utils.readUnsignedShort(buffer,
                    (int)cursor.getAndAdd(Short.SIZE / Byte.SIZE), endian);

            // Retrieve scale values
            double scaleX = Utils.readDouble(buffer,
                    (int)cursor.getAndAdd(Double.SIZE / Byte.SIZE), endian);
            double scaleY = Utils.readDouble(buffer,
                    (int)cursor.getAndAdd(Double.SIZE / Byte.SIZE), endian);

            // Retrieve insertion point values
            double ipX = Utils.readDouble(buffer,
                    (int)cursor.getAndAdd(Double.SIZE / Byte.SIZE), endian);
            double ipY = Utils.readDouble(buffer,
                    (int)cursor.getAndAdd(Double.SIZE / Byte.SIZE), endian);

            // Retrieve XY offset values
            double skewX = Utils.readDouble(buffer,
                    (int)cursor.getAndAdd(Double.SIZE / Byte.SIZE), endian);
            double skewY = Utils.readDouble(buffer,
                    (int)cursor.getAndAdd(Double.SIZE / Byte.SIZE), endian);

            // Retrieve the srid value
            int srid = Utils.readInt(buffer,
                    (int)cursor.getAndAdd(Integer.SIZE / Byte.SIZE), endian);

            // Retrieve width and height values
            int width = Utils.readUnsignedShort(buffer,
                    (int)cursor.getAndAdd(Short.SIZE / Byte.SIZE), endian);
            int height = Utils.readUnsignedShort(buffer,
                    (int)cursor.getAndAdd(Short.SIZE / Byte.SIZE), endian);

            RasterBandMetaData[] bands = new RasterBandMetaData[numBands];
            int idBand = 0;
            if (numBands > 0 && fetchBandsMetaData) {
                while (idBand < numBands) {
                    final long bandOffset = cursor.get();
                    // Read Flag
                    if (1 != raster.read(buffer, 0, 1)) {
                        throw new IOException("H2 is " +
                                "unable to read the " + "raster's band. ");
                    }
                    cursor.addAndGet(1);
                    byte flag = buffer[0];
                    final PixelType pixelType = PixelType.cast(flag &
                            RasterBandMetaData.BANDTYPE_PIXTYPE_MASK);
                    if (pixelType == null) {
                        throw new IOException("H2 is " +
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
                            IOUtils.readFully(raster, buffer,
                                    pixelType.pixelSize)) {
                        throw new IOException("H2 is " +
                                "unable to read the " + "raster's nodata." +
                                " ");
                    }
                    cursor.getAndAdd(pixelType.pixelSize);
                    WKBByteDriver pixelDriver = WKBByteDriver.fetchDriver
                            (pixelType, endian);
                    if(pixelDriver == null) {
                        throw new IOException("H2 is " +
                                "unable to read the " + "raster's " +
                                "nodata. ");
                    }
                    noData = pixelDriver.readAsDouble(buffer, 0);
                    if (offDB) {
                        // Read external band id
                        if (1 != raster.read(buffer, 0, 1)) {
                            throw new IOException("H2 is " +
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
                        // Skip remaining pixel until next band if exists
                        if(idBand < numBands) {
                            long skipLength =
                                    width * height * pixelType.pixelSize;
                            cursor.addAndGet(skipLength - 1);
                            IOUtils.skipFully(raster, skipLength);
                        }
                    }
                }
            }
            return new RasterMetaData(endian, version, numBands,width, height, srid,  scaleX, scaleY,
                    ipX, ipY, skewX, skewY, bands);
        }

        /**
         * Write the raster header
         * 
         * @param stream output stream of raster bytes
         * @param endian 1:ndr/little endian 0:xdr/big endian 
         * @throws IOException 
         */
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


        /**
         * Write the band header
         * 
         * @param stream output stream of raster bytes
         * @param band Band id [0-numBands[
         * @param endian 1:ndr/little endian 0:xdr/big endian 
         * @throws IOException 
         */
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
         * Compute the envelope of the raster
         * 
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
         * Get the envelope of the raster
         * 
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

    /**
     * Define merged API for byte
     */
    public static abstract class WKBByteDriver {
        protected PixelType pixelType;
        protected ByteOrder endian;

        private WKBByteDriver(PixelType pixelType, ByteOrder endian) {
            this.pixelType = pixelType;
            this.endian = endian;
        }

        public abstract double readAsDouble(byte[] buffer, int pos);

        public static WKBByteDriver fetchDriver(PixelType pixelType, ByteOrder
                endian) {
            switch (pixelType) {
                case PT_1BB:
                    return new Driver1BB(pixelType, endian);
                case PT_2BUI:
                    return new Driver2BUI(pixelType, endian);
                case PT_4BUI:
                    return new Driver4BUI(pixelType, endian);
                case PT_8BSI:
                    return new Driver8BSI(pixelType, endian);
                case PT_8BUI:
                    return new Driver8BUI(pixelType, endian);
                case PT_16BSI:
                    return new Driver16BSI(pixelType, endian);
                case PT_16BUI:
                    return new Driver16BUI(pixelType, endian);
                case PT_32BSI:
                    return new Driver32BSI(pixelType, endian);
                case PT_32BUI:
                    return new Driver32BUI(pixelType, endian);
                case PT_32BF:
                    return new Driver32BF(pixelType, endian);
                case PT_64BF:
                    return new Driver64BF(pixelType, endian);
                default:
                    return null;
            }
        }
    }

    private static class Driver1BB  extends WKBByteDriver {
        public Driver1BB(PixelType pixelType, ByteOrder endian) {
            super(pixelType, endian);
        }

        @Override
        public double readAsDouble(byte[] buffer, int pos) {
            return Utils.readUnsignedByte(buffer, pos) &
                    0x01;
        }
    }

    private static class Driver2BUI  extends WKBByteDriver {
        public Driver2BUI(PixelType pixelType, ByteOrder endian) {
            super(pixelType, endian);
        }

        @Override
        public double readAsDouble(byte[] buffer, int pos) {
            return Utils.readUnsignedByte(buffer, pos) &
                    0x03;
        }
    }

    private static class Driver4BUI  extends WKBByteDriver {
        public Driver4BUI(PixelType pixelType, ByteOrder endian) {
            super(pixelType, endian);
        }

        @Override
        public double readAsDouble(byte[] buffer, int pos) {
            return Utils.readUnsignedByte(buffer, pos) &
                    0x0F;
        }
    }
    private static class Driver8BSI  extends WKBByteDriver {
        public Driver8BSI(PixelType pixelType, ByteOrder endian) {
            super(pixelType, endian);
        }

        @Override
        public double readAsDouble(byte[] buffer, int pos) {
            return buffer[pos];
        }
    }
    private static class Driver8BUI  extends WKBByteDriver {
        public Driver8BUI(PixelType pixelType, ByteOrder endian) {
            super(pixelType, endian);
        }

        @Override
        public double readAsDouble(byte[] buffer, int pos) {
            return Utils.readUnsignedByte(buffer, pos);
        }
    }
    private static class Driver16BSI  extends WKBByteDriver {
        public Driver16BSI(PixelType pixelType, ByteOrder endian) {
            super(pixelType, endian);
        }

        @Override
        public double readAsDouble(byte[] buffer, int pos) {
            return Utils.readShort(buffer, pos, endian);
        }
    }
    private static class Driver16BUI  extends WKBByteDriver {
        public Driver16BUI(PixelType pixelType, ByteOrder endian) {
            super(pixelType, endian);
        }

        @Override
        public double readAsDouble(byte[] buffer, int pos) {
            return Utils.readUnsignedShort(buffer, pos, endian);
        }
    }

    private static class Driver32BSI  extends WKBByteDriver {
        public Driver32BSI(PixelType pixelType, ByteOrder endian) {
            super(pixelType, endian);
        }

        @Override
        public double readAsDouble(byte[] buffer, int pos) {
            return Utils.readInt(buffer, pos, endian);
        }
    }

    private static class Driver32BUI  extends WKBByteDriver {
        public Driver32BUI(PixelType pixelType, ByteOrder endian) {
            super(pixelType, endian);
        }

        @Override
        public double readAsDouble(byte[] buffer, int pos) {
            return Utils.readUnsignedInt32(buffer, pos, endian);
        }
    }

    private static class Driver32BF  extends WKBByteDriver {
        public Driver32BF(PixelType pixelType, ByteOrder endian) {
            super(pixelType, endian);
        }

        @Override
        public double readAsDouble(byte[] buffer, int pos) {
            return Float.intBitsToFloat(Utils.readInt(buffer, pos, endian));
        }
    }

    private static class Driver64BF  extends WKBByteDriver {
        public Driver64BF(PixelType pixelType, ByteOrder endian) {
            super(pixelType, endian);
        }

        @Override
        public double readAsDouble(byte[] buffer, int pos) {
            return Utils.readDouble(buffer, pos, endian);
        }
    }
}