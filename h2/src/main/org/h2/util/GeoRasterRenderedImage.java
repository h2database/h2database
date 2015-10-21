/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import org.h2.api.GeoRaster;

import java.awt.*;
import java.awt.image.ColorModel;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteOrder;
import java.util.Vector;

/**
 * Convert RenderedImage into a WKB input stream.
 * This class is used when storing LobDB Raster from external image,
 * BufferedImage or RenderedOp (JAI).
 * @author Nicolas Fortin
 */
public class GeoRasterRenderedImage implements GeoRaster {
    private final RenderedImage image;
    private final RasterUtils.RasterMetaData rasterMetaData;

    private GeoRasterRenderedImage(RenderedImage image,
                                   RasterUtils.RasterMetaData rasterMetaData) {
        this.image = image;
        this.rasterMetaData = rasterMetaData;
    }

    /**
     * Wrap a rendered image with the provided metadata.
     * @param image RenderedImage instance (or PlanarImage)
     * @param metaData MetaData. Width, height and band offset will be
     *                 recomputed.
     * @return Instance of GeoRaster
     * @throws IllegalArgumentException
     */
    public static GeoRasterRenderedImage create(RenderedImage image, RasterUtils
            .RasterMetaData metaData) throws IllegalArgumentException {
        // Copy and fix metaData
        RasterUtils.RasterBandMetaData[] bands = new RasterUtils
                .RasterBandMetaData[metaData.bands.length];
        long offset = RasterUtils.RASTER_METADATA_SIZE;
        for(int idBand = 0; idBand < bands.length; idBand++) {
            final RasterUtils.RasterBandMetaData extBand = metaData
                    .bands[idBand];
            RasterUtils.RasterBandMetaData band;
            if(extBand.offDB) {
                band = new RasterUtils
                        .RasterBandMetaData(extBand.noDataValue, extBand
                        .pixelType, extBand.hasNoData, extBand
                        .externalBandId, extBand.externalPath, offset);
            } else {
                band = new RasterUtils
                        .RasterBandMetaData(extBand.noDataValue, extBand
                        .pixelType, extBand.hasNoData, offset);
            }
            bands[idBand] = band;
            offset += band.getLength(image.getWidth(), image.getHeight());
        }
        RasterUtils.RasterMetaData fixedMetaData = new RasterUtils
                .RasterMetaData(RasterUtils.LAST_WKB_VERSION, metaData.bands
                .length,metaData.scaleX, metaData.scaleY, metaData.ipX,
                metaData.ipY, metaData.skewX, metaData.skewY, metaData.srid,
                image.getWidth(), image.getHeight(), bands);
        return new GeoRasterRenderedImage(image, fixedMetaData);
    }

    /**
     * Wrap a Raster in order to make a WKBRaster of it.
     * @param image Raster
     * @param scaleX Pixel x scale in current projection unit
     * @param scaleY Pixel y scale in current projection unit
     * @param ipX Insertion point X
     * @param ipY Insertion point Y
     * @param skewX Rotation X
     * @param skewY Rotation Y
     * @param srid Srid value
     * @param noDataValue NoData value for all bands
     * @return WKBRasterWrapper instance
     */
    public static GeoRasterRenderedImage create(RenderedImage image, double scaleX,
            double scaleY, double ipX, double ipY, double skewX, double skewY,
            int srid, double noDataValue) throws IOException {
        RasterUtils.RasterBandMetaData[] bands = new RasterUtils.RasterBandMetaData[image
                .getSampleModel().getNumBands()];
        long offset = RasterUtils.RASTER_METADATA_SIZE;
        for(int idBand = 0; idBand < bands.length; idBand++) {
            int sampleSize = image.getSampleModel().getSampleSize(idBand);
            RasterUtils.PixelType pixelType;
            if (sampleSize <= Byte.SIZE) {
                pixelType = RasterUtils.PixelType.PT_8BSI;
            } else if (sampleSize <= Integer.SIZE) {
                pixelType = RasterUtils.PixelType.PT_32BSI;
            } else if(sampleSize <= Long.SIZE) {
                pixelType = RasterUtils.PixelType.PT_64BF;
            } else {
                throw new IOException("Unsupported band size " +
                        ":"+sampleSize);
            }
            bands[idBand] = new RasterUtils.RasterBandMetaData(noDataValue,
                    pixelType, true, offset);
            offset += bands[idBand].getLength(image.getWidth(), image.getHeight());
        }
        RasterUtils.RasterMetaData rasterMetaData =
                new RasterUtils.RasterMetaData(RasterUtils.LAST_WKB_VERSION,
                        bands.length, scaleX, scaleY, ipX, ipY, skewX,
                        skewY, srid, image.getWidth(), image.getHeight(),
                        bands);
        return new GeoRasterRenderedImage(image, rasterMetaData);
    }

    @Override
    public RasterUtils.RasterMetaData getMetaData() {
        return rasterMetaData;
    }

    @Override
    public InputStream asWKBRaster() {
        return new WKBRasterStream(image, rasterMetaData);
    }

    private static class WKBRasterStream extends InputStream {
        /* maximum pixel to buffer in the stream memory*/
        private final RenderedImage raster;
        private final RasterUtils.RasterMetaData rasterMetaData;
        private byte[] buffer = new byte[0];
        private int bufferCursor = 0;
        private RasterPixelDriver pixelDriver;
        private final boolean readTiles;
        private static final int MAX_ROW_CACHE = 32;


        private enum BUFFER_STATE {
            WKB_HEADER_BEGIN,WKB_HEADER_END, WKB_BAND_HEADER_BEGIN,
            WKB_BAND_HEADER_END,
            WKB_BAND_DATA_BEGIN,WKB_BAND_DATA_ITERATE
        }
        private BUFFER_STATE state = BUFFER_STATE.WKB_HEADER_BEGIN;
        private long pixelCursor = 0;
        private int currentBand = -1;
        private long streamPosition = 0;

        public WKBRasterStream(RenderedImage raster,
                RasterUtils.RasterMetaData rasterMetaData) {
            this.raster = raster;
            this.rasterMetaData = rasterMetaData;
            // Compute rows to read (fit with Tiles)
            int numTileY = raster.getNumYTiles();
            // Cache a row
            readTiles = numTileY > 1;
        }

        @Override
        public long skip(long n) throws IOException {
            if(bufferCursor + n  - 1 < buffer.length) {
                // Skip in current buffer
                streamPosition += n;
                bufferCursor += n;
                return n;
            } else {
                // Skip outside buffer
                long newPos = streamPosition + n;
                for (int bandId = rasterMetaData.numBands - 1; bandId >= 0; bandId--) {
                    final RasterUtils.RasterBandMetaData bandMeta =
                            rasterMetaData.bands[bandId];
                    if (!bandMeta.offDB) {
                        long bandBeginPos = bandMeta.offsetPixel;
                        if(bandBeginPos <= newPos) {
                            // Access to pixel in this band
                            currentBand = bandId;
                            state = BUFFER_STATE.WKB_BAND_DATA_ITERATE;
                            pixelDriver = RasterPixelDriver.getDriver
                                    (rasterMetaData.bands[currentBand]
                                            .pixelType, null);
                            long pixelTarget = (newPos - bandMeta.offsetPixel) /
                                    bandMeta.pixelType.pixelSize;
                            pixelCursor = (pixelTarget / rasterMetaData.width);
                            loadRasterTile(pixelTarget);
                            streamPosition += n;
                            return n;
                        }
                    }
                    if(bandMeta.offset <= newPos) {
                        // Skip to band meta
                        currentBand = bandId;
                        loadBandMeta();
                        long moved = bandMeta.offset - streamPosition;
                        streamPosition += moved;
                        return moved;
                    }
                }
                return super.skip(n);
            }
        }

        private void loadBandMeta() throws IOException {
            ByteArrayOutputStream stream = new
                    ByteArrayOutputStream();
            rasterMetaData.writeRasterBandHeader(stream,
                    currentBand, ByteOrder.BIG_ENDIAN);
            buffer = stream.toByteArray();
            bufferCursor = 0;
            state = BUFFER_STATE.WKB_BAND_HEADER_END;
        }

        private void loadRasterTile(long pixelTarget) {
            Raster rasterRow;
            if(readTiles) {
                rasterRow = raster.getData(
                        new Rectangle(0,
                                (int)(pixelTarget / rasterMetaData.width),
                                rasterMetaData.width,
                                Math.min(raster.getTileHeight(), raster.getHeight() -
                                        (int)(pixelTarget / rasterMetaData.width))));

            } else {
                rasterRow = raster.getData(
                        new Rectangle(0,
                                (int) (pixelTarget / rasterMetaData.width),
                                rasterMetaData.width,
                                Math.min(MAX_ROW_CACHE, raster.getHeight() -
                                        (int)(pixelTarget / rasterMetaData.width))));
            }
            int pixelSize = pixelDriver.getPixelType().pixelSize;
            buffer = new byte[rasterRow.getWidth() * rasterRow.getHeight() *
                    pixelSize];
            pixelDriver.setRaster(rasterRow);
            pixelDriver.readSamples(buffer, 0, 0, 0,
                    rasterRow.getWidth() , rasterRow.getHeight(), currentBand);
            pixelCursor += rasterRow
                    .getWidth() * rasterRow.getHeight();
            bufferCursor = (int)(pixelTarget % rasterMetaData.width)* pixelSize;
        }

        @Override
        public int read() throws IOException {
            if(bufferCursor < buffer.length) {
                streamPosition+=1;
                return buffer[bufferCursor++] & 0xff;
            } else {
                switch (state) {
                    case WKB_HEADER_BEGIN: {
                        ByteArrayOutputStream stream = new ByteArrayOutputStream();
                        rasterMetaData.writeRasterHeader(stream, ByteOrder.BIG_ENDIAN);
                        buffer = stream.toByteArray();
                        bufferCursor = 0;
                        state = BUFFER_STATE.WKB_HEADER_END;
                        return read();
                    }
                    case WKB_HEADER_END:
                        // Move to first band
                        state = BUFFER_STATE.WKB_BAND_HEADER_BEGIN;
                        return read();
                    case WKB_BAND_HEADER_END:
                        // Header of band has been transferred, if data is
                        // available for this band transfer data
                        if(!rasterMetaData.bands[currentBand].offDB) {
                            state = BUFFER_STATE.WKB_BAND_DATA_BEGIN;
                            return read();
                        } else {
                            state = BUFFER_STATE.WKB_BAND_HEADER_BEGIN;
                            return read();
                        }
                    case WKB_BAND_HEADER_BEGIN: {
                        currentBand+=1;
                        if (currentBand < rasterMetaData.numBands) {
                            loadBandMeta();
                            return read();
                        } else {
                            return -1; // End of Stream
                        }
                    }
                    case WKB_BAND_DATA_BEGIN:
                        pixelDriver = RasterPixelDriver.getDriver
                                (rasterMetaData.bands[currentBand]
                                        .pixelType, null);
                        state = BUFFER_STATE.WKB_BAND_DATA_ITERATE;
                        buffer = new byte[0];
                        bufferCursor = 0;
                        pixelCursor = 0;
                        return read();
                    case WKB_BAND_DATA_ITERATE:
                        // Want more data from raster
                        if(pixelCursor >= rasterMetaData.width * rasterMetaData.height) {
                            // End of pixels, fetch next band
                            state = BUFFER_STATE.WKB_BAND_HEADER_BEGIN;
                            return read();
                        } else {
                            loadRasterTile(pixelCursor);
                            return read();
                        }
                    default:
                        return -1;
                }
            }
        }
    }

    private static abstract class RasterPixelDriver {
        protected Raster raster;
        protected RasterUtils.PixelType pixelType;

        private RasterPixelDriver(Raster raster,
                RasterUtils.PixelType pixelType) {
            this.raster = raster;
            this.pixelType = pixelType;
        }

        /**
         * Update raster to read
         * @param raster Raster to read
         */
        public void setRaster(Raster raster) {
            this.raster = raster;
        }

        public RasterUtils.PixelType getPixelType() {
            return pixelType;
        }

        public static RasterPixelDriver getDriver(RasterUtils.PixelType
                pixelType, Raster raster) {
            switch (pixelType) {
                case PT_1BB:
                case PT_2BUI:
                case PT_4BUI:
                case PT_8BSI:
                case PT_8BUI:
                    return new Driver1BB(raster, pixelType);
                case PT_16BSI:
                    return new Driver16BSI(raster);
                case PT_16BUI:
                    return new Driver16BUI(raster);
                case PT_32BF:
                    return new Driver32BF(raster);
                case PT_32BSI:
                    return new Driver32BSI(raster);
                case PT_32BUI:
                    return new Driver32BUI(raster);
                case PT_64BF:
                    return new Driver64BF(raster);
                default:
                    return null;
            }
        }

        protected abstract void readSamples(byte[] buffer, int pos, int x, int y
                ,int width,int height, int band);

        private static class Driver1BB extends RasterPixelDriver {
            public Driver1BB(Raster raster, RasterUtils.PixelType pixelType) {
                super(raster, pixelType);
            }
            @Override
            public void readSamples(byte[] buffer, int pos, int x, int y,int
                    width,int height, int band) {
                int[] samples = new int[width * height];
                raster.getSamples(x + raster.getSampleModelTranslateX(), y + raster.getSampleModelTranslateY(),
                        width, height, band, samples);
                for(int i=0; i < samples.length; i++) {
                    buffer[pos + i] = (byte)samples[i];
                }
            }
        }
        private static class Driver16BSI extends RasterPixelDriver {
            public Driver16BSI(Raster raster) {
                super(raster, RasterUtils.PixelType.PT_16BSI);
            }
            @Override
            public void readSamples(byte[] buffer, int pos, int x, int y,int
                    width,int height, int band) {
                int[] samples = new int[width * height];
                raster.getSamples(x + raster.getSampleModelTranslateX(), y + raster.getSampleModelTranslateY(),
                        width, height, band, samples);
                for(int i=0; i < samples.length; i++) {
                    Utils.writeShort(buffer, pos + (i * pixelType.pixelSize),
                            (short) samples[i], ByteOrder.BIG_ENDIAN);
                }
            }
        }
        private static class Driver16BUI extends RasterPixelDriver {
            public Driver16BUI(Raster raster) {
                super(raster, RasterUtils.PixelType.PT_16BUI);
            }
            @Override
            public void readSamples(byte[] buffer, int pos, int x, int y,int
                    width,int height, int band) {
                int[] samples = new int[width * height];
                raster.getSamples(x + raster.getSampleModelTranslateX(), y + raster.getSampleModelTranslateY(),
                        width, height, band, samples);
                for(int i=0; i < samples.length; i++) {
                    Utils.writeUnsignedShort(buffer,
                            pos + (i * pixelType.pixelSize), samples[i],
                            ByteOrder.BIG_ENDIAN);
                }
            }
        }

        private static class Driver32BF extends RasterPixelDriver {
            public Driver32BF(Raster raster) {
                super(raster, RasterUtils.PixelType.PT_32BF);
            }
            @Override
            public void readSamples(byte[] buffer, int pos, int x, int y,int
                    width,int height, int band) {
                float[] samples = new float[width * height];
                raster.getSamples(x + raster.getSampleModelTranslateX(), y + raster.getSampleModelTranslateY(),
                        width, height, band, samples);
                for(int i=0; i < samples.length; i++) {
                    Utils.writeInt(buffer,
                            pos + (i * pixelType.pixelSize), Float
                                    .floatToIntBits(samples[i]),
                            ByteOrder.BIG_ENDIAN);
                }
            }
        }
        private static class Driver32BUI extends RasterPixelDriver {
            public Driver32BUI(Raster raster) {
                super(raster, RasterUtils.PixelType.PT_32BUI);
            }
            @Override
            public void readSamples(byte[] buffer, int pos, int x, int y,int
                    width,int height, int band) {
                double[] samples = new double[width * height];
                raster.getSamples(x + raster.getSampleModelTranslateX(), y + raster.getSampleModelTranslateY(),
                        width, height, band, samples);
                for(int i=0; i < samples.length; i++) {
                    Utils.writeUnsignedInt(buffer,
                            pos + (i * pixelType.pixelSize),
                            Double.doubleToLongBits(samples[i]),
                            ByteOrder.BIG_ENDIAN);
                }
            }
        }
        private static class Driver32BSI extends RasterPixelDriver {
            public Driver32BSI(Raster raster) {
                super(raster, RasterUtils.PixelType.PT_32BSI);
            }

            @Override
            public void readSamples(byte[] buffer, int pos, int x, int y,int
                    width,int height, int band) {
                int[] samples = new int[width * height];
                raster.getSamples(x + raster.getSampleModelTranslateX(), y + raster.getSampleModelTranslateY(),
                        width, height, band, samples);
                for(int i=0; i < samples.length; i++) {
                    Utils.writeInt(buffer, pos + (i * pixelType.pixelSize),
                            samples[i],ByteOrder.BIG_ENDIAN);
                }
            }
        }

        private static class Driver64BF extends RasterPixelDriver {
            public Driver64BF(Raster raster) {
                super(raster, RasterUtils.PixelType.PT_64BF);
            }
            @Override
            public void readSamples(byte[] buffer, int pos, int x, int y,int
                    width,int height, int band) {
                double[] samples = new double[width * height];
                raster.getSamples(x + raster.getSampleModelTranslateX(), y + raster.getSampleModelTranslateY(),
                        width, height, band, samples);
                for(int i=0; i < samples.length; i++) {
                    Utils.writeDouble(buffer,
                            pos + (i * pixelType.pixelSize), samples[i],
                            ByteOrder.BIG_ENDIAN);
                }
            }
        }
    }

    @Override
    public Vector<RenderedImage> getSources() {
        return image.getSources();
    }

    @Override
    public Object getProperty(String name) {
        return image.getProperty(name);
    }

    @Override
    public String[] getPropertyNames() {
        return image.getPropertyNames();
    }

    @Override
    public ColorModel getColorModel() {
        return image.getColorModel();
    }

    @Override
    public SampleModel getSampleModel() {
        return image.getSampleModel();
    }

    @Override
    public int getWidth() {
        return image.getWidth();
    }

    @Override
    public int getHeight() {
        return image.getHeight();
    }

    @Override
    public int getMinX() {
        return image.getMinX();
    }

    @Override
    public int getMinY() {
        return image.getMinY();
    }

    @Override
    public int getNumXTiles() {
        return image.getNumXTiles();
    }

    @Override
    public int getNumYTiles() {
        return image.getNumYTiles();
    }

    @Override
    public int getMinTileX() {
        return image.getMinTileX();
    }

    @Override
    public int getMinTileY() {
        return image.getMinTileY();
    }

    @Override
    public int getTileWidth() {
        return image.getTileWidth();
    }

    @Override
    public int getTileHeight() {
        return image.getTileHeight();
    }

    @Override
    public int getTileGridXOffset() {
        return image.getTileGridXOffset();
    }

    @Override
    public int getTileGridYOffset() {
        return image.getTileGridYOffset();
    }

    @Override
    public Raster getTile(int tileX, int tileY) {
        return image.getTile(tileX, tileY);
    }

    @Override
    public Raster getData() {
        return image.getData();
    }

    @Override
    public Raster getData(Rectangle rect) {
        return image.getData(rect);
    }

    @Override
    public WritableRaster copyData(WritableRaster raster) {
        return this.image.copyData(raster);
    }
}
