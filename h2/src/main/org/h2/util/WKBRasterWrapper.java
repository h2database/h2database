/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.awt.*;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteOrder;

/**
 * Convert RenderedImage into a WKB input stream.
 * This class is used when storing LobDB Raster from external image, BufferedImage or RenderedOp (JAI).
 * @author Nicolas Fortin
 */
public class WKBRasterWrapper {
    private final RenderedImage raster;
    private final RasterUtils.RasterMetaData rasterMetaData;

    private WKBRasterWrapper(RenderedImage raster,
            RasterUtils.RasterMetaData rasterMetaData) {
        this.raster = raster;
        this.rasterMetaData = rasterMetaData;
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
    public static WKBRasterWrapper create(RenderedImage image, double scaleX,
            double scaleY, double ipX, double ipY, double skewX, double skewY,
            int srid, double noDataValue) throws IOException {
        RasterUtils.RasterBandMetaData[] bands = new RasterUtils.RasterBandMetaData[image
                .getSampleModel().getNumBands()];
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
            bands[idBand] = new RasterUtils.RasterBandMetaData(noDataValue, pixelType, true, 0);
        }
        RasterUtils.RasterMetaData rasterMetaData =
                new RasterUtils.RasterMetaData(RasterUtils.LAST_WKB_VERSION,
                        bands.length, scaleX, scaleY, ipX, ipY, skewX,
                        skewY, srid, image.getWidth(), image.getHeight(),
                        bands);
        return new WKBRasterWrapper(image, rasterMetaData);
    }

    public InputStream toWKBRasterStream() {
        return new WKBRasterStream(raster, rasterMetaData);
    }

    private static class WKBRasterStream extends InputStream {
        /* maximum pixel to buffer in the stream memory*/
        private final RenderedImage raster;
        private final RasterUtils.RasterMetaData rasterMetaData;
        private byte[] buffer = new byte[0];
        private int bufferCursor = 0;
        private RasterPixelDriver pixelDriver;
        private final boolean readTiles;


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
            // TODO, fast access to pixels, may not be required if this class
            // always store into a Blob (never skip)
            return super.skip(n);
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
                            ByteArrayOutputStream stream = new
                                    ByteArrayOutputStream();
                            rasterMetaData.bands[currentBand].setOffset
                                    (streamPosition);
                            rasterMetaData.writeRasterBandHeader(stream,
                                    currentBand, ByteOrder.BIG_ENDIAN);
                            buffer = stream.toByteArray();
                            bufferCursor = 0;
                            state = BUFFER_STATE.WKB_BAND_HEADER_END;
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
                            Raster rasterRow;
                            if(readTiles) {
                                rasterRow = raster.getData(
                                        new Rectangle(0,
                                                (int)(pixelCursor / rasterMetaData.width),
                                                rasterMetaData.width,
                                                Math.min(raster.getTileHeight(), raster.getHeight() -
                                                        (int)(pixelCursor / rasterMetaData.width))));

                            } else {
                                rasterRow = raster.getData(
                                        new Rectangle((int) (pixelCursor % rasterMetaData.width),
                                                (int) (pixelCursor / rasterMetaData.width),
                                                rasterMetaData.width,
                                                1));
                            }
                            buffer = new byte[rasterRow.getWidth() * rasterRow.getHeight() *
                                    pixelDriver.getPixelType().pixelSize];
                            pixelDriver.setRaster(rasterRow);
                            pixelDriver.readSamples(buffer, 0, 0, 0,
                                    rasterRow.getWidth() * rasterRow.getHeight(), currentBand);
                            pixelCursor += rasterRow.getWidth() * rasterRow.getHeight();
                            bufferCursor = 0;
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

        /**
         * Read band data, copy into buffer. This function compute the
         * maximum width
         * @param buffer Pre-allocated, output buffer
         * @param pos Position in output buffer to write samples
         * @param x first pixel X
         * @param y first pixel Y
         * @param count Pixel count to read
         * @param band band
         */
        public void readSamples(byte[] buffer, int pos, int x, int
                y, int count,
                int band) {
            int countDone = 0;
            while (countDone < count) {
                int readWidth = Math.min(raster.getWidth() - x, count - countDone );
                readSamples(buffer, pos, x, y, readWidth, 1, band);
                countDone+=readWidth;
                pos += readWidth * pixelType.pixelSize;
                if(countDone < count) {
                    y +=1;
                    x = 0;
                }
            }
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
                int[] samples = new int[width];
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
                int[] samples = new int[width];
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
                int[] samples = new int[width];
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
                float[] samples = new float[width];
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
                double[] samples = new double[width];
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
                int[] samples = new int[width];
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
                double[] samples = new double[width];
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
}
