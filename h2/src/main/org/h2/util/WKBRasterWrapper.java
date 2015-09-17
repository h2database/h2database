package org.h2.util;

import org.h2.value.ValueRaster;

import java.awt.image.Raster;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteOrder;

/**
 * Convert raster(s) into a WKB input stream.
 * @author Nicolas Fortin
 */
public class WKBRasterWrapper {
    private final Raster raster;
    private final ValueRaster.RasterMetaData rasterMetaData;

    public WKBRasterWrapper(Raster raster,
            ValueRaster.RasterMetaData rasterMetaData) {
        this.raster = raster;
        this.rasterMetaData = rasterMetaData;
    }

    public InputStream toWKBRasterStream() {
        return new WKBRasterStream(raster, rasterMetaData);
    }

    private static class WKBRasterStream extends InputStream {
        /* maximum pixel to buffer in the stream memory*/
        private static final int PIXEL_BUFFER_COUNT = 10000;
        private final Raster raster;
        private final ValueRaster.RasterMetaData rasterMetaData;
        private byte[] buffer = new byte[0];
        private int bufferCursor = 0;
        private RasterPixelDriver pixelDriver;

        private enum BUFFER_STATE {
            WKB_HEADER_BEGIN,WKB_HEADER_END, WKB_BAND_HEADER_BEGIN,
            WKB_BAND_HEADER_END,
            WKB_BAND_DATA_BEGIN,WKB_BAND_DATA_ITERATE
        }
        private BUFFER_STATE state = BUFFER_STATE.WKB_HEADER_BEGIN;
        private long pixelCursor = 0;
        private int currentBand = -1;
        private long streamPosition = 0;

        public WKBRasterStream(Raster raster,
                ValueRaster.RasterMetaData rasterMetaData) {
            this.raster = raster;
            this.rasterMetaData = rasterMetaData;
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
                                        .pixelType, raster);
                        state = BUFFER_STATE.WKB_BAND_DATA_ITERATE;
                        buffer = new byte[PIXEL_BUFFER_COUNT * pixelDriver
                                .getPixelType().pixelSize];
                        bufferCursor = buffer.length;
                        pixelCursor = 0;
                        return read();
                    case WKB_BAND_DATA_ITERATE:
                        // Want more data from raster
                        long readWidth = Math.min(PIXEL_BUFFER_COUNT,
                                rasterMetaData.width * rasterMetaData.height -
                                        pixelCursor);
                        if(readWidth <= 0) {
                            // End of pixels, fetch next band
                            state = BUFFER_STATE.WKB_BAND_HEADER_BEGIN;
                            return read();
                        } else {
                            if(readWidth < PIXEL_BUFFER_COUNT) {
                                // Resize buffer
                                buffer = new byte[(int)(readWidth * pixelDriver
                                        .getPixelType().pixelSize)];
                            }
                            pixelDriver.readSamples(buffer, 0,
                                    (int) (pixelCursor % rasterMetaData.width),
                                    (int) (pixelCursor / rasterMetaData.width),
                                    (int) readWidth, currentBand);
                            pixelCursor += readWidth;
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
        protected ValueRaster.PixelType pixelType;

        private RasterPixelDriver(Raster raster,
                ValueRaster.PixelType pixelType) {
            this.raster = raster;
            this.pixelType = pixelType;
        }

        public ValueRaster.PixelType getPixelType() {
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

        public static RasterPixelDriver getDriver(ValueRaster.PixelType
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
            public Driver1BB(Raster raster, ValueRaster.PixelType pixelType) {
                super(raster, pixelType);
            }
            @Override
            public void readSamples(byte[] buffer, int pos, int x, int y,int
                    width,int height, int band) {
                int[] samples = new int[width];
                raster.getSamples(x, y , width, height, band, samples);
                for(int i=0; i < samples.length; i++) {
                    buffer[pos + i] = (byte)samples[i];
                }
            }
        }
        private static class Driver16BSI extends RasterPixelDriver {
            public Driver16BSI(Raster raster) {
                super(raster, ValueRaster.PixelType.PT_16BSI);
            }
            @Override
            public void readSamples(byte[] buffer, int pos, int x, int y,int
                    width,int height, int band) {
                int[] samples = new int[width];
                raster.getSamples(x, y , width, height, band, samples);
                for(int i=0; i < samples.length; i++) {
                    Utils.writeShort(buffer, pos + (i * pixelType.pixelSize),
                            (short) samples[i], ByteOrder.BIG_ENDIAN);
                }
            }
        }
        private static class Driver16BUI extends RasterPixelDriver {
            public Driver16BUI(Raster raster) {
                super(raster, ValueRaster.PixelType.PT_16BUI);
            }
            @Override
            public void readSamples(byte[] buffer, int pos, int x, int y,int
                    width,int height, int band) {
                int[] samples = new int[width];
                raster.getSamples(x, y , width, height, band, samples);
                for(int i=0; i < samples.length; i++) {
                    Utils.writeUnsignedShort(buffer,
                            pos + (i * pixelType.pixelSize), samples[i],
                            ByteOrder.BIG_ENDIAN);
                }
            }
        }

        private static class Driver32BF extends RasterPixelDriver {
            public Driver32BF(Raster raster) {
                super(raster, ValueRaster.PixelType.PT_32BF);
            }
            @Override
            public void readSamples(byte[] buffer, int pos, int x, int y,int
                    width,int height, int band) {
                float[] samples = new float[width];
                raster.getSamples(x, y , width, height, band, samples);
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
                super(raster, ValueRaster.PixelType.PT_32BUI);
            }
            @Override
            public void readSamples(byte[] buffer, int pos, int x, int y,int
                    width,int height, int band) {
                double[] samples = new double[width];
                raster.getSamples(x, y , width, height, band, samples);
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
                super(raster, ValueRaster.PixelType.PT_32BSI);
            }

            @Override
            public void readSamples(byte[] buffer, int pos, int x, int y,int
                    width,int height, int band) {
                int[] samples = new int[width];
                raster.getSamples(x, y , width, height, band, samples);
                for(int i=0; i < samples.length; i++) {
                    Utils.writeInt(buffer, pos + (i * pixelType.pixelSize),
                            samples[i],ByteOrder.BIG_ENDIAN);
                }
            }
        }

        private static class Driver64BF extends RasterPixelDriver {
            public Driver64BF(Raster raster) {
                super(raster, ValueRaster.PixelType.PT_64BF);
            }
            @Override
            public void readSamples(byte[] buffer, int pos, int x, int y,int
                    width,int height, int band) {
                double[] samples = new double[width];
                raster.getSamples(x, y , width, height, band, samples);
                for(int i=0; i < samples.length; i++) {
                    Utils.writeDouble(buffer,
                            pos + (i * pixelType.pixelSize), samples[i],
                            ByteOrder.BIG_ENDIAN);
                }
            }
        }
    }
}
