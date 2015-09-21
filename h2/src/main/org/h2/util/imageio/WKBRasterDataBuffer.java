package org.h2.util.imageio;

import org.h2.util.IOUtils;
import org.h2.util.RasterUtils;

import javax.imageio.stream.ImageInputStream;
import java.awt.image.DataBuffer;
import java.io.IOException;

/**
 * A DataBuffer backed by an ImageInputStream
 * @author Nicolas Fortin
 */
public class WKBRasterDataBuffer extends DataBuffer {
    private ImageInputStream inputStream;
    private RasterUtils.RasterMetaData metaData;
    private RasterUtils.WKBByteDriver[] wkbByteDriver;
    private byte[] buffer = new byte[8];

    /**
     * Constructor.
     * @param dataType Main datatype
     * @param size Main sample size
     * @param inputStream Input stream
     * @param metaData Raster metadata
     */
    public WKBRasterDataBuffer(int dataType, int size,
            ImageInputStream inputStream, RasterUtils.RasterMetaData metaData) {
        super(dataType, size);
        this.inputStream = inputStream;
        this.metaData = metaData;
        this.wkbByteDriver = new RasterUtils.WKBByteDriver[metaData.numBands];
        for(int bandId = 0; bandId < wkbByteDriver.length; bandId++) {
            this.wkbByteDriver[bandId] = RasterUtils.WKBByteDriver
                    .fetchDriver(metaData.bands[bandId].pixelType, metaData
                            .endian);
        }
    }

    private void seek(int bank, int i) throws IOException {
        long position = metaData.bands[bank].offset + i * metaData
                .bands[bank].pixelType.pixelSize;
        inputStream.seek(position);
    }

    @Override
    public int getElem(int bank, int i) {
        try {
            seek(bank, i);
            IOUtils.readFully(inputStream, buffer, metaData.bands[bank]
                    .pixelType.pixelSize);
            return (int)wkbByteDriver[bank].readAsDouble(buffer, 0);
        } catch (IOException ex) {
            throw new IllegalStateException("Error while reading " +
                    "ImageInputStream in WKBRasterDataBuffer", ex);
        }
    }

    @Override
    public double getElemDouble(int i) {
        try {
            seek(0, i);
            IOUtils.readFully(inputStream, buffer,
                    metaData.bands[0].pixelType.pixelSize);
            return wkbByteDriver[0].readAsDouble(buffer, 0);
        } catch (IOException ex) {
            throw new IllegalStateException("Error while reading " +
                    "ImageInputStream in WKBRasterDataBuffer", ex);
        }
    }

    @Override
    public double getElemDouble(int bank, int i) {
        try {
            seek(bank, i);
            IOUtils.readFully(inputStream, buffer,
                    metaData.bands[bank].pixelType.pixelSize);
            return wkbByteDriver[bank].readAsDouble(buffer, 0);
        } catch (IOException ex) {
            throw new IllegalStateException("Error while reading " +
                    "ImageInputStream in WKBRasterDataBuffer", ex);
        }
    }

    @Override
    public float getElemFloat(int i) {
        try {
            seek(0, i);
            IOUtils.readFully(inputStream, buffer,
                    metaData.bands[0].pixelType.pixelSize);
            return (float)wkbByteDriver[0].readAsDouble(buffer, 0);
        } catch (IOException ex) {
            throw new IllegalStateException("Error while reading " +
                    "ImageInputStream in WKBRasterDataBuffer", ex);
        }
    }

    @Override
    public float getElemFloat(int bank, int i) {
        try {
            seek(bank, i);
            IOUtils.readFully(inputStream, buffer,
                    metaData.bands[bank].pixelType.pixelSize);
            return (float)wkbByteDriver[bank].readAsDouble(buffer, 0);
        } catch (IOException ex) {
            throw new IllegalStateException("Error while reading " +
                    "ImageInputStream in WKBRasterDataBuffer", ex);
        }
    }

    @Override
    public void setElem(int bank, int i, int val) {
        throw new UnsupportedOperationException("Read only data buffer");
    }

}
