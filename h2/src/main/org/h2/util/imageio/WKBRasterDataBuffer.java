package org.h2.util.imageio;

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
    }

    private void seek(int bank, int i) throws IOException {
        long position = metaData.getStreamOffset(bank) + i * metaData
                .bands[bank].pixelType.pixelSize;
        inputStream.seek(position);
    }

    @Override
    public int getElem(int bank, int i) {
        try {
            seek(bank, i);
            return inputStream.readInt();
        } catch (IOException ex) {
            throw new IllegalStateException("Error while reading " +
                    "ImageInputStream in WKBRasterDataBuffer", ex);
        }
    }

    @Override
    public double getElemDouble(int i) {
        try {
            seek(0, i);
            return inputStream.readDouble();
        } catch (IOException ex) {
            throw new IllegalStateException("Error while reading " +
                    "ImageInputStream in WKBRasterDataBuffer", ex);
        }
    }

    @Override
    public double getElemDouble(int bank, int i) {
        try {
            seek(bank, i);
            return inputStream.readDouble();
        } catch (IOException ex) {
            throw new IllegalStateException("Error while reading " +
                    "ImageInputStream in WKBRasterDataBuffer", ex);
        }
    }

    @Override
    public float getElemFloat(int i) {
        try {
            seek(0, i);
            return inputStream.readFloat();
        } catch (IOException ex) {
            throw new IllegalStateException("Error while reading " +
                    "ImageInputStream in WKBRasterDataBuffer", ex);
        }
    }

    @Override
    public float getElemFloat(int bank, int i) {
        try {
            seek(bank, i);
            return inputStream.readFloat();
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
