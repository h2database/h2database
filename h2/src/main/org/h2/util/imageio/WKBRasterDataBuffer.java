package org.h2.util.imageio;

import org.h2.util.IOUtils;
import org.h2.util.RasterUtils;

import javax.imageio.ImageReadParam;
import javax.imageio.stream.ImageInputStream;
import java.awt.image.DataBuffer;
import java.awt.image.SampleModel;
import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * A DataBuffer backed by an ImageInputStream
 * @author Nicolas Fortin, CNRS
 * @author Erwan Bocher, CNRS
 */
public class WKBRasterDataBuffer extends DataBuffer {
    private ImageInputStream inputStream;
    private RasterUtils.RasterMetaData metaData;
    private RasterUtils.WKBByteDriver[] wkbByteDriver;
    private byte[] buffer = new byte[8];
    private ByteArrayInputStream[] cacheRow;
    private int cacheRowId = -1;
    private final int pixelXSubSampling;
    private final int pixelYSubSampling;
    private final boolean specialSubSampling;
    private final int outWidth;
    private final int outHeight;
    private final int pixelOffset;
    private final int cacheInterval;


    /**
     * Constructor.
     * @param dataType Main datatype
     * @param size Main sample size
     * @param inputStream Input stream
     * @param metaData Raster metadata
     * @param imageReadParam Image parameters to decode the input stream image
     * @param sampleModel interface for extracting samples of pixels in an image.
     */
    public WKBRasterDataBuffer(int dataType, int size,
            ImageInputStream inputStream, RasterUtils.RasterMetaData metaData, ImageReadParam imageReadParam,
                               SampleModel sampleModel) {
        super(dataType, size);
        this.inputStream = inputStream;
        this.metaData = metaData;
        this.wkbByteDriver = new RasterUtils.WKBByteDriver[metaData.numBands];
        for(int bandId = 0; bandId < wkbByteDriver.length; bandId++) {
            this.wkbByteDriver[bandId] = RasterUtils.WKBByteDriver
                    .fetchDriver(metaData.bands[bandId].pixelType, metaData
                            .endian);
        }
        this.pixelXSubSampling = imageReadParam.getSourceXSubsampling();
        this.pixelYSubSampling = imageReadParam.getSourceYSubsampling();
        this.outHeight = sampleModel.getHeight();
        this.outWidth = sampleModel.getWidth();
        this.pixelOffset = imageReadParam.getSourceRegion() == null ? 0 : imageReadParam.getSourceRegion().y * metaData.width + imageReadParam.getSourceRegion().x;
        this.cacheInterval = metaData.width;
        // TODO prepare OffDB buffer
        specialSubSampling = pixelXSubSampling != 1 || pixelYSubSampling != 1;
    }

    /**
     * Return the row indice for a pixel position
     * @param i
     * @return 
     */
    private int getRow(int i) {
        return (i - pixelOffset) / metaData.width;
    }

    /**
     * Return the column indice for a pixel position
     * @param i
     * @return 
     */
    private int getColumn(int i) {
        return (i - pixelOffset) % metaData.width;
    }

    /**
     * Cache row in order to reused it
     * @param rowId
     * @throws IOException 
     */
    private void cachePixels(int rowId) throws IOException {
        if(cacheRow == null) {
            cacheRow = new ByteArrayInputStream[metaData.numBands];
        }
        for(int idBand = 0; idBand < metaData.numBands; idBand++) {
            long position = metaData.getStreamOffset(idBand, rowId  * cacheInterval);
            inputStream.seek(position);
            byte[] row = new byte[cacheInterval * metaData.bands[idBand].pixelType.pixelSize];
            IOUtils.readFully(inputStream, row, row.length);
            cacheRow[idBand] = new ByteArrayInputStream(row);
        }
        cacheRowId = rowId;
    }

    /**
     * Load a row according a pixel position
     * 
     * @param bank band index
     * @param i pixel position
     * @throws IOException 
     */
    private void seek(int bank, int i) throws IOException {
        if(specialSubSampling) {
            final int column = ((i - pixelOffset) % metaData.width) * pixelXSubSampling;
            final int row = ((i - pixelOffset) / metaData.width) * pixelYSubSampling;
            if(column >= metaData.width || row >= metaData.height) {
                throw new IOException("Out of band exception");
            }
            i = pixelOffset + row * metaData.width + column;
        }
        if(i / cacheInterval != cacheRowId) {
            cachePixels(i / cacheInterval);
        }
        cacheRow[bank].reset();
        cacheRow[bank].skip((i % cacheInterval) * metaData.bands[bank]
                .pixelType.pixelSize);
    }

    @Override
    public int getElem(int bank, int i) {
        return (int)getElemDouble(bank, i);
    }

    @Override
    public double getElemDouble(int i) {
        return getElemDouble(0, i);
    }

    @Override
    public double getElemDouble(int bank, int i) {
        try {
            seek(bank, i);
            IOUtils.readFully(cacheRow[bank], buffer,
                    metaData.bands[bank].pixelType.pixelSize);
            return wkbByteDriver[bank].readAsDouble(buffer, 0);
        } catch (IOException ex) {
            throw new IllegalStateException("Error while reading " +
                    "ImageInputStream in WKBRasterDataBuffer", ex);
        }
    }

    @Override
    public float getElemFloat(int i) {
        return (float)getElemDouble(i);
    }

    @Override
    public float getElemFloat(int bank, int i) {
        return (float)getElemDouble(bank, i);
    }

    @Override
    public void setElem(int bank, int i, int val) {
        throw new UnsupportedOperationException("Read only data buffer");
    }

    @Override
    public int getSize() {
        return super.getSize();
    }
}
