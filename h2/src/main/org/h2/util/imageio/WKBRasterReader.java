/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.imageio;

import org.h2.util.RasterUtils;

import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.ImageTypeSpecifier;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.spi.ImageReaderSpi;
import javax.imageio.stream.ImageInputStream;
import java.awt.Point;
import java.awt.Transparency;
import java.awt.color.ColorSpace;
import java.awt.image.BandedSampleModel;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.ComponentColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * ImageIO reader for WKB Raster driver
 * @author Nicolas Fortin
 */
public class WKBRasterReader extends ImageReader {
    private RasterUtils.RasterMetaData metaData;

    public WKBRasterReader(ImageReaderSpi originatingProvider) {
        super(originatingProvider);
    }

    protected RasterUtils.RasterMetaData getMetaData() throws IOException {
        if(input == null) {
            throw new IOException("Call setInput before fetching ImageReader " +
                    "data");
        }
        if(metaData == null) {
            metaData = RasterUtils.RasterMetaData.fetchMetaData(new
                    ImageInputStreamWrapper((ImageInputStream)this.input), true);
        }
        return metaData;
    }

    private ColorModel getColorModel() throws IOException {
        return new ComponentColorModel(ColorSpace
                .getInstance(ColorSpace.CS_sRGB), true, false,
                Transparency.OPAQUE,getCommonDataType(getMetaData()));
    }

    @Override
    public int getNumImages(boolean allowSearch) throws IOException {
        return 1;
    }

    @Override
    public int getWidth(int imageIndex) throws IOException {
        return getMetaData().width;
    }

    @Override
    public int getHeight(int imageIndex) throws IOException {
        return getMetaData().height;
    }

    @Override
    public Iterator<ImageTypeSpecifier> getImageTypes(int imageIndex)
            throws IOException {
        List<ImageTypeSpecifier> typeList = new ArrayList<ImageTypeSpecifier>();
        typeList.add(new ImageTypeSpecifier(getColorModel(), getSampleModel()));
        return typeList.iterator();
    }

    @Override
    public IIOMetadata getStreamMetadata() throws IOException {
        return null;
    }

    @Override
    public IIOMetadata getImageMetadata(int imageIndex) throws IOException {
        return null;
    }

    @Override
    public BufferedImage read(int imageIndex, ImageReadParam param)
            throws IOException {
        return new BufferedImage(getColorModel(), readRaster
                (imageIndex, param), false, null);
    }

    @Override
    public boolean isRandomAccessEasy(int imageIndex) throws IOException {
        return true;
    }

    @Override
    public boolean canReadRaster() {
        return true;
    }

    protected SampleModel getSampleModel() throws IOException {
        RasterUtils.RasterMetaData meta = getMetaData();
        final int commonDataType = getCommonDataType(meta);
        int[] bankIndices = new int[meta.numBands];
        // Skip band metadata
        int[] bandOffset = new int[meta.numBands];
        for(int i=0; i < meta.numBands; i++) {
            bankIndices[i] = i;
            // The offset is quite small. This is only the
            bandOffset[i] = 0; // ImageIO does not support well offset when
            // it duplicate databuffer
        }
        return new BandedSampleModel(commonDataType,
                meta.width, meta.height, meta.width, bankIndices, bandOffset);
    }

    @Override
    public WritableRaster readRaster(int imageIndex, ImageReadParam param)
            throws IOException {
        RasterUtils.RasterMetaData meta = getMetaData();
        final int commonDataType = getCommonDataType(meta);
        SampleModel sampleModel = getSampleModel();
        DataBuffer dataBuffer = new WKBRasterDataBuffer(commonDataType,
                -1, (ImageInputStream)input, meta);
        return Raster.createWritableRaster(sampleModel, dataBuffer, new Point
                ());
    }


    /**
     * @param metaData Raster metadata
     * @return Bands type, or {@link java.awt.image.DataBuffer#TYPE_UNDEFINED}
     * if one pixel type is different.
     */
    public static int getCommonDataType(RasterUtils.RasterMetaData metaData) {
        int common = DataBuffer.TYPE_UNDEFINED;
        for(int idBand = 0; idBand < metaData.numBands; idBand++) {
            int pType = getDataTypeFromPixelType(metaData.bands[idBand]
                    .pixelType);
            if(common == DataBuffer.TYPE_UNDEFINED || common == pType) {
                common = pType;
            } else {
                return DataBuffer.TYPE_UNDEFINED;
            }
        }
        return common;
    }

    public static int getDataTypeFromPixelType(RasterUtils.PixelType
            pixelType) {
        switch (pixelType) {
            case PT_1BB:
            case PT_2BUI:
            case PT_4BUI:
            case PT_8BSI:
            case PT_8BUI:
                return DataBuffer.TYPE_BYTE;
            case PT_16BSI:
                return DataBuffer.TYPE_SHORT;
            case PT_16BUI:
                return DataBuffer.TYPE_USHORT;
            case PT_32BF:
                return DataBuffer.TYPE_FLOAT;
            case PT_32BSI:
                return DataBuffer.TYPE_INT;
            case PT_32BUI:
            case PT_64BF:
                return DataBuffer.TYPE_DOUBLE;
            default:
                return -1;
        }
    }
}
