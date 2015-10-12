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
import java.awt.Rectangle;
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
        int colorSpaceIndex;
        if(getMetaData().numBands >= 3) {
            colorSpaceIndex = ColorSpace.CS_sRGB;
        } else {
            colorSpaceIndex = ColorSpace.CS_GRAY;
        }
        return new ComponentColorModel(
                ColorSpace.getInstance( colorSpaceIndex),
                getMetaData().numBands == 4, false, getMetaData().numBands == 4 ?
                Transparency.TRANSLUCENT : Transparency.OPAQUE,
                getCommonDataType(getMetaData()));
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
        typeList.add(new ImageTypeSpecifier(getColorModel(), getSampleModel(getDefaultReadParam())));
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

    protected SampleModel getSampleModel(ImageReadParam param) throws IOException {
        RasterUtils.RasterMetaData meta = getMetaData();
        Rectangle srcRegion = new Rectangle();
        Rectangle destRegion = new Rectangle();
        computeRegions(param, meta.width, meta.height, null, srcRegion, destRegion);
        int width = destRegion.width;
        int height = destRegion.height;
        final int commonDataType = getCommonDataType(meta);
        int[] bankIndices = new int[meta.numBands];
        // Skip band metadata
        int[] bandOffset = new int[meta.numBands];
        int offset = 0;
        if(param.getSourceRegion() != null) {
            offset = param.getSourceRegion().y * meta.width + param.getSourceRegion().x;
        }
        for(int i=0; i < meta.numBands; i++) {
            bankIndices[i] = i;
            bandOffset[i] = offset;
        }
        return new BandedSampleModel(commonDataType,
                width, height, meta.width, bankIndices, bandOffset);
    }

    @Override
    public WritableRaster readRaster(int imageIndex, ImageReadParam param)
            throws IOException {
        if(param == null) {
            param = getDefaultReadParam();
        }
        RasterUtils.RasterMetaData meta = getMetaData();

        Rectangle destRegion = new Rectangle(0, 0, 0, 0);
        Rectangle sourceRegion = new Rectangle(0, 0, 0, 0);
        computeRegions(param, meta.width, meta.height, null, sourceRegion, destRegion);

        //checkReadParamBandSettings(param, meta.numBands, dest.getSampleModel().getNumBands());

        final int commonDataType = getCommonDataType(meta);
        SampleModel sampleModel = getSampleModel(param);
        DataBuffer dataBuffer = new WKBRasterDataBuffer(commonDataType,
                -1, (ImageInputStream)input, meta, param, sampleModel);
        return Raster.createWritableRaster(sampleModel, dataBuffer, new Point());
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
