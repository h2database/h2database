package org.h2.util.imageio;

import org.h2.message.DbException;

import java.awt.*;
import java.awt.image.BandedSampleModel;
import java.awt.image.ColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.DataBufferDouble;
import java.awt.image.DataBufferFloat;
import java.awt.image.DataBufferInt;
import java.awt.image.DataBufferShort;
import java.awt.image.DataBufferUShort;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.util.Vector;

/**
 * This implementation of rendered image is necessary to give an hint about
 * tiling to JAI process.
 * @author Nicolas Fortin
 * @author Erwan Bocher
 */
public class WKBRasterRenderedImage implements RenderedImage {
    private ColorModel colorModel;
    private WritableRaster raster;

    public WKBRasterRenderedImage(ColorModel colorModel, WritableRaster raster) {
        this.colorModel = colorModel;
        this.raster = raster;
    }

    @Override
    public Vector<RenderedImage> getSources() {
        return new Vector<RenderedImage>();
    }

    @Override
    public Object getProperty(String name) {
        return null;
    }

    @Override
    public String[] getPropertyNames() {
        return new String[0];
    }

    @Override
    public ColorModel getColorModel() {
        return colorModel;
    }

    @Override
    public SampleModel getSampleModel() {
        return raster.getSampleModel();
    }

    @Override
    public int getWidth() {
        return raster.getWidth();
    }

    @Override
    public int getHeight() {
        return raster.getHeight();
    }

    @Override
    public int getMinX() {
        return 0;
    }

    @Override
    public int getMinY() {
        return 0;
    }

    @Override
    public int getNumXTiles() {
        return 1;
    }

    @Override
    public int getNumYTiles() {
        return getHeight();
    }

    @Override
    public int getMinTileX() {
        return 0;
    }

    @Override
    public int getMinTileY() {
        return 0;
    }

    @Override
    public int getTileWidth() {
        return getWidth();
    }

    @Override
    public int getTileHeight() {
        return 1;
    }

    @Override
    public int getTileGridXOffset() {
        return 0;
    }

    @Override
    public int getTileGridYOffset() {
        return 0;
    }

    @Override
    public Raster getTile(int tileX, int tileY) {
        return getData(new Rectangle(0, tileY, getTileWidth(), getTileHeight
                ()));
    }

    @Override
    public WritableRaster getData() {
        return getData(new Rectangle(getWidth(), getHeight()));
    }


    @Override
    public WritableRaster getData(Rectangle rect) {
        DataBuffer dataBuffer;
        int size = rect.height * rect.width;
        switch (raster.getTransferType()) {
            case DataBuffer.TYPE_BYTE:
                dataBuffer = new DataBufferByte(size,raster.getNumBands());
                break;
            case DataBuffer.TYPE_SHORT:
                dataBuffer = new DataBufferShort(size,raster.getNumBands());
                break;
            case DataBuffer.TYPE_USHORT:
                dataBuffer = new DataBufferUShort(size,raster.getNumBands());
                break;
            case DataBuffer.TYPE_FLOAT:
                dataBuffer = new DataBufferFloat(size,raster.getNumBands());
                break;
            case DataBuffer.TYPE_INT:
                dataBuffer = new DataBufferInt(size,raster.getNumBands());
                break;
            case DataBuffer.TYPE_DOUBLE:
                dataBuffer = new DataBufferDouble(size,raster.getNumBands());
                break;
            default:
                // GeoRaster is not undefined data type
                throw DbException.throwInternalError();

        }
        SampleModel compatibleSampleModel = getSampleModel()
                .createCompatibleSampleModel(rect.width, rect.height);
        WritableRaster memoryRaster = WritableRaster.createWritableRaster
                (compatibleSampleModel, dataBuffer, rect.getLocation());
        copyData(memoryRaster);
        return memoryRaster;
    }

    @Override
    public WritableRaster copyData(WritableRaster rasterArg) {
        if (rasterArg == null) {
            return getData();
        }
        int width = rasterArg.getWidth();
        int height = rasterArg.getHeight();
        int startX = rasterArg.getMinX();
        int startY = rasterArg.getMinY();

        Object tdata = null;

        for (int i = startY; i < startY+height; i++)  {
            tdata = raster.getDataElements(startX,i,width,1,tdata);
            rasterArg.setDataElements(startX,i,width,1, tdata);
        }

        return rasterArg;
    }
}
