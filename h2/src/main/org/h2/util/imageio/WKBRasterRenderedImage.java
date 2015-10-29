package org.h2.util.imageio;

import java.awt.Rectangle;
import java.awt.image.ColorModel;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.util.Vector;

/**
 * This implementation of rendered image is necessary to give an hint about
 * tiling to JAI process.
 * @author Nicolas Fortin
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
        return raster;
    }

    @Override
    public Raster getData(Rectangle rect) {
        return getData().createChild(rect.x, rect.y, rect.width, rect.height,
                rect.x, rect.y, null);
    }

    @Override
    public WritableRaster copyData(WritableRaster raster) {
        if (raster == null) {
            return getData();
        }
        int width = raster.getWidth();
        int height = raster.getHeight();
        int startX = raster.getMinX();
        int startY = raster.getMinY();

        Object tdata = null;

        for (int i = startY; i < startY+height; i++)  {
            tdata = raster.getDataElements(startX,i,width,1,tdata);
            raster.setDataElements(startX,i,width,1, tdata);
        }

        return raster;
    }
}
