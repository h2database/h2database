package org.h2.util.imageio;

import org.h2.message.DbException;

import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.util.Vector;

/**
 * This class is used for OffDb bands.
 * Instead of reading the entire image, it query small tiles to ImageReader.
 * This help to reduce memory usage, with some random cost of cpu time and io
 * processing.
 * @author Nicolas Fortin
 * @author Erwan Bocher
 */
public class RenderedImageReader implements RenderedImage {
    private final ImageReader imageReader;
    private BufferedImage pixelSample;


    public RenderedImageReader(ImageReader imageReader) {
        this.imageReader = imageReader;
        try {
            ImageReadParam param = imageReader.getDefaultReadParam();
            param.setSourceRegion(new Rectangle(1, 1));
            pixelSample = imageReader.read(imageReader.getMinIndex(), param);
        } catch (IOException ex) {
            pixelSample = null;
        }
    }

    @Override
    public int getWidth() {
        try {
            return imageReader.getWidth(imageReader.getMinIndex());
        } catch (IOException ex) {
            throw DbException.convertIOException(ex, "");
        }
    }

    @Override
    public int getHeight() {
        try {
            return imageReader.getHeight(imageReader.getMinIndex());
        } catch (IOException ex) {
            throw DbException.convertIOException(ex, "");
        }
    }

    @Override
    public Vector<RenderedImage> getSources() {
        return null;
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
        if(pixelSample != null) {
            return pixelSample.getColorModel();
        } else {
            return null;
        }
    }

    @Override
    public SampleModel getSampleModel() {
        if(pixelSample != null) {
            return pixelSample.getSampleModel();
        } else {
            return null;
        }
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
        try {
            return (int)Math.ceil(imageReader.getWidth(imageReader.getMinIndex()) / (double)imageReader.getTileWidth(imageReader.getMinIndex()));
        } catch (IOException ex) {
            throw DbException.convertIOException(ex, "");
        }
    }

    @Override
    public int getNumYTiles() {
        try {
            return (int)Math.ceil(imageReader.getHeight(imageReader.getMinIndex()) / (double)imageReader.getTileHeight(imageReader.getMinIndex()));
        } catch (IOException ex) {
            throw DbException.convertIOException(ex, "");
        }
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
        try {
            return imageReader.getTileWidth(imageReader.getMinIndex());
        } catch (IOException ex) {
            throw DbException.convertIOException(ex, "");
        }
    }

    @Override
    public int getTileHeight() {
        try {
            return imageReader.getTileHeight(imageReader.getMinIndex());
        } catch (IOException ex) {
            throw DbException.convertIOException(ex, "");
        }
    }

    @Override
    public int getTileGridXOffset() {
        try {
            return imageReader.getTileGridXOffset(imageReader.getMinIndex());
        } catch (IOException ex) {
            throw DbException.convertIOException(ex, "");
        }
    }

    @Override
    public int getTileGridYOffset() {
        try {
            return imageReader.getTileGridYOffset(imageReader.getMinIndex());
        } catch (IOException ex) {
            throw DbException.convertIOException(ex, "");
        }
    }

    @Override
    public Raster getTile(int tileX, int tileY) {
        try {
            return imageReader.readAsRenderedImage(imageReader.getMinIndex(),
                    null).getTile(tileX, tileY);
        } catch (IOException ex) {
            throw DbException.convertIOException(ex, "");
        }
    }

    @Override
    public Raster getData() {
        try {
            return imageReader.readAsRenderedImage(imageReader.getMinIndex(),
                    null).getData();
        } catch (IOException ex) {
            throw DbException.convertIOException(ex, "");
        }
    }

    @Override
    public Raster getData(Rectangle rect) {
        try {
            ImageReadParam param = imageReader.getDefaultReadParam();
            param.setSourceRegion(rect);
            return imageReader.readAsRenderedImage(imageReader.getMinIndex(),
                    param).getData();
        } catch (IOException ex) {
            throw DbException.convertIOException(ex, "");
        }
    }

    @Override
    public WritableRaster copyData(WritableRaster raster) {
        throw new UnsupportedOperationException();
    }
}
