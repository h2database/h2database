/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import org.h2.api.GeoRaster;
import org.h2.message.DbException;
import org.h2.util.imageio.RenderedImageReader;
import org.h2.util.imageio.WKBRasterReader;
import org.h2.value.Value;

import javax.imageio.ImageReader;
import java.awt.*;
import java.awt.image.ColorModel;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.io.InputStream;
import java.util.Vector;

/**
 * Cast a Value into GeoRaster object.
 * @author Nicolas Fortin
 */
public class GeoRasterBlob implements GeoRaster {

    private final Value value;
    private RenderedImage image;
    private RasterUtils.RasterMetaData metaData;

    /**
     * Constructor
     * @param value H2 Value
     */
    public GeoRasterBlob(Value value) {
        this.value = value;
    }

    @Override
    public RasterUtils.RasterMetaData getMetaData() throws IOException {
        if(metaData == null) {
            metaData = RasterUtils.RasterMetaData.fetchMetaData(value
                    .getInputStream(), true);
        }
        return metaData;
    }

    @Override
    public String toString() {
        try {
            return getMetaData().toString();
        } catch (IOException ex) {
            throw DbException.convertIOException(ex, "Cannot read raster metadata");
        }
    }

    @Override
    public InputStream asWKBRaster() {
        return value.getInputStream();
    }

    /**
     * Init image to return
     */
    private void initImage() {
        if(image == null) {
            ImageReader imageReader = new WKBRasterReader(null);
            try {
                imageReader.setInput(ImageInputStreamWrapper.create(value));
                image = imageReader.readAsRenderedImage(imageReader
                        .getMinIndex(), imageReader.getDefaultReadParam());
            } catch (IOException ex) {
                throw DbException.convertIOException(ex, "Cannot create Image" +
                        " from raster");
            }
        }
    }

    @Override
    public Vector<RenderedImage> getSources() {
        initImage();
        return image.getSources();
    }

    @Override
    public Object getProperty(String name) {
        initImage();
        return image.getProperty(name);
    }

    @Override
    public String[] getPropertyNames() {
        initImage();
        return image.getPropertyNames();
    }

    @Override
    public ColorModel getColorModel() {
        initImage();
        return image.getColorModel();
    }

    @Override
    public SampleModel getSampleModel() {
        initImage();
        return image.getSampleModel();
    }

    @Override
    public int getWidth() {
        try {
            return getMetaData().width;
        } catch (IOException ex) {
            throw DbException.convertIOException(ex, "Cannot read raster metadata");
        }
    }

    @Override
    public int getHeight() {
        try {
            return getMetaData().height;
        } catch (IOException ex) {
            throw DbException.convertIOException(ex, "Cannot read raster metadata");
        }
    }

    @Override
    public int getMinX() {
        initImage();
        return image.getMinX();
    }

    @Override
    public int getMinY() {
        initImage();
        return image.getMinY();
    }

    @Override
    public int getNumXTiles() {
        initImage();
        return image.getNumXTiles();
    }

    @Override
    public int getNumYTiles() {
        initImage();
        return image.getNumYTiles();
    }

    @Override
    public int getMinTileX() {
        initImage();
        return image.getMinTileX();
    }

    @Override
    public int getMinTileY() {
        initImage();
        return image.getMinTileY();
    }

    @Override
    public int getTileWidth() {
        initImage();
        return image.getTileWidth();
    }

    @Override
    public int getTileHeight() {
        initImage();
        return image.getTileHeight();
    }

    @Override
    public int getTileGridXOffset() {
        initImage();
        return image.getTileGridXOffset();
    }

    @Override
    public int getTileGridYOffset() {
        initImage();
        return image.getTileGridYOffset();
    }

    @Override
    public Raster getTile(int tileX, int tileY) {
        initImage();
        return image.getTile(tileX, tileY);
    }

    @Override
    public Raster getData() {
        initImage();
        return image.getData();
    }

    @Override
    public Raster getData(Rectangle rect) {
        initImage();
        return image.getData(rect);
    }

    @Override
    public WritableRaster copyData(WritableRaster raster) {
        initImage();
        return image.copyData(raster);
    }
}
