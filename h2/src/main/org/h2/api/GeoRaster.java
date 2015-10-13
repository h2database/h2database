/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.api;

import org.h2.util.RasterUtils;

import java.awt.image.RenderedImage;
import java.io.IOException;
import java.io.InputStream;

/**
 * API for raster related functions
 * @author Nicolas Fortin
 */
public interface GeoRaster extends RenderedImage {

    /**
     * Convert data stored as WKB Raster stream
     * @return WKB Raster format
     */
    InputStream asWKBRaster();

    /**
     * @return The raster metadata
     */
    RasterUtils.RasterMetaData getMetaData() throws IOException;
}
