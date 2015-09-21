/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.imageio;

import org.h2.util.RasterUtils;

import javax.imageio.ImageReader;
import javax.imageio.spi.ImageReaderSpi;
import javax.imageio.stream.ImageInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Locale;

/**
 * @author Nicolas Fortin
 */
public class WKBRasterReaderSpi extends ImageReaderSpi {


    /**
     * Constructor used by {@link javax.imageio.spi.IIORegistry}
     */
    public WKBRasterReaderSpi() {
        super(
                WKBRasterReaderSpi.class.getName(),
                String.valueOf(RasterUtils.LAST_WKB_VERSION),
                new String[]{"wkbraster"},
                new String[]{"wkb"},
                new String[]{},
                WKBRasterReader.class.getName(),
                new Class[]{ImageInputStream.class},
                null,
                false,
                null,
                null,
                null,
                null,
                false,
                "",
                null,
                null,
                null);
    }

    @Override
    public boolean canDecodeInput(Object source) throws IOException {
        return source instanceof ImageInputStream && isWKBRaster(
                (ImageInputStream) source);
    }

    private boolean isWKBRaster(ImageInputStream source) throws IOException {
        byte[] header = new byte[RasterUtils.RASTER_METADATA_SIZE];
        source.readFully(header);
        try {
            RasterUtils.RasterMetaData meta = RasterUtils.RasterMetaData
                    .fetchMetaData(new ByteArrayInputStream(header), false);
            return meta.version == RasterUtils.LAST_WKB_VERSION && meta
                    .height >= 0 && meta.width >= 0 && meta.numBands >= 0;
        } catch (IOException ex) {
            return false;
        }
    }

    @Override
    public ImageReader createReaderInstance(Object extension)
            throws IOException {
        return new WKBRasterReader(this);
    }

    @Override
    public String getDescription(Locale locale) {
        return "WKB Raster Format";
    }
}
