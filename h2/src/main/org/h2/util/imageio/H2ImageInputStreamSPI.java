package org.h2.util.imageio;

import org.h2.util.ImageInputStreamWrapper;

import javax.imageio.spi.ImageInputStreamSpi;
import javax.imageio.stream.ImageInputStream;
import java.io.File;
import java.io.IOException;
import java.sql.Blob;
import java.util.Locale;

/**
 * ImageInputStream provider for ImageIO.
 * @author Nicolas Fortin
 * @author Erwan Bocher
 */
public class H2ImageInputStreamSPI extends ImageInputStreamSpi {
    public static final String VERSION = "1";
    /**
     * Default constructor for java SPI
     */
    public H2ImageInputStreamSPI() {
        super("H2 Database", VERSION, Blob.class);
    }

    @Override
    public ImageInputStream createInputStreamInstance(Object input,
            boolean useCache, File cacheDir) throws IOException {
        return new ImageInputStreamWrapper(new ImageInputStreamWrapper
                .BlobStreamProvider((Blob)input), null);
    }

    @Override
    public String getDescription(Locale locale) {
        return "Blob stream wrapper";
    }
}
