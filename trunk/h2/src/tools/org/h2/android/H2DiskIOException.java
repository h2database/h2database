package org.h2.android;

/**
 * This exception is thrown when there was a IO exception.
 */
public class H2DiskIOException extends H2Exception {
    private static final long serialVersionUID = 1L;

    H2DiskIOException() {
        super();
    }

    H2DiskIOException(String error) {
        super(error);
    }
}
