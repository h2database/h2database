package org.h2.android;

/**
 * This exception is thrown when the database file is corrupt.
 */
public class H2DatabaseCorruptException extends H2Exception {
    private static final long serialVersionUID = 1L;

    H2DatabaseCorruptException() {
        super();
    }

    H2DatabaseCorruptException(String error) {
        super(error);
    }
}
