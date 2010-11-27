package org.h2.android;

/**
 * This exception is thrown the requested data is not available, for example
 * when calling simpleQueryForString() or simpleQueryForLong() for a statement
 * that doesn't return a value.
 */
public class H2DoneException extends H2Exception {
    private static final long serialVersionUID = 1L;

    H2DoneException() {
        super();
    }

    H2DoneException(String error) {
        super(error);
    }
}
