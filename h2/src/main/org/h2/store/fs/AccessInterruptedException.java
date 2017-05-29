/*
 * Copyright 2016 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Bernhard Haumacher
 */
package org.h2.store.fs;

/**
 * Internal exception that propagates a thread interruption.
 */
public class AccessInterruptedException extends RuntimeException {

	/**
	 * Creates a {@link AccessInterruptedException}.
	 *
	 * @param message See {@link Exception#getMessage()}.
	 * @param cause See {@link Exception#getCause()}.
	 */
	public AccessInterruptedException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * Creates a {@link AccessInterruptedException}.
	 *
	 * @param cause See {@link Exception#getCause()}.
	 */
	public AccessInterruptedException(Throwable cause) {
		super(cause);
	}

	
}
