/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.utils;

import java.sql.SQLException;
import org.h2.message.DbException;

/**
 * Helper class to simplify negative testing. Usage:
 * <pre>
 * new AssertThrows() { public void test() {
 *     Integer.parseInt("not a number");
 * }};
 * </pre>
 */
public abstract class AssertThrows {

    public AssertThrows(final Class<? extends Exception> expectedExceptionClass) {
        this(new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread t, Throwable e) {
                if (e == null) {
                    throw new AssertionError("Expected an exception of type " +
                            expectedExceptionClass.getSimpleName() +
                            ", but no exception was thrown");
                }
                if (!expectedExceptionClass.isAssignableFrom(e.getClass())) {
                    AssertionError ae = new AssertionError(
                            "Expected an exception of type\n" +
                            expectedExceptionClass.getSimpleName() +
                            " to be thrown, but the method under test threw an exception of type\n" +
                            e.getClass().getSimpleName() +
                            " (see in the 'Caused by' for the exception tha was thrown)");
                    ae.initCause(e);
                    throw ae;
                }
            }
        });
    }

    public AssertThrows() {
        this(new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread t, Throwable e) {
                if (e != null) {
                    throw new AssertionError(
                            "Expected an exception to be thrown, but the test was successful");
                }
                // all exceptions are fine
            }
        });
    }

    public AssertThrows(final int expectedErrorCode) {
        this(new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread t, Throwable e) {
                int errorCode;
                if (e instanceof DbException) {
                    errorCode = ((DbException) e).getErrorCode();
                } else if (e instanceof SQLException) {
                    errorCode = ((SQLException) e).getErrorCode();
                } else {
                    errorCode = 0;
                }
                if (errorCode != expectedErrorCode) {
                    AssertionError ae = new AssertionError(
                            "Expected an SQLException or DbException with error code " + expectedErrorCode);
                    ae.initCause(e);
                    throw ae;
                }
            }
        });
    }


    private AssertThrows(Thread.UncaughtExceptionHandler handler) {
        try {
            test();
            handler.uncaughtException(null, null);
        } catch (Exception e) {
            handler.uncaughtException(null, e);
        }
    }

    public abstract void test() throws Exception;

}
