/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.utils;

/**
 * Helper class to for negative testing. Usage:
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
        }, "Expected an exception of type " +
                expectedExceptionClass.getSimpleName() +
                ", but the test was successful");
    }

    public AssertThrows() {
        this(new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread t, Throwable e) {
                // all exceptions are fine
            }
        }, "Expected an exception to be thrown, but the test was successful");
    }

    private AssertThrows(Thread.UncaughtExceptionHandler handler, String expected) {
        try {
            test();
            throw new AssertionError(expected);
        } catch (Exception e) {
            handler.uncaughtException(null, e);
        }
    }

    public abstract void test() throws Exception;

}
