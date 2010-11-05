/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

/**
 * A method call that is executed in a separate thread. If the method throws an
 * exception, it is wrapped in a RuntimeException.
 *
 * @param <R> the return value
 */
public abstract class CallThread<R> extends Thread {

    /**
     * A flag indicating the get() method has been called.
     */
    protected volatile boolean stop;

    private Exception ex;
    private R result;

    /**
     * The method to be implemented.
     *
     * @return the value, or null
     * @throws Exception any exception is wrapped in a RuntimeException
     */
    public abstract R call() throws Exception;

    public void run() {
        try {
            result = call();
        } catch (Exception e) {
            this.ex = e;
        }
    }

    /**
     * Start the thread.
     *
     * @return this
     */
    public CallThread<R> execute() {
        setDaemon(true);
        setName(getClass().getName());
        start();
        return this;
    }

    /**
     * Calling this method will set the stop flag and wait until the thread is stopped.
     *
     * @return the return value, or null
     * @throws RuntimeException if an exception in the method call occurs
     */
    public R get() {
        stop = true;
        try {
            join();
        } catch (InterruptedException e) {
            // ignore
        }
        if (ex != null) {
            throw new RuntimeException(ex);
        }
        return result;
    }

}
