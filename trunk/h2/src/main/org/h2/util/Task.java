/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

/**
 * A method call that is executed in a separate thread. If the method throws an
 * exception, it is wrapped in a RuntimeException.
 */
public abstract class Task implements Runnable {

    /**
     * A flag indicating the get() method has been called.
     */
    protected volatile boolean stop;

    /**
     * The result, if any.
     */
    protected Object result;

    private Thread thread;

    private Exception ex;

    /**
     * The method to be implemented.
     *
     * @throws Exception any exception is wrapped in a RuntimeException
     */
    public abstract void call() throws Exception;

    public void run() {
        try {
            call();
        } catch (Exception e) {
            this.ex = e;
        }
    }

    /**
     * Start the thread.
     *
     * @return this
     */
    public Task execute() {
        thread = new Thread(this, getClass().getName());
        thread.setDaemon(true);
        thread.start();
        return this;
    }

    /**
     * Calling this method will set the stop flag and wait until the thread is
     * stopped.
     *
     * @return the result, or null
     * @throws RuntimeException if an exception in the method call occurs
     */
    public Object get() {
        Exception e = getException();
        if (e != null) {
            throw new RuntimeException(e);
        }
        return result;
    }

    /**
     * Get the exception that was thrown in the call (if any).
     *
     * @return the exception or null
     */
    public Exception getException() {
        stop = true;
        if (thread == null) {
            throw new IllegalStateException("Thread not started");
        }
        try {
            thread.join();
        } catch (InterruptedException e) {
            // ignore
        }
        if (ex != null) {
            return ex;
        }
        return null;
    }

}
