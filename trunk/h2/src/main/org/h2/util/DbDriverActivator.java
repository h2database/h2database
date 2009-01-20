/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

/**
 * The driver activator loads the H2 driver when starting the bundle.
 * The driver is unloaded when stopping the bundle.
 */
public class DbDriverActivator implements BundleActivator {

    /**
     * Start the bundle. This will load and register the database driver.
     *
     * @param bundleContext the bundle context
     */
    public void start(BundleContext bundleContext) {
        org.h2.Driver.load();
    }

    /**
     * Stop the bundle. This will deregister the database driver.
     *
     * @param bundleContext the bundle context
     */
    public void stop(BundleContext bundleContext) {
        org.h2.Driver.unload();
    }

}
