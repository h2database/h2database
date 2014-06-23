/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.Properties;
import org.h2.engine.Constants;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.service.jdbc.DataSourceFactory;

/**
 * The driver activator loads the H2 driver when starting the bundle. The driver
 * is unloaded when stopping the bundle.
 */
public class DbDriverActivator implements BundleActivator {

    /**
     * Start the bundle. This will load the database driver and register the
     * DataSourceFactory service.
     *
     * @param bundleContext the bundle context
     */
    @Override
    public void start(BundleContext bundleContext) {
        org.h2.Driver driver = org.h2.Driver.load();
        Properties properties = new Properties();
        properties.put(DataSourceFactory.OSGI_JDBC_DRIVER_CLASS, org.h2.Driver.class.getName());
        properties.put(DataSourceFactory.OSGI_JDBC_DRIVER_NAME, "H2 JDBC Driver");
        properties.put(DataSourceFactory.OSGI_JDBC_DRIVER_VERSION, Constants.getFullVersion());
        bundleContext.registerService(DataSourceFactory.class.getName(), new OsgiDataSourceFactory(driver), properties);
    }

    /**
     * Stop the bundle. This will unload the database driver. The
     * DataSourceFactory service is implicitly un-registered by the OSGi
     * framework.
     *
     * @param bundleContext the bundle context
     */
    @Override
    public void stop(BundleContext bundleContext) {
        org.h2.Driver.unload();
    }

}
