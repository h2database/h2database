/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.util;

import org.h2.engine.Constants;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.service.jdbc.DataSourceFactory;

import java.util.Properties;

/**
 * The driver activator loads the H2 driver when starting the bundle. The driver
 * is unloaded when stopping the bundle.
 */
public class DbDriverActivator implements BundleActivator {

    private OSGIClassFactory osgiClassFactory;
    private OSGIServiceClassFactory osgiServiceClassFactory;

    /**
     * Start the bundle. This will load the database driver and register the
     * DataSourceFactory service.
     *
     * @param bundleContext the bundle context
     */
    @Override
    public void start(BundleContext bundleContext) {
        org.h2.Driver driver = org.h2.Driver.load();
        osgiClassFactory = new OSGIClassFactory(bundleContext);
        osgiServiceClassFactory = new OSGIServiceClassFactory(bundleContext);
        Utils.addClassFactory(osgiClassFactory);
        Utils.addClassFactory(osgiServiceClassFactory);
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
        Utils.removeClassFactory(osgiClassFactory);
        Utils.removeClassFactory(osgiServiceClassFactory);
        org.h2.Driver.unload();
        osgiClassFactory = null;
    }

    /**
     * Extend the H2 class loading in order to find class defined in other bundles.
     *
     * The class format for bundle class is the following:
     * BundleSymbolicName:BundleVersion:BinaryClassName
     */
    private static class OSGIClassFactory implements Utils.ClassFactory {

        /**
         * Separator character to merge bundle name,version and class binary
         * name
         */
        public static final String SEPARATOR = ":";
        private static final int BUNDLE_SYMBOLIC_NAME_INDEX = 0;
        private static final int BUNDLE_VERSION_INDEX = 1;
        private static final int BINARY_CLASS_NAME_INDEX = 2;
        private BundleContext bundleContext;

        /**
         * Constructor
         *
         * @param bundleContext Valid bundleContext instance
         */
        OSGIClassFactory(BundleContext bundleContext) {
            this.bundleContext = bundleContext;
        }

        @Override
        public boolean match(String name) {
            // OSGi binary class name must contain two SEPARATOR character
            int index = name.indexOf(SEPARATOR);
            return !(index == -1 || name.indexOf(SEPARATOR, index + 1) == -1);
        }

        @Override
        public Class<?> loadClass(String name) throws ClassNotFoundException {
            String[] parts = name.split(SEPARATOR);
            if (parts.length != 3) {
                throw new ClassNotFoundException(
                        "OSGi class binary name must contain only 2 '"
                                + SEPARATOR + "' characters");
            }
            for (Bundle bundle : bundleContext.getBundles()) {
                if (bundle.getSymbolicName().equals(
                        parts[BUNDLE_SYMBOLIC_NAME_INDEX])
                        && bundle.getVersion().toString()
                                .equals(parts[BUNDLE_VERSION_INDEX])) {
                    // Found the right bundle
                    return bundle.loadClass(parts[BINARY_CLASS_NAME_INDEX]);
                }
            }
            throw new ClassNotFoundException("OSGi Bundle not found "
                    + parts[BUNDLE_SYMBOLIC_NAME_INDEX] + " "
                    + parts[BUNDLE_VERSION_INDEX]);
        }
    }

    /**
     * Extend the H2 class loading in order to find class defined in other
     * bundles.
     *
     * The class must be registered as a service.
     *
     * The main difference with {@link OSGIClassFactory} is that it does not
     * rely to a specific bundle. OSGIServiceClassFactory is the preferred way
     * to register function used in table constraints, these functions should
     * not be removed from the database.
     *
     * The class format for bundle service class is the following:
     * OSGI:BinaryClassNameService
     */
    private static class OSGIServiceClassFactory implements Utils.ClassFactory {

        /**
         * Separator character to merge bundle name, version and class binary
         * name
         */
        public static final String SEPARATOR = "=";

        private static final int BINARY_CLASS_NAME_INDEX = 1;

        private BundleContext bundleContext;

        /**
         * Constructor
         * @param bundleContext Valid bundleContext instance
         */
        OSGIServiceClassFactory(BundleContext bundleContext) {
            this.bundleContext = bundleContext;
        }

        @Override
        public boolean match(String name) {
            return name.startsWith("OSGI" + SEPARATOR);
        }

        @Override
        public Class<?> loadClass(String name) throws ClassNotFoundException {
            String[] parts = name.split(SEPARATOR);
            if (parts.length != 2) {
                throw new ClassNotFoundException(
                        "OSGi class binary name must contain only 1 '"
                                + SEPARATOR + "' characters");
            }
            ServiceReference serviceReference = bundleContext
                    .getServiceReference(parts[BINARY_CLASS_NAME_INDEX]);
            if (serviceReference != null) {
                return bundleContext.getService(serviceReference).getClass();
            }
            throw new ClassNotFoundException("OSGi Service not found "+parts[BINARY_CLASS_NAME_INDEX]);
        }
    }

}
