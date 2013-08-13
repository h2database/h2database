/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: Noah Fontes <nfontes@invectorate.com>
 */
package org.h2.test.unit;

import org.h2.message.DbException;
import org.h2.test.TestBase;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * Tests the ability to deserialize objects that are not part of the system
 * class-loading scope.
 */
public class TestObjectDeserialization extends TestBase {

    private static final String CLAZZ = "org.h2.test.unit.SampleObject";
    private static final String OBJECT =
        "aced00057372001d6f72672e68322e746573742e756e69742e53616d706c654f626a65637400000000000000010200007870";

    /**
     * The thread context class loader was used.
     */
    protected boolean usesThreadContextClassLoader;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        System.setProperty("h2.useThreadContextClassLoader", "true");
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() {
        testThreadContextClassLoader();
    }

    private void testThreadContextClassLoader() {
        usesThreadContextClassLoader = false;
        Thread.currentThread().setContextClassLoader(new TestClassLoader());
        try {
            Utils.deserialize(StringUtils.convertHexToBytes(OBJECT), null);
            fail();
        } catch (DbException e) {
            // expected
        }
        assertTrue(usesThreadContextClassLoader);
    }

    /**
     * A special class loader.
     */
    private class TestClassLoader extends ClassLoader {

        public TestClassLoader() {
            super();
        }

        @Override
        protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (name.equals(CLAZZ)) {
                usesThreadContextClassLoader = true;
            }
            return super.loadClass(name, resolve);
        }

    }

}
