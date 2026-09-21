/*
 * Copyright 2004-2026 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: Noah Fontes <nfontes@invectorate.com>
 */
package org.h2.test.unit;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;
import org.h2.util.JdbcUtils;
import org.h2.util.StringUtils;
import java.io.Serializable;

/**
 * Tests the ability to deserialize objects that are not part of the system
 * class-loading scope.
 */
public class TestObjectDeserialization extends TestBase {

    private static final String CLAZZ = "org.h2.test.unit.SampleObject";
    private static final String OBJECT =
        "aced00057372001d6f72672e68322e746573742e756" +
        "e69742e53616d706c654f626a65637400000000000000010200007870";

    private static final String GOOD_OBJECT =
        "aced00057372003b6f72672e68322e746573742e756e69742e546573744f626a6563744465736572" +
        "69616c697a6174696f6e24476f6f6453616d706c654f626a65637400000000000000010200007870";

    private static final String BAD_OBJECT =
        "aced00057372003a6f72672e68322e746573742e756e69742e546573744f626a6563744465736572" +
        "69616c697a6174696f6e2442616453616d706c654f626a65637400000000000000010200007870";

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
        System.setProperty("h2.allowedClasses",
                "org.h2.test.unit.SampleObject, org.h2.test.unit.TestObjectDeserialization$GoodSampleObject");
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() {
        testThreadContextClassLoader();
    }

    private void testThreadContextClassLoader() {
        usesThreadContextClassLoader = false;
        Thread.currentThread().setContextClassLoader(new TestClassLoader());
        assertThrows(ErrorCode.DESERIALIZATION_FAILED_1,
                () -> JdbcUtils.deserialize(StringUtils.convertHexToBytes(OBJECT), null));

        assertThrows(ErrorCode.DESERIALIZATION_FAILED_1,
                () -> JdbcUtils.deserialize(StringUtils.convertHexToBytes(BAD_OBJECT), null));

        assertNotNull(JdbcUtils.deserialize(StringUtils.convertHexToBytes(GOOD_OBJECT), null));
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
        protected synchronized Class<?> loadClass(String name, boolean resolve)
                throws ClassNotFoundException {
            if (name.equals(CLAZZ)) {
                usesThreadContextClassLoader = true;
            }
            return super.loadClass(name, resolve);
        }

    }

    public static final class GoodSampleObject implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    public static final class BadSampleObject implements Serializable {
        private static final long serialVersionUID = 1L;
    }
}
