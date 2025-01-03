/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.lang.ProcessBuilder.Redirect;
import java.nio.ByteBuffer;

import org.h2.test.TestBase;
import org.h2.util.MemoryUnmapper;

/**
 * Tests memory unmapper.
 */
public class TestMemoryUnmapper extends TestBase {
    private static final int OK = 0, /* EXCEPTION = 1, */ UNAVAILABLE = 2;

    /**
     * May be used to run only this test and may be launched by this test in a
     * subprocess.
     *
     * @param a
     *            if empty run this test only
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    public static final class Tester {

        public static void main(String[] args) {
            ByteBuffer buffer = ByteBuffer.allocateDirect(10);
            System.exit(MemoryUnmapper.unmap(buffer) ? OK : UNAVAILABLE);
        }

    }

    @Override
    public void test() throws Exception {
        String className = Tester.class.getName();
        ProcessBuilder pb = new ProcessBuilder().redirectError(Redirect.INHERIT);
        // Test that unsafe unmapping is disabled by default
        pb.command(getJVM(), "-cp", getClassPath(), "-ea", className);
        assertEquals(UNAVAILABLE, pb.start().waitFor());
        // Test that it can be enabled
        pb.command(getJVM(), "-cp", getClassPath(), "-ea", "-Dh2.nioCleanerHack=true", className);
        assertEquals(OK, pb.start().waitFor());
        // Test that it will not be enabled with a security manager
        pb.command(getJVM(), "-cp", getClassPath(), "-ea", "-Djava.security.manager", "-Dh2.nioCleanerHack=true",
                className);
        assertEquals(UNAVAILABLE, pb.start().waitFor());
    }

}
