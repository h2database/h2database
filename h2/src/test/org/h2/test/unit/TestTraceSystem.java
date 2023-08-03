/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.message.TraceSystem;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.util.Utils10;

/**
 * Tests the trace system
 */
public class TestTraceSystem extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws Exception {
        testTraceDebug();
        testReadOnly();
        testAdapter();
        testInvalidLevel();
    }

    private void testAdapter() {
        TraceSystem ts = new TraceSystem(null);
        ts.setName("test");
        ts.setLevelFile(TraceSystem.ADAPTER);
        ts.getTrace("test").debug("test");
        ts.getTrace("test").info("test");
        ts.getTrace("test").error(new Exception(), "test");

        // The used SLF4J-nop logger has all log levels disabled,
        // so this should be reflected in the trace system.
        assertFalse(ts.isEnabled(TraceSystem.INFO));
        assertFalse(ts.getTrace("test").isInfoEnabled());

        ts.close();
    }

    private void testTraceDebug() throws Exception {
        TraceSystem ts = new TraceSystem(null);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ts.setSysOut(new PrintStream(out, false, "UTF-8"));
        ts.setLevelSystemOut(TraceSystem.DEBUG);
        ts.getTrace("test").debug(new Exception("error"), "test");
        ts.close();
        String outString = Utils10.byteArrayOutputStreamToString(out, StandardCharsets.UTF_8);
        assertContains(outString, "error");
        assertContains(outString, "Exception");
        assertContains(outString, "test");
    }

    private void testReadOnly() throws Exception {
        String readOnlyFile = getBaseDir() + "/readOnly.log";
        FileUtils.delete(readOnlyFile);
        FileUtils.newOutputStream(readOnlyFile, false).close();
        FileUtils.setReadOnly(readOnlyFile);
        TraceSystem ts = new TraceSystem(readOnlyFile);
        ts.setLevelFile(TraceSystem.INFO);
        ts.getTrace("test").info("test");
        FileUtils.delete(readOnlyFile);
        ts.close();
    }

    private void testInvalidLevel() {
        TraceSystem ts = new TraceSystem(null);
        testInvalidLevel(ts, false, TraceSystem.PARENT - 1);
        testInvalidLevel(ts, false, TraceSystem.ADAPTER);
        testInvalidLevel(ts, false, TraceSystem.ADAPTER + 1);
        testInvalidLevel(ts, true, TraceSystem.PARENT - 1);
        testInvalidLevel(ts, true, TraceSystem.ADAPTER + 1);
        ts.close();
    }

    private void testInvalidLevel(TraceSystem ts, boolean file, int level) {
        try {
            if (file) {
                ts.setLevelFile(level);
            } else {
                ts.setLevelSystemOut(level);
            }
            fail("Expected DbException: 90008");
        } catch (DbException ex) {
            assertEquals(ErrorCode.INVALID_VALUE_2, ex.getErrorCode());
        }
    }

}
