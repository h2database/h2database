/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.h2.test.TestBase;
import org.h2.tools.DeleteDbFiles;
import org.h2.util.StringUtils;

/**
 * Tests the sample apps.
 */
public class TestSampleApps extends TestBase {

    public void test() throws Exception {
        deleteDb("optimizations");
        String url = "jdbc:h2:" + baseDir + "/optimizations";
        testApp(org.h2.tools.RunScript.class, new String[] { "-url", url, "-user", "sa", "-password", "sa", "-script",
                "src/test/org/h2/samples/optimizations.sql", "-checkResults" }, "");
        deleteDb("optimizations");

        testApp(org.h2.samples.Compact.class, null, "Compacting...\nDone.");
        testApp(org.h2.samples.CsvSample.class, null, "NAME: Bob Meier\n" + "EMAIL: bob.meier@abcde.abc\n"
                + "PHONE: +41123456789\n\n" + "NAME: John Jones\n" + "EMAIL: john.jones@abcde.abc\n"
                + "PHONE: +41976543210\n");
        testApp(org.h2.samples.Function.class, null,
                "2 is prime\n3 is prime\n5 is prime\n7 is prime\n11 is prime\n13 is prime\n17 is prime\n19 is prime");
        // Not compatible with PostgreSQL JDBC driver (throws a NullPointerException)
        //testApp(org.h2.samples.SecurePassword.class, null, "Joe");
        // TODO test ShowProgress (percent numbers are hardware specific)
        // TODO test ShutdownServer (server needs to be started in a separate
        // process)
        testApp(org.h2.samples.TriggerSample.class, null, "The sum is 20.00");

        // tools
        testApp(org.h2.tools.ChangeFileEncryption.class, new String[] { "-help" },
                "Allows changing the database file encryption password or algorithm*");
        testApp(org.h2.tools.ChangeFileEncryption.class, null,
                "Allows changing the database file encryption password or algorithm*");
        testApp(org.h2.tools.DeleteDbFiles.class, new String[] { "-help" },
                "Deletes all files belonging to a database.*");
    }

    private void testApp(Class clazz, String[] args, String expected) throws Exception {
        DeleteDbFiles.execute("data", "test", true);
        Method m = clazz.getMethod("main", new Class[] { String[].class });
        PrintStream oldOut = System.out, oldErr = System.err;
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(buff, false, "UTF-8");
        System.setOut(out);
        System.setErr(out);
        try {
            m.invoke(null, new Object[] { args });
        } catch (InvocationTargetException e) {
            TestBase.logError("error", e.getTargetException());
        } catch (Throwable e) {
            TestBase.logError("error", e);
        }
        out.flush();
        System.setOut(oldOut);
        System.setErr(oldErr);
        String s = new String(buff.toByteArray(), "UTF-8");
        s = StringUtils.replaceAll(s, "\r\n", "\n");
        s = s.trim();
        expected = expected.trim();
        if (expected.endsWith("*")) {
            expected = expected.substring(0, expected.length() - 1);
            if (!s.startsWith(expected)) {
                assertEquals(s.trim(), expected.trim());
            }
        } else {
            assertEquals(s.trim(), expected.trim());
        }
    }
}
