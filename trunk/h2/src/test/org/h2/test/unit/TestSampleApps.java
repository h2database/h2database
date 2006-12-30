/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Method;

import org.h2.test.TestBase;
import org.h2.tools.DeleteDbFiles;
import org.h2.util.StringUtils;

public class TestSampleApps extends TestBase {

    public void test() throws Exception {
        testApp(org.h2.samples.Compact.class, null, "Compacting...\nDone.");
        testApp(org.h2.samples.CsvSample.class, null, 
                "NAME: Bob Meier\n" 
                + "EMAIL: bob.meier@abcde.fgh\n"
                +"PHONE: +41123456789\n\n"
                +"NAME: John Jones\n"
                +"EMAIL: johnjones@abcde.fgh\n"
                +"PHONE: +41976543210\n");
        testApp(org.h2.samples.Function.class, null, 
                "2 is prime\n3 is prime\n5 is prime\n7 is prime\n11 is prime\n13 is prime\n17 is prime\n19 is prime");
        testApp(org.h2.samples.SecurePassword.class, null,  "Hello");
        // TODO test ShowProgress (percent numbers are hardware specific)
        // TODO test ShutdownServer (server needs to be started in a separater process)
        testApp(org.h2.samples.TriggerSample.class, null, "The sum is 20.00");
        
        // tools
        testApp(org.h2.tools.ChangePassword.class, new String[]{"-?"}, "java org.h2.tools.ChangePassword [-dir <dir>] "
                + "[-db <database>] [-cipher <cipher>] [-decrypt <pwd>] [-encrypt <pwd>] [-quiet]");
        testApp(org.h2.tools.ChangePassword.class, null, "java org.h2.tools.ChangePassword [-dir <dir>] "
                + "[-db <database>] [-cipher <cipher>] [-decrypt <pwd>] [-encrypt <pwd>] [-quiet]");
        testApp(org.h2.tools.DeleteDbFiles.class, new String[]{"-?"}, "java org.h2.tools.DeleteDbFiles [-dir <dir>] [-db <database>] [-quiet]");
    }

    private void testApp(Class clazz, String[] args, String expected) throws Exception {
        DeleteDbFiles.execute("data", "test", true);
        Method m = clazz.getMethod("main", new Class[]{String[].class});
        PrintStream oldOut = System.out, oldErr = System.err;
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(buff, false, "UTF-8");
        System.setOut(out);
        System.setErr(out);
        try {
            m.invoke(null, new Object[]{args});
        } catch(Throwable e) {
            System.out.print("EXCEPTION");
            e.printStackTrace();
        }
        out.flush();
        System.setOut(oldOut);
        System.setErr(oldErr);
        String s = new String(buff.toByteArray(), "UTF-8");
        s = StringUtils.replaceAll(s, "\r\n", "\n");
        check(s.trim(), expected.trim());
    }
}
