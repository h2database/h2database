/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import org.h2.test.TestBase;
import org.h2.tools.Shell;
import org.h2.util.Task;

/**
 * Test the shell tool.
 */
public class TestShell extends TestBase {

    /**
     * The output stream of the tool.
     */
    PrintStream toolOut;

    /**
     * The input stream of the tool.
     */
    InputStream toolIn;

    private PrintStream testOut;
    private PipedInputStream testIn;
    private LineNumberReader lineReader;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        Shell shell = new Shell();
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        shell.setOut(new PrintStream(buff));
        shell.runTool("-url", "jdbc:h2:mem:", "-driver", "org.h2.Driver",
                "-user", "sa", "-password", "sa", "-properties", "null", "-sql", "select 'Hello ' || 'World' as hi");
        String s = new String(buff.toByteArray());
        assertContains(s, "HI");
        assertContains(s, "Hello World");
        assertContains(s, "(1 row, ");
        test(true);
        test(false);
    }

    private void test(final boolean commandLineArgs) throws IOException {
        testIn = new PipedInputStream();
        PipedOutputStream out = new PipedOutputStream(testIn);
        toolOut = new PrintStream(out, true);
        out = new PipedOutputStream();
        testOut = new PrintStream(out, true);
        toolIn = new PipedInputStream(out);
        Task task = new Task() {
            public void call() throws Exception {
                try {
                    Shell shell = new Shell();
                    shell.setIn(toolIn);
                    shell.setOut(toolOut);
                    shell.setErr(toolOut);
                    if (commandLineArgs) {
                        shell.runTool("-url", "jdbc:h2:mem:",
                                "-user", "sa", "-password", "sa");
                    } else {
                        shell.runTool();
                    }
                } finally {
                    toolOut.close();
                }
            }
        };
        task.execute();
        InputStreamReader reader = new InputStreamReader(testIn);
        lineReader = new LineNumberReader(reader);
        read("");
        read("Welcome to H2 Shell");
        read("Exit with");
        if (!commandLineArgs) {
            read("[Enter]");
            testOut.println("jdbc:h2:mem:");
            read("URL");
            testOut.println("");
            read("Driver");
            testOut.println("sa");
            read("User");
            testOut.println("sa");
            read("Password");
        }
        read("Commands are case insensitive");
        read("help or ?");
        read("list");
        read("maxwidth");
        read("show");
        read("describe");
        read("autocommit");
        read("history");
        read("quit or exit");
        read("");
        testOut.println("history");
        read("sql> No history");
        testOut.println("1");
        read("sql> Not found");
        testOut.println("select 1 a;");
        read("sql> A");
        read("1");
        read("(1 row,");
        testOut.println("history");
        read("sql> #1: select 1 a");
        read("To re-run a statement, type the number and press and enter");
        testOut.println("1");
        read("sql> select 1 a");
        read("A");
        read("1");
        read("(1 row,");

        testOut.println("select 'x' || space(1000) large, 'y' small;");
        read("sql> LARGE");
        read("x");
        read("(data is partially truncated)");
        read("(1 row,");
        testOut.println("select error;");
        read("sql> Error:");
        if (read("").startsWith("Column \"ERROR\" not found")) {
            read("");
        }
        testOut.println("create table test(id int primary key, name varchar)\n;");
        read("sql> ...>");
        testOut.println("show public");
        read("sql>");
        while (read("").startsWith("INFORMATION_SCHEMA")) {
            // ignore
        }
        testOut.println("insert into test values(1, 'Hello');");
        read("sql>");
        testOut.println("select * from test;");
        read("sql> ID");
        read("1 ");
        read("(1 row,");
        testOut.println("maxwidth");
        read("sql> Usage: maxwidth <integer value>");
        read("Maximum column width is now 100");
        testOut.println("maxwidth 80");
        read("sql> Maximum column width is now 80");
        testOut.println("autocommit");
        read("sql> Usage: autocommit [true|false]");
        read("Autocommit is now true");
        testOut.println("autocommit false");
        read("sql> Autocommit is now false");
        testOut.println("autocommit true");
        read("sql> Autocommit is now true");
        testOut.println("describe");
        read("sql> Usage: describe [<schema name>.]<table name>");
        testOut.println("describe test");
        read("sql> Column Name");
        read("ID");
        read("NAME");
        testOut.println("describe public.test");
        read("sql> Column Name");
        read("ID");
        read("NAME");
        testOut.println("\n;");
        read("sql>");
        testOut.println("list");
        read("sql> Result list mode is now on");
        testOut.println("select 1 a, 2 b;");
        read("sql> A: 1");
        read("B: 2");
        read("(1 row, ");
        testOut.println("list");
        read("sql> Result list mode is now off");
        testOut.println("help");
        read("sql> Commands are case insensitive");
        read("help or ?");
        read("list");
        read("maxwidth");
        read("show");
        read("describe");
        read("autocommit");
        read("history");
        read("quit or exit");
        read("");
        testOut.println("exit");
        read("sql>");
        task.get();
    }

    private String read(String expectedStart) throws IOException {
        String line = lineReader.readLine();
        // System.out.println(": " + line);
        assertStartsWith(line, expectedStart);
        return line;
    }

}
