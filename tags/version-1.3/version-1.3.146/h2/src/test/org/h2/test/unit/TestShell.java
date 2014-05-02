/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.sql.SQLException;

import org.h2.test.TestBase;
import org.h2.tools.Shell;

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

    public void test() throws IOException {
        testIn = new PipedInputStream();
        PipedOutputStream out = new PipedOutputStream(testIn);
        toolOut = new PrintStream(out, true);
        out = new PipedOutputStream();
        testOut = new PrintStream(out, true);
        toolIn = new PipedInputStream(out);
        new Thread(new Runnable() {
            public void run() {
                try {
                    Shell shell = new Shell();
                    shell.setIn(toolIn);
                    shell.setOut(toolOut);
                    shell.setErr(toolOut);
                    shell.runTool();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    toolOut.close();
                }
            }
        }).start();
        InputStreamReader reader = new InputStreamReader(testIn);
        lineReader = new LineNumberReader(reader);
        read("");
        read("Welcome to H2 Shell");
        read("Exit with");
        read("[Enter]");
        testOut.println("jdbc:h2:mem:");
        read("URL");
        testOut.println("org.h2.Driver");
        read("Driver");
        testOut.println("sa");
        read("User");
        testOut.println("sa");
        read("Password");
        read("Commands are case insensitive");
        read("help or ?");
        read("list");
        read("maxwidth");
        read("show");
        read("describe");
        read("history");
        read("quit or exit");
        read("");
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
        testOut.println("describe test");
        read("sql> Column Name");
        read("ID");
        read("NAME");
        testOut.println("exit");
        read("sql>");
    }

    private String read(String expectedStart) throws IOException {
        String line = lineReader.readLine();
        // System.out.println(": " + line);
        assertStartsWith(line, expectedStart);
        return line;
    }

}
