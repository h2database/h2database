/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.Properties;
import org.h2.constant.SysProperties;

/**
 * Command line tools implement the tool interface so that they can be used in
 * the H2 Console.
 */
public abstract class Tool {

    /**
     * The output stream where this tool writes to.
     */
    protected PrintStream out = System.out;

    private Properties resources;

    /**
     * Sets the standard output stream.
     *
     * @param out the new standard output stream
     */
    public void setOut(PrintStream out) {
        this.out = out;
    }

    /**
     * Run the tool with the given output stream and arguments.
     *
     * @param args the argument list
     */
    public abstract void run(String... args) throws SQLException;

    /**
     * Throw a SQLException saying this command line option is not supported.
     *
     * @param option the unsupported option
     * @return this method never returns normally
     */
    protected SQLException throwUnsupportedOption(String option) throws SQLException {
        showUsage();
        throw new SQLException("Unsupported option: " + option);
    }

    /**
     * Print to the output stream that no database files have been found.
     *
     * @param dir the directory or null
     * @param db the database name or null
     */
    protected void printNoDatabaseFilesFound(String dir, String db) {
        StringBuilder buff = new StringBuilder("No database files have been found");
        if (dir != null) {
            buff.append(" in directory ").append(dir);
        }
        if (db != null) {
            buff.append(" for the database ").append(db);
        }
        out.println(buff.toString());
    }

    /**
     * Print the usage of the tool. This method reads the description from the
     * resource file.
     */
    protected void showUsage() {
        if (resources == null) {
            resources = new Properties();
            String resourceName = "/org/h2/res/javadoc.properties";
            try {
                byte[] buff = Resources.get(resourceName);
                if (buff != null) {
                    resources.load(new ByteArrayInputStream(buff));
                }
            } catch (IOException e) {
                out.println("Cannot load " + resourceName);
            }
        }
        String className = getClass().getName();
        out.println(resources.get(className));
        out.println("Usage: java "+getClass().getName() + " <options>");
        out.println(resources.get(className + ".main"));
        out.println("See also http://h2database.com/javadoc/" + className.replace('.', '/') + ".html");
    }

    /**
     * Read an argument and check if it is true (1), false (-1), or not (0).
     * This method is used for compatibility with older versions only.
     *
     * @param args the list of arguments
     * @param i the index - 1
     * @return 1 for true, -1 for false, or 0 for not read
     */
    public static int readArgBoolean(String[] args, int i) {
        if (!SysProperties.OLD_COMMAND_LINE_OPTIONS) {
            return 0;
        }
        if (i + 1 < args.length) {
            String a = args[++i];
            if ("true".equals(a)) {
                return 1;
            } else if ("false".equals(a)) {
                return -1;
            }
        }
        return 0;
    }

}
