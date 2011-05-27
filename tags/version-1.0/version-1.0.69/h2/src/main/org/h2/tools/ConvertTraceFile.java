/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.sql.SQLException;

import org.h2.message.Message;
import org.h2.util.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.StringUtils;
import org.h2.util.Tool;

/**
 * Convert a trace file to a java class.
 * This is required because the find command truncates lines.
 */
public class ConvertTraceFile extends Tool {

    private void showUsage() {
        out.println("Converts a .trace.db file to a SQL script and Java source code.");
        out.println("java "+getClass().getName() + "\n" +
                " [-traceFile <file>]  The trace file name (default: test.trace.db)\n" +
                " [-script <file>]     The script file name (default: test.sql)\n" +
                " [-javaClass <file>]  The Java directory and class file name (default: Test)");
        out.println("See also http://h2database.com/javadoc/" + getClass().getName().replace('.', '/') + ".html");
    }

    /**
     * The command line interface for this tool. The options must be split into
     * strings like this: "-traceFile", "test.trace.db",... Options are case
     * sensitive. The following options are supported:
     * <ul>
     * <li>-help or -? (print the list of options) </li>
     * <li>-traceFile filename (the default is test.trace.db) </li>
     * <li>-script filename (the default is test.sql) </li>
     * <li>-javaClass className (the default is Test) </li>
     * </ul>
     * 
     * @param args the command line arguments
     * @throws Exception
     */
    public static void main(String[] args) throws SQLException {
        new ConvertTraceFile().run(args);
    }

    public void run(String[] args) throws SQLException {
        String traceFile = "test.trace.db";
        String javaClass = "Test";
        String script = "test.sql";
        for (int i = 0; args != null && i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("-traceFile")) {
                traceFile = args[++i];
            } else if (arg.equals("-javaClass")) {
                javaClass = args[++i];
            } else if (arg.equals("-script")) {
                script = args[++i];
            } else if (arg.equals("-help") || arg.equals("-?")) {
                showUsage();
                return;
            } else {
                out.println("Unsupported option: " + arg);
                showUsage();
                return;
            }
        }
        try {
            convertFile(traceFile, javaClass, script);
        } catch (IOException e) {
            throw Message.convertIOException(e, traceFile);
        }
    }

    /**
     * Converts a trace file to a Java class file and a script file.
     *
     * @param traceFileName
     * @param javaClassName
     * @throws IOException
     */
    private void convertFile(String traceFileName, String javaClassName, String script) throws IOException, SQLException {
        LineNumberReader reader = new LineNumberReader(IOUtils.getReader(FileUtils.openFileInputStream(traceFileName)));
        PrintWriter javaWriter = new PrintWriter(FileUtils.openFileWriter(javaClassName + ".java", false));
        PrintWriter scriptWriter = new PrintWriter(FileUtils.openFileWriter(script, false));
        javaWriter.println("import java.io.*;");
        javaWriter.println("import java.sql.*;");
        javaWriter.println("import java.math.*;");
        javaWriter.println("import java.util.Calendar;");
        String cn = javaClassName.replace('\\', '/');
        int idx = cn.lastIndexOf('/');
        if (idx > 0) {
            cn = cn.substring(idx + 1);
        }
        javaWriter.println("public class " + cn + " {");
        javaWriter.println("    public static void main(String[] args) throws Exception {");
        javaWriter.println("        Class.forName(\"org.h2.Driver\");");
        while (true) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }
            if (line.startsWith("/**/")) {
                line = "        " + line.substring(4);
                javaWriter.println(line);
            } else if (line.startsWith("/*SQL*/")) {
                line = line.substring("/*SQL*/".length());
                scriptWriter.println(StringUtils.javaDecode(line));
            }
        }
        javaWriter.println("    }");
        javaWriter.println("}");
        reader.close();
        javaWriter.close();
        scriptWriter.close();
    }

}
