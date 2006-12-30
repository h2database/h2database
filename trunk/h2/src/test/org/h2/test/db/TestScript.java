/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.h2.test.TestAll;
import org.h2.test.TestBase;
import org.h2.util.StringUtils;

public class TestScript extends TestBase {

    private boolean failFast;

    private boolean alwaysReconnect;
    private Connection conn;
    private Statement stat;
    private LineNumberReader in;
    private int line;
    private PrintStream out;
    private ArrayList result = new ArrayList();
    private String putback;
    private StringBuffer errors;
    private ArrayList statements;
    private String fileName = "org/h2/test/test.in.txt";

    public ArrayList getAllStatements(TestAll conf, String file) throws Exception {
        config = conf;
        fileName = file;
        statements = new ArrayList();
        test();
        return statements;
    }

    public void test() throws Exception {
        if(config.networked && config.big) {
            return;
        }
        alwaysReconnect = false;
        testScript();
        if(!config.memory) {
            if(config.big) {
                alwaysReconnect = true;
                testScript();
            }
        }
    }

    public void testScript() throws Exception {
        deleteDb("script");
        String outfile = "test.out.txt";
        String infile = fileName;
        conn = getConnection("script");
        stat = conn.createStatement();
        out = new PrintStream(new FileOutputStream(outfile));
        errors = new StringBuffer();
        testFile(infile);
        conn.close();
        out.close();
        if(errors.length()>0) {
            throw new Exception("errors:\n" + errors.toString());
        } else {
            new File(outfile).delete();
        }
    }

    private String readLine() throws IOException {
        if (putback != null) {
            String s = putback;
            putback = null;
            return s;
        }
        while(true) {
            String s = in.readLine();
            if(s==null) {
                return s;
            }
            s = s.trim();
            if(s.length() > 0) {
                return s;
            }
        }
    }

    private void testFile(String infile) throws Exception {
        InputStream is = getClass().getClassLoader().getResourceAsStream(infile);
        in = new LineNumberReader(new InputStreamReader(is, "Cp1252"));
        StringBuffer buff = new StringBuffer();
        while (true) {
            String sql = readLine();
            if (sql == null) {
                break;
            }
            if (sql.startsWith("--")) {
                write(sql);
            } else if (sql.startsWith(">")) {
                // do nothing
            } else if (sql.endsWith(";")) {
                write(sql);
                buff.append(sql.substring(0, sql.length() - 1));
                sql = buff.toString();
                buff = new StringBuffer();
                process(sql);
            } else {
                write(sql);
                buff.append(sql);
                buff.append('\n');
            }
        }
    }

    private boolean containsTempTables() throws SQLException {
        ResultSet rs = conn.getMetaData().getTables(null, null, null, new String[]{"TABLE"});
        while(rs.next()) {
            String sql = rs.getString("SQL");
            if(sql != null) {
                if(sql.indexOf("TEMPORARY") >= 0) {
                    return true;
                }
            }
        }
        return false;
    }

    private void process(String sql) throws Exception {
        if(alwaysReconnect) {
            if(!containsTempTables()) {
                boolean autocommit = conn.getAutoCommit();
                if(autocommit) {
                    conn.close();
                    conn = getConnection("script");
                    conn.setAutoCommit(autocommit);
                    stat = conn.createStatement();
                }
            }
        }
        if(statements != null) {
            statements.add(sql);
        }
        if (sql.indexOf('?') == -1) {
            processStatement(sql);
        } else {
            String param = readLine();
            write(param);
            if (!param.equals("{")) {
                throw new Error("expected '{', got " + param + " in " + sql);
            }
            try {
                PreparedStatement prep = conn.prepareStatement(sql);
                int count = 0;
                while (true) {
                    param = readLine();
                    write(param);
                    if (param.startsWith("}")) {
                        break;
                    }
                    count += processPrepared(sql, prep, param);
                }
                writeResult("update count: " + count, null);
            } catch (SQLException e) {
                writeException(e);
            }
        }
        write("");
    }

    private void setParameter(PreparedStatement prep, int i, String param)
            throws SQLException {
        if (param.equalsIgnoreCase("null")) {
            param = null;
        }
        prep.setString(i, param);
    }

    private int processPrepared(String sql, PreparedStatement prep, String param)
            throws Exception {
        try {
            StringBuffer buff = new StringBuffer();
            int index = 0;
            for (int i = 0; i < param.length(); i++) {
                char c = param.charAt(i);
                if (c == ',') {
                    setParameter(prep, ++index, buff.toString());
                    buff = new StringBuffer();
                } else if (c == '"') {
                    while (true) {
                        c = param.charAt(++i);
                        if (c == '"') {
                            break;
                        }
                        buff.append(c);
                    }
                } else if (c > ' ') {
                    buff.append(c);
                }
            }
            if (buff.length() > 0) {
                setParameter(prep, ++index, buff.toString());
            }
            if (prep.execute()) {
                writeResultSet(sql, prep.getResultSet());
                return 0;
            }
            return prep.getUpdateCount();
        } catch (SQLException e) {
            writeException(e);
            return 0;
        }
    }

    private int processStatement(String sql) throws Exception {
        try {
            if (stat.execute(sql)) {
                writeResultSet(sql, stat.getResultSet());
            } else {
                int count = stat.getUpdateCount();
                writeResult(count < 1 ? "ok" : "update count: " + count, null);
            }
        } catch (SQLException e) {
            writeException(e);
        }
        return 0;
    }
    
    private String formatString(String s) {
        if (s== null) {
            return "null";
        }
        return s.replace('\n', ' ');
    }

    private void writeResultSet(String sql, ResultSet rs) throws Exception {
        boolean ordered = StringUtils.toLowerEnglish(sql).indexOf("order by") >= 0;
        ResultSetMetaData meta = rs.getMetaData();
        int len = meta.getColumnCount();
        int[] max = new int[len];
        String[] head = new String[len];
        for (int i = 0; i < len; i++) {
            String label = formatString(meta.getColumnLabel(i + 1));
            max[i] = label.length();
            head[i] = label;
        }
        result.clear();
        while (rs.next()) {
            String[] row = new String[len];
            for (int i = 0; i < len; i++) {
                String data = formatString(rs.getString(i + 1));
                if (max[i] < data.length()) {
                    max[i] = data.length();
                }
                row[i] = data;
            }
            result.add(row);
        }
        rs.close();
        writeResult(format(head, max), null);
        writeResult(format(null, max), null);
        String[] array = new String[result.size()];
        for (int i = 0; i < result.size(); i++) {
            array[i] = format((String[]) result.get(i), max);
        }
        if (!ordered) {
            sort(array);
        }
        int i = 0;
        for (; i < array.length; i++) {
            writeResult(array[i], null);
        }
        writeResult((ordered ? "rows (ordered): " : "rows: ") + i, null);
    }

    private String format(String[] row, int[] max) throws Exception {
        int length = max.length;
        StringBuffer buff = new StringBuffer();
        for (int i = 0; i < length; i++) {
            if(i>0) {
                buff.append(' ');
            }
            if (row == null) {
                for (int j = 0; j < max[i]; j++) {
                    buff.append('-');
                }
            } else {
                int len = row[i].length();
                buff.append(row[i]);
                if(i < length - 1) {
                    for (int j = len; j < max[i]; j++) {
                        buff.append(' ');
                    }
                }
            }
        }
        return buff.toString();
    }

    private void writeException(SQLException e) throws Exception {
        writeResult("exception", e);
    }

    private void writeResult(String s, SQLException e) throws Exception {
        checkNotGeneralException(e);
        s = ("> " + s).trim();
        String compare = readLine();
        if (compare != null && compare.startsWith(">")) {
            if (!compare.equals(s)) {
                errors.append("line: ");
                errors.append(line);
                errors.append("\nexp: ");
                errors.append(compare);
                errors.append("\ngot: ");
                errors.append(s);
                errors.append("\n");
                if(e!=null) {
                    e.printStackTrace();
                }
                if(failFast) {
                    new Exception(errors.toString()).printStackTrace();
                    conn.close();
                    System.exit(1);
                }
            }
        } else {
            putback = compare;
        }
        write(s);

    }

    private void write(String s) throws Exception {
        line++;
        out.println(s);
    }

    private void sort(String[] a) {
        for (int i = 1, j, len = a.length; i < len; i++) {
            String t = a[i];
            for (j = i - 1; j >= 0 && t.compareTo(a[j]) < 0; j--) {
                a[j + 1] = a[j];
            }
            a[j + 1] = t;
        }
    }

}
