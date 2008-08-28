/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.doc;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import org.h2.bnf.Bnf;
import org.h2.engine.Constants;
import org.h2.server.web.PageParser;
import org.h2.util.IOUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.StringUtils;

/**
 * This application generates sections of the documentation
 * by converting the built-in help section (INFORMATION_SCHEMA.HELP)
 * to cross linked html.
 */
public class GenerateDoc {

    private String inDir = "src/docsrc/html";
    private String outDir = "docs/html";
    private Connection conn;
    private HashMap session = new HashMap();
    private Bnf bnf;

    /**
     * This method is called when executing this application from the command
     * line.
     * 
     * @param args the command line parameters
     */
    public static void main(String[] args) throws Exception {
        new GenerateDoc().run(args);
    }

    private void run(String[] args) throws Exception {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-in")) {
                inDir = args[++i];
            } else if (args[i].equals("-out")) {
                outDir = args[++i];
            }
        }
        Class.forName("org.h2.Driver");
        conn = DriverManager.getConnection("jdbc:h2:mem:");
        new File(outDir).mkdirs();
        bnf = Bnf.getInstance(null);
        session.put("version", Constants.getVersion());
        session.put("versionDate", Constants.BUILD_DATE);
        session.put("previousVersion", Constants.getVersionPrevious());
        session.put("previousVersionDate", Constants.BUILD_DATE_PREVIOUS);
        map("commands", "SELECT * FROM INFORMATION_SCHEMA.HELP WHERE SECTION LIKE 'Commands%' ORDER BY ID");
        map("commandsDML", "SELECT * FROM INFORMATION_SCHEMA.HELP WHERE SECTION='Commands (DML)' ORDER BY ID");
        map("commandsDDL", "SELECT * FROM INFORMATION_SCHEMA.HELP WHERE SECTION='Commands (DDL)' ORDER BY ID");
        map("commandsOther", "SELECT * FROM INFORMATION_SCHEMA.HELP WHERE SECTION='Commands (Other)' ORDER BY ID");
        map("otherGrammar", "SELECT * FROM INFORMATION_SCHEMA.HELP WHERE SECTION='Other Grammar' ORDER BY ID");
        map("functionsAggregate",
                "SELECT * FROM INFORMATION_SCHEMA.HELP WHERE SECTION = 'Functions (Aggregate)' ORDER BY ID");
        map("functionsNumeric",
                "SELECT * FROM INFORMATION_SCHEMA.HELP WHERE SECTION = 'Functions (Numeric)' ORDER BY ID");
        map("functionsString", "SELECT * FROM INFORMATION_SCHEMA.HELP WHERE SECTION = 'Functions (String)' ORDER BY ID");
        map("functionsTimeDate",
                "SELECT * FROM INFORMATION_SCHEMA.HELP WHERE SECTION = 'Functions (Time and Date)' ORDER BY ID");
        map("functionsSystem", "SELECT * FROM INFORMATION_SCHEMA.HELP WHERE SECTION = 'Functions (System)' ORDER BY ID");
        map("functionsAll",
                "SELECT * FROM INFORMATION_SCHEMA.HELP WHERE SECTION LIKE 'Functions%' ORDER BY SECTION, ID");
        map("dataTypes", "SELECT * FROM INFORMATION_SCHEMA.HELP WHERE SECTION LIKE 'Data Types%' ORDER BY SECTION, ID");
        map("informationSchema", "SELECT TABLE_NAME TOPIC, GROUP_CONCAT(COLUMN_NAME " + 
                "ORDER BY ORDINAL_POSITION SEPARATOR ', ') SYNTAX FROM INFORMATION_SCHEMA.COLUMNS " +
                "WHERE TABLE_SCHEMA='INFORMATION_SCHEMA' GROUP BY TABLE_NAME ORDER BY TABLE_NAME");
        processAll("");
        conn.close();
    }
    
    private void processAll(String dir) throws Exception {
        if (dir.endsWith(".svn")) {
            return;
        }
        File[] list = new File(inDir + "/" + dir).listFiles();
        for (int i = 0; i < list.length; i++) {
            File file = list[i];
            if (file.isDirectory()) {
                processAll(dir + file.getName());
            } else {
                process(dir, file.getName());
            }
        }
    }

    private void process(String dir, String fileName) throws Exception {
        String inFile = inDir + "/" + dir + "/" + fileName;
        String outFile = outDir + "/" + dir + "/" + fileName;
        new File(outFile).getParentFile().mkdirs();
        FileOutputStream out = new FileOutputStream(outFile);
        FileInputStream in = new FileInputStream(inFile);
        byte[] bytes = IOUtils.readBytesAndClose(in, 0);
        if (fileName.endsWith(".html")) {
            String page = new String(bytes);
            page = PageParser.parse(page, session);
            bytes = page.getBytes();
        }
        out.write(bytes);
        out.close();
    }

    private void map(String key, String sql) throws Exception {
        ResultSet rs = null;
        Statement stat = null;
        try {
            stat = conn.createStatement();
            rs = stat.executeQuery(sql);
            ArrayList list = new ArrayList();
            while (rs.next()) {
                HashMap map = new HashMap();
                ResultSetMetaData meta = rs.getMetaData();
                for (int i = 0; i < meta.getColumnCount(); i++) {
                    String k = StringUtils.toLowerEnglish(meta.getColumnLabel(i + 1));
                    String value = rs.getString(i + 1);
                    map.put(k, PageParser.escapeHtml(value));
                }
                String topic = rs.getString("TOPIC");
                String syntax = rs.getString("SYNTAX");
                syntax = PageParser.escapeHtml(syntax);
                syntax = StringUtils.replaceAll(syntax, "<br />", "");
                syntax = bnf.getSyntaxHtml(syntax);
                map.put("syntax", syntax);
                String link = topic.toLowerCase();
                link = StringUtils.replaceAll(link, " ", "");
                link = StringUtils.replaceAll(link, "_", "");
                link = StringUtils.replaceAll(link, "@", "");
                map.put("link", StringUtils.urlEncode(link));
                list.add(map);
            }
            session.put(key, list);
        } finally {
            JdbcUtils.closeSilently(rs);
            JdbcUtils.closeSilently(stat);
        }
    }
}
