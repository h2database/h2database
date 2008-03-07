/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (license2)
 * Initial Developer: H2 Group
 */
package org.h2.tools.doc;

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

    public static void main(String[] args) throws Exception {
        new GenerateDoc().run(args);
    }

    String inDir = "src/docsrc/html";
    String outDir = "docs/html";
    Connection conn;
    HashMap session = new HashMap();
    Bnf bnf;

    void run(String[] args) throws Exception {
        System.out.println(getClass().getName());
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
        process("grammar");
        process("functions");
        process("datatypes");
        conn.close();
    }

    void process(String fileName) throws Exception {
        FileOutputStream out = new FileOutputStream(outDir + "/" + fileName + ".html");
        FileInputStream in = new FileInputStream(inDir + "/" + fileName + ".jsp");
        byte[] bytes = IOUtils.readBytesAndClose(in, 0);
        String page = new String(bytes);
        page = PageParser.parse(null, page, session);
        out.write(page.getBytes());
        out.close();
    }

    void map(String key, String sql) throws Exception {
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
                syntax = bnf.getSyntaxHtml(topic, syntax);
                map.put("syntax", syntax);
                String link = topic.toLowerCase();
                link = StringUtils.replaceAll(link, " ", "");
                link = StringUtils.replaceAll(link, "_", "");
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
