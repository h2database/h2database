/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import org.h2.tools.RunScript;
import org.h2.util.StringUtils;

public class Newsfeed {
    public static void main(String[] args) throws Exception {
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection("jdbc:h2:mem:", "sa", "");
        InputStream in = Newsfeed.class.getResourceAsStream("newsfeed.sql");
        ResultSet rs = RunScript.execute(conn, new InputStreamReader(in, "ISO-8859-1"));
        while(rs.next()) {
            String file = rs.getString("FILE");
            String content = rs.getString("CONTENT");
            if(file.equals("-newsletter-")) {
                System.out.println(convertHtml2Text(content));
            } else {
                FileOutputStream out = new FileOutputStream(file);
                Writer writer = new OutputStreamWriter(out, "UTF-8");
                writer.write(content);
                writer.close();
                out.close();
            }
        }
        conn.close();
    }

    private static String convertHtml2Text(String html) {
        String s = html;
        s = StringUtils.replaceAll(s, "<b>", "");
        s = StringUtils.replaceAll(s, "</b>", "");
        s = StringUtils.replaceAll(s, "<ul>", "");
        s = StringUtils.replaceAll(s, "</ul>", "");
        s = StringUtils.replaceAll(s, "<li>", "- ");
        s = StringUtils.replaceAll(s, "<a href=\"", "( ");
        s = StringUtils.replaceAll(s, "\">", " ) ");
        s = StringUtils.replaceAll(s, "</a>", "");
        s = StringUtils.replaceAll(s, "<br>", "");
        if(s.indexOf('<') >= 0 || s.indexOf('>') >= 0) {
            throw new Error("Unsupported HTML Tag: < or > in " + s);
        }
        return s;
    }
}
