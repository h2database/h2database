/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools.code;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;

import org.h2.server.web.PageParser;
import org.h2.util.IOUtils;
import org.h2.util.StringUtils;

public class PropertiesToUTF8 {
    public static void main(String[] args) throws Exception {
        File[] list = new File("bin/org/h2/web/res").listFiles();
        for(int i=0; i<list.length; i++) {
            File f = list[i];
            if(!f.getName().endsWith(".properties")) {
                continue;
            }
            FileInputStream in = new FileInputStream(f);
            InputStreamReader r = new InputStreamReader(in, "UTF-8");
            String s = IOUtils.readStringAndClose(r, -1);
            in.close();
            String name = f.getName();
            if(name.startsWith("utf8")) {
                s = PageParser.escapeHtml(s, false);
                RandomAccessFile out = new RandomAccessFile(name.substring(4), "rw");
                out.write(s.getBytes());
                out.close();
            } else {
                new CheckTextFiles().checkOrFixFile(f, false, false);
                s = unescapeHtml(s);
                s = StringUtils.javaDecode(s);
                FileOutputStream out = new FileOutputStream("utf8" + f.getName());
                OutputStreamWriter w = new OutputStreamWriter(out, "UTF-8");
                w.write(s);
                w.close();
                out.close();
            }
        }
    }

    private static String unescapeHtml(String s) {
        String codes = "&lt; < &amp; & &gt; > &Auml; \u00c4 &Ouml; \u00d6 &Uuml; \u00dc &auml; \u00e4 &ouml; \u00f6 &uuml; \u00fc &ntilde; \u00f1 &oacute; \u00f3 &Iacute; \u00cd &ccedil; \u00e7 &eagrave; \u00e8 &ecirc; \u00ea &Uacute; \u00da &aacute; \u00e1 &uacute; \u00fa &eacute; \u00e9 &egrave; \u00e8 &icirc; \u00ee";
        String[] list = StringUtils.arraySplit(codes, ' ', false);
        for(int i=0; i<list.length; i+=2) {
            s = StringUtils.replaceAll(s, list[i], list[i+1]);
        }
        if(s.indexOf("&") >= 0) {
            throw new Error("??? " + s);
        }
        return s;
    }
}
