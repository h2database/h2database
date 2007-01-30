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

import org.h2.tools.indexer.HtmlConverter;
import org.h2.util.IOUtils;
import org.h2.util.StringUtils;

public class PropertiesToUTF8 {
    
    public static void main(String[] args) throws Exception {
        File[] list = new File("bin/org/h2/server/web/res").listFiles();
        for(int i=0; list != null && i<list.length; i++) {
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
                s = HtmlConverter.convertStringToHtml(s);
                RandomAccessFile out = new RandomAccessFile(name.substring(4), "rw");
                out.write(s.getBytes());
                out.close();
            } else {
                new CheckTextFiles().checkOrFixFile(f, false, false);
                s = HtmlConverter.convertHtmlToString(s);
                // s = unescapeHtml(s);
                s = StringUtils.javaDecode(s);
                FileOutputStream out = new FileOutputStream("utf8" + f.getName());
                OutputStreamWriter w = new OutputStreamWriter(out, "UTF-8");
                w.write(s);
                w.close();
                out.close();
            }
        }
    }

}
