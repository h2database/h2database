/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.doc;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;

import org.h2.util.StringUtils;

/**
 * This application merges the html documentation to one file
 * (onePage.html), so that the PDF document can be created.
 */
public class MergeDocs {

    String baseDir = "docs/html";

    public static void main(String[] args) throws Exception {
        new MergeDocs().run();
    }

    private void run() throws Exception {
        // the order of pages is important here
        String[] pages = { "quickstart.html", "installation.html", "tutorial.html", "features.html",
                "performance.html", "advanced.html", "grammar.html", "functions.html", "datatypes.html", "build.html",
                "history.html", "faq.html" };    
        StringBuffer buff = new StringBuffer();
        for (int i = 0; i < pages.length; i++) {
            String fileName = pages[i];
            String text = getContent(fileName);
            for (int j = 0; j < pages.length; j++) {
                text = StringUtils.replaceAll(text, pages[j] + "#", "#");
            }
            text = removeHeaderFooter(fileName, text);
            buff.append(text);
        }
        String finalText = buff.toString();
        File output = new File(baseDir, "onePage.html");
        PrintWriter writer = new PrintWriter(new FileWriter(output));
        writer.println("<html><head><meta http-equiv=\"Content-Type\" content=\"text/html;charset=utf-8\" /><title>");
        writer.println("H2 Documentation");
        writer.println("</title><link rel=\"stylesheet\" type=\"text/css\" href=\"stylesheetPdf.css\" /></head><body>");
        writer.println(finalText);
        writer.println("</body></html>");
        writer.close();
    }

    private String removeHeaderFooter(String fileName, String text) {
        // String start = "<body";
        // String end = "</body>";

        String start = "<div class=\"contentDiv\"";
        String end = "</div></td></tr></table><!-- analytics --></body></html>";

        int idx = text.indexOf(end);
        if (idx < 0) {
            throw new Error("Footer not found in file " + fileName);
        }
        text = text.substring(0, idx);
        idx = text.indexOf(start);
        idx = text.indexOf('>', idx);
        text = text.substring(idx + 1);
        return text;
    }

    String getContent(String fileName) throws Exception {
        File file = new File(baseDir, fileName);
        int length = (int) file.length();
        char[] data = new char[length];
        FileReader reader = new FileReader(file);
        int off = 0;
        while (length > 0) {
            int len = reader.read(data, off, length);
            off += len;
            length -= len;
        }
        reader.close();
        String s = new String(data);
        return s;
    }
}
