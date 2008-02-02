/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools.doc;

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
        new MergeDocs().run(args);
    }

    private void run(String[] args) throws Exception {
        String[] pages = { "quickstartText.html", "installation.html", "tutorial.html", "features.html",
                "performance.html", "advanced.html", "grammar.html", "functions.html", "datatypes.html", "build.html",
                "history.html", "faq.html", "license.html" };
        StringBuffer buff = new StringBuffer();
        for (int i = 0; i < pages.length; i++) {
            String text = getContent(pages[i]);
            for (int j = 0; j < pages.length; j++) {
                text = StringUtils.replaceAll(text, pages[j] + "#", "#");
            }
            text = removeHeaderFooter(text);
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

    private String removeHeaderFooter(String text) {
        // String start = "<body";
        // String end = "</body>";

        String start = "<div class=\"contentDiv\"";
        String end = "</div></td></tr></table></body></html>";

        int idx = text.indexOf(end);
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
