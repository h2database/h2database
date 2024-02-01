/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.doc;

import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.h2.engine.Constants;
import org.h2.util.StringUtils;

/**
 * This application merges the html documentation to one file
 * (onePage.html), so that the PDF document can be created.
 */
public class MergeDocs {

    private static final String BASE_DIR = "docs/html";

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        // the order of pages is important here
        String[] pages = { "quickstart.html", "installation.html",
                "tutorial.html", "features.html", "security.html", "performance.html",
                "advanced.html", "commands.html",
                "functions.html", "functions-aggregate.html", "functions-window.html",
                "datatypes.html", "grammar.html", "systemtables.html",
                "build.html", "history.html", "faq.html" };
        StringBuilder buff = new StringBuilder();
        for (String fileName : pages) {
            String text = getContent(fileName);
            for (String page : pages) {
                text = StringUtils.replaceAll(text, page + "#", "#");
            }
            text = disableRailroads(text);
            text = removeHeaderFooter(fileName, text);
            text = fixLinks(text);
            text = fixTableBorders(text);
            text = addLegacyFontTag(fileName, text);
            buff.append(text);
        }
        String finalText = buff.toString();
        PrintWriter writer = new PrintWriter(Files.newBufferedWriter(Paths.get(BASE_DIR, "onePage.html")));
        writer.println("<html><head><meta http-equiv=\"Content-Type\" " +
                "content=\"text/html;charset=utf-8\" /><title>");
        writer.println("H2 Documentation");
        writer.println("</title><link rel=\"stylesheet\" type=\"text/css\" " +
                "href=\"stylesheetPdf.css\" /></head><body>");
        writer.println("<p class=\"title\">H2 Database Engine</p>");
        writer.println("<p>Version " + Constants.FULL_VERSION + "</p>");
        writer.println(finalText);
        writer.println("</body></html>");
        writer.close();
    }

    private static String disableRailroads(String text) {
        text = StringUtils.replaceAll(text,
                "<!-- railroad-start -->",
                "<!-- railroad-start ");
        text = StringUtils.replaceAll(text,
                "<!-- railroad-end -->",
                " railroad-end -->");
        text = StringUtils.replaceAll(text,
                "<!-- syntax-start",
                "<!-- syntax-start -->");
        text = StringUtils.replaceAll(text,
                "syntax-end -->",
                "<!-- syntax-end -->");
        return text;
    }

    private static String addLegacyFontTag(String fileName, String text) {
        int idx1 = text.indexOf("<span class=\"rule");
        if (idx1 < 0) {
            return text;
        }
        int idx2 = 0, length = text.length();
        StringBuilder builder = new StringBuilder(length + (length >> 4));
        do {
            builder.append(text, idx2, idx1);
            boolean compat = text.regionMatches(idx1 + 17, "Compat\">", 0, 8);
            boolean h2 = text.regionMatches(idx1 + 17, "H2\">", 0, 4);
            if (compat == h2) {
                throw new RuntimeException("Unknown BNF rule style in file " + fileName);
            }
            idx2 = text.indexOf("</span>", idx1 + (compat ? 8 : 4));
            if (idx2 <= 0) {
                throw new RuntimeException("</span> not found in file " + fileName);
            }
            idx2 += 7;
            builder.append("<font color=\"").append(compat ? "darkred" : "green").append("\">")
                    .append(text, idx1, idx2).append("</font>");
            idx1 = text.indexOf("<span class=\"rule", idx2);
        } while (idx1 >= 0);
        return builder.append(text, idx2, length).toString();
    }

    private static String removeHeaderFooter(String fileName, String text) {
        // String start = "<body";
        // String end = "</body>";

        String start = "<!-- } -->";
        String end = "<!-- [close] { --></div></td></tr></table>" +
                "<!-- } --><!-- analytics --></body></html>";

        int idx = text.indexOf(end);
        if (idx < 0) {
            throw new RuntimeException("Footer not found in file " + fileName);
        }
        text = text.substring(0, idx);
        idx = text.indexOf(start) + start.length();
        text = text.substring(idx + 1);
        return text;
    }

    private static String fixLinks(String text) {
        return text
                .replaceAll("href=\"build.html\"", "href=\"#build_index\"")
                .replaceAll("href=\"datatypes.html\"", "href=\"#datatypes_index\"")
                .replaceAll("href=\"faq.html\"", "href=\"#faq_index\"")
                .replaceAll("href=\"commands.html\"", "href=\"#commands_index\"")
                .replaceAll("href=\"grammar.html\"", "href=\"#grammar_index\"")
                .replaceAll("href=\"functions.html\"", "href=\"#functions_index\"")
                .replaceAll("href=\"functions-aggregate.html\"", "href=\"#functions_aggregate_index\"")
                .replaceAll("href=\"functions-window.html\"", "href=\"#functions_window_index\"")
                .replaceAll("href=\"tutorial.html\"", "href=\"#tutorial_index\"");
    }

    private static String fixTableBorders(String text) {
        return text
                .replaceAll("<table class=\"main\">",
                        "<table class=\"main\" border=\"1\" cellpadding=\"5\" cellspacing=\"0\">");
    }

    private static String getContent(String fileName) throws Exception {
        return new String(Files.readAllBytes(Paths.get(BASE_DIR, fileName)), StandardCharsets.UTF_8);
    }
}
