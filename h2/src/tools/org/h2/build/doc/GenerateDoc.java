/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.doc;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.h2.bnf.Bnf;
import org.h2.engine.Constants;
import org.h2.server.web.PageParser;
import org.h2.tools.Csv;
import org.h2.util.StringUtils;

/**
 * This application generates sections of the documentation
 * by converting the built-in help section
 * to cross linked html.
 */
public class GenerateDoc {

    private static final String IN_HELP = "src/main/org/h2/res/help.csv";
    private Path inDir = Paths.get("src/docsrc/html");
    private Path outDir = Paths.get("docs/html");
    private Connection conn;
    private final HashMap<String, Object> session =
            new HashMap<>();
    private Bnf bnf;

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        new GenerateDoc().run(args);
    }

    private void run(String... args) throws Exception {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-in")) {
                inDir = Paths.get(args[++i]);
            } else if (args[i].equals("-out")) {
                outDir = Paths.get(args[++i]);
            }
        }
        Class.forName("org.h2.Driver");
        conn = DriverManager.getConnection("jdbc:h2:mem:");
        Files.createDirectories(outDir);
        new RailroadImages().run(outDir.resolve("images"));
        bnf = Bnf.getInstance(null);
        bnf.linkStatements();
        session.put("version", Constants.VERSION);
        session.put("versionDate", Constants.BUILD_DATE);
        session.put("downloadRoot",
                "https://github.com/h2database/h2database/releases/download/version-" + Constants.VERSION);
        String help = "SELECT ROWNUM ID, * FROM CSVREAD('" +
                IN_HELP + "', NULL, 'lineComment=#') WHERE SECTION ";
        map("commandsDML",
                help + "= 'Commands (DML)' ORDER BY ID", true, false);
        map("commandsDDL",
                help + "= 'Commands (DDL)' ORDER BY ID", true, false);
        map("commandsOther",
                help + "= 'Commands (Other)' ORDER BY ID", true, false);
        map("literals",
                help + "= 'Literals' ORDER BY ID", true, false);
        map("datetimeFields",
                help + "= 'Datetime fields' ORDER BY ID", true, false);
        map("otherGrammar",
                help + "= 'Other Grammar' ORDER BY ID", true, false);

        map("functionsNumeric",
                help + "= 'Functions (Numeric)' ORDER BY ID", true, false);
        map("functionsString",
                help + "= 'Functions (String)' ORDER BY ID", true, false);
        map("functionsTimeDate",
                help + "= 'Functions (Time and Date)' ORDER BY ID", true, false);
        map("functionsSystem",
                help + "= 'Functions (System)' ORDER BY ID", true, false);
        map("functionsJson",
                help + "= 'Functions (JSON)' ORDER BY ID", true, false);
        map("functionsTable",
                help + "= 'Functions (Table)' ORDER BY ID", true, false);

        map("aggregateFunctionsGeneral",
                help + "= 'Aggregate Functions (General)' ORDER BY ID", true, false);
        map("aggregateFunctionsBinarySet",
                help + "= 'Aggregate Functions (Binary Set)' ORDER BY ID", true, false);
        map("aggregateFunctionsOrdered",
                help + "= 'Aggregate Functions (Ordered)' ORDER BY ID", true, false);
        map("aggregateFunctionsHypothetical",
                help + "= 'Aggregate Functions (Hypothetical Set)' ORDER BY ID", true, false);
        map("aggregateFunctionsInverse",
                help + "= 'Aggregate Functions (Inverse Distribution)' ORDER BY ID", true, false);
        map("aggregateFunctionsJSON",
                help + "= 'Aggregate Functions (JSON)' ORDER BY ID", true, false);

        map("windowFunctionsRowNumber",
                help + "= 'Window Functions (Row Number)' ORDER BY ID", true, false);
        map("windowFunctionsRank",
                help + "= 'Window Functions (Rank)' ORDER BY ID", true, false);
        map("windowFunctionsLeadLag",
                help + "= 'Window Functions (Lead or Lag)' ORDER BY ID", true, false);
        map("windowFunctionsNth",
                help + "= 'Window Functions (Nth Value)' ORDER BY ID", true, false);
        map("windowFunctionsOther",
                help + "= 'Window Functions (Other)' ORDER BY ID", true, false);

        map("dataTypes",
                help + "LIKE 'Data Types%' ORDER BY SECTION, ID", true, true);
        map("intervalDataTypes",
                help + "LIKE 'Interval Data Types%' ORDER BY SECTION, ID", true, true);
        HashMap<String, String> informationSchemaTables = new HashMap<>();
        HashMap<String, String> informationSchemaColumns = new HashMap<>(512);
        Csv csv = new Csv();
        csv.setLineCommentCharacter('#');
        try (ResultSet rs = csv.read("src/docsrc/help/information_schema.csv", null, null)) {
            while (rs.next()) {
                String tableName = rs.getString(1);
                String columnName = rs.getString(2);
                String description = rs.getString(3);
                if (columnName != null) {
                    informationSchemaColumns.put(tableName == null ? columnName : tableName + '.' + columnName,
                            description);
                } else {
                    informationSchemaTables.put(tableName, description);
                }
            }
        }
        int errorCount = 0;
        try (Statement stat = conn.createStatement();
                PreparedStatement prep = conn.prepareStatement("SELECT COLUMN_NAME, "
                        + "DATA_TYPE_SQL('INFORMATION_SCHEMA', TABLE_NAME, 'TABLE', DTD_IDENTIFIER) DT "
                        + "FROM INFORMATION_SCHEMA.COLUMNS "
                        + "WHERE TABLE_SCHEMA = 'INFORMATION_SCHEMA' AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION")) {
            ResultSet rs = stat.executeQuery("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                    + "WHERE TABLE_SCHEMA = 'INFORMATION_SCHEMA' ORDER BY TABLE_NAME");

            ArrayList<HashMap<String, String>> list = new ArrayList<>();
            StringBuilder builder = new StringBuilder();
            while (rs.next()) {
                HashMap<String, String> map = new HashMap<>(8);
                String table = rs.getString(1);
                map.put("table", table);
                map.put("link", "information_schema_" + StringUtils.urlEncode(table.toLowerCase()));
                String description = informationSchemaTables.get(table);
                if (description == null) {
                    System.out.println("No documentation for INFORMATION_SCHEMA." + table);
                    errorCount++;
                    description = "";
                }
                map.put("description", StringUtils.xmlText(description));
                prep.setString(1, table);
                ResultSet rs2 = prep.executeQuery();
                builder.setLength(0);
                while (rs2.next()) {
                    if (rs2.getRow() > 1) {
                        builder.append('\n');
                    }
                    String column = rs2.getString(1);
                    description = informationSchemaColumns.get(table + '.' + column);
                    if (description == null) {
                        description = informationSchemaColumns.get(column);
                        if (description == null) {
                            System.out.println("No documentation for INFORMATION_SCHEMA." + table + '.' + column);
                            errorCount++;
                            description = "";
                        }
                    }
                    builder.append("<tr><td>").append(column).append("</td><td>").append(rs2.getString(2))
                            .append("</td></tr><tr><td colspan=\"2\">")
                            .append(StringUtils.xmlText(description)).append("</td></tr>");
                }
                map.put("columns", builder.toString());
                list.add(map);
            }
            putToMap("informationSchema", list);
        }
        Files.walkFileTree(inDir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                process(file);
                return FileVisitResult.CONTINUE;
            }
        });
        conn.close();
        if (errorCount > 0) {
            throw new IOException(errorCount + (errorCount == 1 ? " error" : " errors") +  " found");
        }
    }

    /**
     * Process a file.
     *
     * @param inFile the file
     */
    void process(Path inFile) throws IOException {
        Path outFile = outDir.resolve(inDir.relativize(inFile));
        Files.createDirectories(outFile.getParent());
        byte[] bytes = Files.readAllBytes(inFile);
        if (inFile.getFileName().toString().endsWith(".html")) {
            String page = new String(bytes);
            page = PageParser.parse(page, session);
            bytes = page.getBytes();
        }
        Files.write(outFile, bytes);
    }

    private void map(String key, String sql, boolean railroads, boolean forDataTypes)
            throws Exception {
        try (Statement stat = conn.createStatement();
                ResultSet rs = stat.executeQuery(sql)) {
            ArrayList<HashMap<String, String>> list =
                    new ArrayList<>();
            while (rs.next()) {
                HashMap<String, String> map = new HashMap<>();
                ResultSetMetaData meta = rs.getMetaData();
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    String k = StringUtils.toLowerEnglish(meta.getColumnLabel(i));
                    String value = rs.getString(i);
                    value = value.trim();
                    map.put(k, PageParser.escapeHtml(value));
                }
                String topic = rs.getString("TOPIC");
                // Convert "INT Type" to "INT" etc.
                if (forDataTypes && topic.endsWith(" Type")) {
                    map.put("topic", topic.substring(0, topic.length() - 5));
                }
                String syntax = rs.getString("SYNTAX").trim();
                if (railroads) {
                    BnfRailroad r = new BnfRailroad();
                    String railroad = r.getHtml(bnf, syntax);
                    map.put("railroad", railroad);
                }
                BnfSyntax visitor = new BnfSyntax();
                String syntaxHtml = visitor.getHtml(bnf, syntax);
                map.put("syntax", syntaxHtml);

                // remove newlines in the regular text
                String text = map.get("text");
                if (text != null) {
                    // text is enclosed in <p> .. </p> so this works.
                    text = StringUtils.replaceAll(text,
                            "<br /><br />", "</p><p>");
                    text = StringUtils.replaceAll(text,
                            "<br />", " ");
                    text = addCode(text);
                    text = addLinks(text);
                    map.put("text", text);
                }

                String link = topic.toLowerCase();
                link = link.replace(' ', '_');
                // link = StringUtils.replaceAll(link, "_", "");
                link = link.replace('@', '_');
                map.put("link", StringUtils.urlEncode(link));

                list.add(map);
            }
            putToMap(key, list);
        }
    }

    private void putToMap(String key, ArrayList<HashMap<String, String>> list) {
        session.put(key, list);
        int div = 3;
        int part = (list.size() + div - 1) / div;
        for (int i = 0, start = 0; i < div; i++, start += part) {
            int end = Math.min(start + part, list.size());
            List<HashMap<String, String>> listThird = start <= end ? list.subList(start, end)
                    : Collections.emptyList();
            session.put(key + '-' + i, listThird);
        }
    }

    private static String addCode(String text) {
        text = StringUtils.replaceAll(text, "&quot;", "\"");
        StringBuilder buff = new StringBuilder(text.length());
        int len = text.length();
        boolean code = false, codeQuoted = false;
        for (int i = 0; i < len; i++) {
            char c = text.charAt(i);
            if (i < len - 1) {
                char next = text.charAt(i+1);
                if (!code && !codeQuoted) {
                    if (Character.isUpperCase(c) && Character.isUpperCase(next)) {
                        buff.append("<code>");
                        code = true;
                    } else if (c == '\"' && (i == 0 || text.charAt(i - 1) != '\\')) {
                        buff.append("<code>");
                        codeQuoted = true;
                        continue;
                    }
                }
            }
            if (code) {
                if (!Character.isLetterOrDigit(c) && "_.".indexOf(c) < 0) {
                    buff.append("</code>");
                    code = false;
                }
            } else if (codeQuoted && c == '\"' && (i == 0 || text.charAt(i - 1) != '\\')) {
                buff.append("</code>");
                codeQuoted = false;
                continue;
            }
            buff.append(c);
        }
        if (code) {
            buff.append("</code>");
        }
        String s = buff.toString();
        s = StringUtils.replaceAll(s, "</code>, <code>", ", ");
        s = StringUtils.replaceAll(s, ".</code>", "</code>.");
        s = StringUtils.replaceAll(s, ",</code>", "</code>.");
        s = StringUtils.replaceAll(s, " @<code>", " <code>@");
        s = StringUtils.replaceAll(s, "</code> <code>", " ");
        s = StringUtils.replaceAll(s, "<code>SQL</code>", "SQL");
        s = StringUtils.replaceAll(s, "<code>XML</code>", "XML");
        s = StringUtils.replaceAll(s, "<code>URL</code>", "URL");
        s = StringUtils.replaceAll(s, "<code>URLs</code>", "URLs");
        s = StringUtils.replaceAll(s, "<code>HTML</code>", "HTML");
        s = StringUtils.replaceAll(s, "<code>KB</code>", "KB");
        s = StringUtils.replaceAll(s, "<code>MB</code>", "MB");
        s = StringUtils.replaceAll(s, "<code>GB</code>", "GB");
        return s;
    }

    private static String addLinks(String text) {
        int start = nextLink(text, 0);
        if (start < 0) {
            return text;
        }
        StringBuilder buff = new StringBuilder(text.length());
        int len = text.length();
        int offset = 0;
        do {
            if (start > 2 && text.regionMatches(start - 2, "](https://h2database.com/html/", 0, 30)) {
                int descEnd = start - 2;
                int descStart = text.lastIndexOf('[', descEnd - 1) + 1;
                int linkStart = start + 28;
                int linkEnd = text.indexOf(')', start + 29);
                buff.append(text, offset, descStart - 1) //
                        .append("<a href=\"").append(text, linkStart, linkEnd).append("\">") //
                        .append(text, descStart, descEnd) //
                        .append("</a>");
                offset = linkEnd + 1;
            } else {
                int end = start + 7;
                for (; end < len && !Character.isWhitespace(text.charAt(end)); end++) {
                    // Nothing to do
                }
                buff.append(text, offset, start) //
                        .append("<a href=\"").append(text, start, end).append("\">") //
                        .append(text, start, end) //
                        .append("</a>");
                offset = end;
            }
        } while ((start = nextLink(text, offset)) >= 0);
        return buff.append(text, offset, len).toString();
    }

    private static int nextLink(String text, int i) {
        int found = -1;
        found = findLink(text, i, "http://", found);
        found = findLink(text, i, "https://", found);
        return found;
    }

    private static int findLink(String text, int offset, String prefix, int found) {
        int idx = text.indexOf(prefix, offset);
        if (idx >= 0 && (found < 0 || idx < found)) {
            found = idx;
        }
        return found;
    }

}
