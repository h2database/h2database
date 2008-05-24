/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.web;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * A page parser can parse an HTML page and replace the tags there.
 * This class is used by the H2 Console.
 */
public class PageParser {
    private WebServer server;
    private String page;
    private int pos;
    private Map settings;
    private int len;
    private StringBuffer result;

    public static String parse(WebServer server, String page, Map settings) {
        PageParser block = new PageParser(server, page, settings, 0);
        return block.parse();
    }

    private PageParser(WebServer server, String page, Map settings, int pos) {
        this.server = server;
        this.page = page;
        this.pos = pos;
        this.len = page.length();
        this.settings = settings;
        result = new StringBuffer(len);
    }

    private void setError(int i) {
        String s = page.substring(0, i) + "####BUG####" + page.substring(i);
        s = PageParser.escapeHtml(s);
        result = new StringBuffer(s);
    }

    private String parseBlockUntil(String end) throws Exception {
        PageParser block = new PageParser(server, page, settings, pos);
        block.parseAll();
        if (!block.readIf(end)) {
            throw new Exception();
        }
        pos = block.pos;
        return block.result.toString();
    }

    public String parse() {
        try {
            parseAll();
            if (pos != len) {
                setError(pos);
            }
        } catch (Exception e) {
            // TODO log error
            setError(pos);
        }
        return result.toString();
    }

    private void parseAll() throws Exception {
        StringBuffer buff = result;
        String p = page;
        int i = pos;
        for (; i < len; i++) {
            char c = p.charAt(i);
            switch (c) {
            case '<': {
//                if (p.charAt(i + 1) == '%') {
//                    // TODO <%@include %>: never used
//                    if (p.charAt(i + 2) == '@') {
//                        i += 3;
//                        pos = i;
//                        read("include");
//                        String file = readParam("file");
//                        read("%>");
//                        String s = server.getTextFile(file);
//                        append(s);
//                        i = pos;
//                    } else {
//                        buff.append(c);
//                    }
//                    break;
//                } else 
                if (p.charAt(i + 3) == ':' && p.charAt(i + 1) == '/') {
                    // end tag
                    pos = i;
                    return;
                } else if (p.charAt(i + 2) == ':') {
                    pos = i;
                    if (readIf("<c:forEach")) {
                        String var = readParam("var");
                        String items = readParam("items");
                        read(">");
                        int start = pos;
                        ArrayList list = (ArrayList) get(items);
                        if (list == null) {
                            result.append("?items?");
                            list = new ArrayList();
                        }
                        if (list.size() == 0) {
                            parseBlockUntil("</c:forEach>");
                        }
                        for (int j = 0; j < list.size(); j++) {
                            settings.put(var, list.get(j));
                            pos = start;
                            String block = parseBlockUntil("</c:forEach>");
                            result.append(block);
                        }
                    } else if (readIf("<c:if")) {
                        String test = readParam("test");
                        int eq = test.indexOf("=='");
                        if (eq < 0) {
                            setError(i);
                            return;
                        }
                        String val = test.substring(eq + 3, test.length() - 1);
                        test = test.substring(0, eq);
                        String value = (String) get(test);
                        read(">");
                        String block = parseBlockUntil("</c:if>");
                        pos--;
                        if (value.equals(val)) {
                            result.append(block);
                        }
                    } else {
                        setError(i);
                        return;
                    }
                    i = pos;
                } else {
                    buff.append(c);
                }
                break;
            }
            case '$':
                if (p.charAt(i + 1) == '{') {
                    i += 2;
                    int j = p.indexOf('}', i);
                    if (j < 0) {
                        setError(i);
                        return;
                    }
                    String item = p.substring(i, j).trim();
                    i = j;
                    String s = (String) get(item);
                    append(s);
                } else {
                    buff.append(c);
                }
                break;
            default:
                buff.append(c);
            }
        }
        pos = i;
    }

    private Object get(String item) {
        int dot = item.indexOf('.');
        if (dot >= 0) {
            String sub = item.substring(dot + 1);
            item = item.substring(0, dot);
            HashMap map = (HashMap) settings.get(item);
            if (map == null) {
                return "?" + item + "?";
            }
            return map.get(sub);
        }
        return settings.get(item);
    }

    private void append(String s) {
        if (s != null) {
            result.append(PageParser.parse(server, s, settings));
        }
    }

    private String readParam(String name) throws Exception {
        read(name);
        read("=");
        read("\"");
        int start = pos;
        while (page.charAt(pos) != '"') {
            pos++;
        }
        int end = pos;
        read("\"");
        String s = page.substring(start, end);
        return PageParser.parse(server, s, settings);
    }

    private void skipSpaces() {
        while (page.charAt(pos) == ' ') {
            pos++;
        }
    }

    private void read(String s) throws Exception {
        if (!readIf(s)) {
            throw new Exception();
        }
    }

    private boolean readIf(String s) {
        skipSpaces();
        if (page.regionMatches(pos, s, 0, s.length())) {
            pos += s.length();
            skipSpaces();
            return true;
        }
        return false;
    }

    /**
     * Convert data to HTML, but don't convert newlines and multiple spaces.
     *
     * @param s the data
     * @return the escaped html text
     */
    public static String escapeHtmlData(String s) {
        return escapeHtml(s, false);
    }

    /**
     * Convert data to HTML, including newlines and multiple spaces.
     *
     * @param s the data
     * @return the escaped html text
     */
    public static String escapeHtml(String s) {
        return escapeHtml(s, true);
    }

    private static String escapeHtml(String s, boolean convertBreakAndSpace) {
        if (s == null) {
            return null;
        }
        if (convertBreakAndSpace) {
            if (s.length() == 0) {
                return "&nbsp;";
            }
        }
        StringBuffer buff = new StringBuffer(s.length());
        boolean convertSpace = true;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == ' ') {
                if (convertSpace && convertBreakAndSpace) {
                    buff.append("&nbsp;");
                } else {
                    buff.append(' ');
                    convertSpace = true;
                }
                continue;
            } 
            convertSpace = false;
            switch (c) {
            case '$':
                // so that ${ } in the text is interpreted correctly
                buff.append("&#36;");
                break;
            case '<':
                buff.append("&lt;");
                break;
            case '>':
                buff.append("&gt;");
                break;
            case '&':
                buff.append("&amp;");
                break;
            case '"':
                buff.append("&quot;");
                break;
            case '\'':
                buff.append("&#39;");
                break;
            case '\n':
                if (convertBreakAndSpace) {
                    buff.append("<br />");
                    convertSpace = true;
                } else {
                    buff.append(c);
                }
                break;
            default:
                if (c >= 128) {
                    buff.append("&#");
                    buff.append((int) c);
                    buff.append(';');
                } else {
                    buff.append(c);
                }
            }
        }
        return buff.toString();
    }

    public static String escapeJavaScript(String s) {
        if (s == null) {
            return null;
        }
        if (s.length() == 0) {
            return "";
        }
        StringBuffer buff = new StringBuffer(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
            case '"':
                buff.append("\\\"");
                break;
            case '\'':
                buff.append("\\'");
                break;
            case '\\':
                buff.append("\\\\");
                break;
            case '\n':
                buff.append("\\n");
                break;
            case '\r':
                buff.append("\\r");
                break;
            case '\t':
                buff.append("\\t");
                break;
            default:
                buff.append(c);
            }
        }
        return buff.toString();
        // return escapeHtml(buff.toString());
    }
}
