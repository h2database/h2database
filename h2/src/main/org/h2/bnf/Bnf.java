/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.StringTokenizer;

import org.h2.server.web.DbContextRule;
import org.h2.tools.Csv;
import org.h2.util.Resources;
import org.h2.util.StringCache;
import org.h2.util.StringUtils;

/**
 * This class can read a file that is similar to BNF (Backus-Naur form).
 * It is made specially to support SQL grammar.
 */
public class Bnf {

    static final boolean COMBINE_KEYWORDS = false;

    private static final String SEPARATORS = " [](){}|.,\r\n<>:-+*/=<\">!'";
    private static final long MAX_PARSE_TIME = 100;

    private final Random random = new Random();
    private final HashMap ruleMap = new HashMap();
    private String syntax;
    private String currentToken;
    private String[] tokens;
    private char firstChar;
    private int index;
    private Rule lastRepeat;
    private ArrayList statements;
    private String currentTopic;

    /**
     * Create an instance using the grammar specified in the CSV file.
     *
     * @param csv if not specified, the help.csv is used
     * @return a new instance
     */
    public static Bnf getInstance(Reader csv) throws Exception {
        Bnf bnf = new Bnf();
        if (csv == null) {
            byte[] data = Resources.get("/org/h2/res/help.csv");
            csv = new InputStreamReader(new ByteArrayInputStream(data));
        }
        bnf.parse(csv);
        return bnf;
    }

    Bnf() {
        random.setSeed(1);
    }

    void addFixedRule(String name, int fixedType) {
        Rule rule = new RuleFixed(fixedType);
        addRule(name, 0, "Fixed", rule);
    }

    RuleHead addRule(String topic, int id, String section, Rule rule) {
        RuleHead head = new RuleHead(id, section, topic, rule);
        if (ruleMap.get(StringUtils.toLowerEnglish(topic)) != null) {
            throw new Error("already exists: " + topic);
        }
        ruleMap.put(StringUtils.toLowerEnglish(topic), head);
        return head;
    }

    public Random getRandom() {
        return random;
    }

    private void parse(Reader csv) throws Exception {
        csv = new BufferedReader(csv);
        Rule functions = null;
        statements = new ArrayList();
        ResultSet rs = Csv.getInstance().read(csv, null);
        for (int id = 0; rs.next(); id++) {
            String section = rs.getString("SECTION").trim();
            if (section.startsWith("System")) {
                continue;
            }
            String topic = StringUtils.toLowerEnglish(rs.getString("TOPIC").trim());
            topic = StringUtils.replaceAll(topic, " ", "");
            topic = StringUtils.replaceAll(topic, "_", "");
            syntax = rs.getString("SYNTAX").trim();
            currentTopic = section;
            if (section.startsWith("Function")) {
                int end = syntax.indexOf(':');
                syntax = syntax.substring(0, end);
            }
            tokens = tokenize();
            index = 0;
            Rule rule = parseRule();
            if (section.startsWith("Command")) {
                rule = new RuleList(rule, new RuleElement(";\n\n", currentTopic), false);
            }
            RuleHead head = addRule(topic, id, section, rule);
            if (section.startsWith("Function")) {
                if (functions == null) {
                    functions = rule;
                } else {
                    functions = new RuleList(rule, functions, true);
                }
            } else if (section.startsWith("Commands")) {
                statements.add(head);
            }
        }
        addRule("@func@", 0, "Function", functions);
        addFixedRule("@ymd@", RuleFixed.YMD);
        addFixedRule("@hms@", RuleFixed.HMS);
        addFixedRule("@nanos@", RuleFixed.NANOS);
        addFixedRule("anythingExceptSingleQuote", RuleFixed.ANY_EXCEPT_SINGLE_QUOTE);
        addFixedRule("anythingExceptDoubleQuote", RuleFixed.ANY_EXCEPT_DOUBLE_QUOTE);
        addFixedRule("anythingUntilEndOfLine", RuleFixed.ANY_UNTIL_EOL);
        addFixedRule("anythingUntilEndComment", RuleFixed.ANY_UNTIL_END);
        addFixedRule("anything", RuleFixed.ANY_WORD);
        addFixedRule("@hexStart@", RuleFixed.HEX_START);
        addFixedRule("@concat@", RuleFixed.CONCAT);
        addFixedRule("@az_@", RuleFixed.AZ_UNDERLINE);
        addFixedRule("@af@", RuleFixed.AF);
        addFixedRule("@digit@", RuleFixed.DIGIT);
    }

    public String getSyntax(String rule, String syntax) {
        StringTokenizer tokenizer = new StringTokenizer(syntax, SEPARATORS, true);
        StringBuffer buff = new StringBuffer();
        while (tokenizer.hasMoreTokens()) {
            String s = tokenizer.nextToken();
            if (s.length() == 1 || StringUtils.toUpperEnglish(s).equals(s)) {
                buff.append(s);
                continue;
            }
            RuleHead found = null;
            for (int i = 0; i < s.length(); i++) {
                String test = StringUtils.toLowerEnglish(s.substring(i));
                RuleHead r = (RuleHead) ruleMap.get(test);
                if (r != null) {
                    found = r;
                    break;
                }
            }
            if (found == null || found.rule instanceof RuleFixed) {
                buff.append(s);
                continue;
            }
            String page = "grammar.html";
            if (found.section.startsWith("Data Types")) {
                page = "datatypes.html";
            } else if (found.section.startsWith("Functions")) {
                page = "functions.html";
            }
            String link = StringUtils.urlEncode(found.getTopic().toLowerCase());
            buff.append("<a href=\""+page+"#"+link+"\">");
            buff.append(s);
            buff.append("</a>");
        }
        return buff.toString();
    }

    private Rule parseRule() {
        read();
        return parseOr();
    }

    private Rule parseOr() {
        Rule r = parseList();
        if (firstChar == '|') {
            read();
            r = new RuleList(r, parseOr(), true);
        }
        lastRepeat = r;
        return r;
    }

    private Rule parseList() {
        Rule r = parseToken();
        if (firstChar != '|' && firstChar != ']' && firstChar != '}' && firstChar != 0) {
            r = new RuleList(r, parseList(), false);
        }
        lastRepeat = r;
        return r;
    }

    private Rule parseToken() {
        Rule r;
        if ((firstChar >= 'A' && firstChar <= 'Z') || (firstChar >= 'a' && firstChar <= 'z')) {
            // r = new RuleElement(currentToken+ " syntax:" + syntax);
            r = new RuleElement(currentToken, currentTopic);
        } else if (firstChar == '[') {
            read();
            Rule r2 = parseOr();
            boolean repeat = false;
            if (r2.last() instanceof RuleRepeat) {
                repeat = true;
            }
            r = new RuleOptional(r2, repeat);
            if (firstChar != ']') {
                throw new Error("expected ], got " + currentToken + " syntax:" + syntax);
            }
        } else if (firstChar == '{') {
            read();
            r = parseOr();
            if (firstChar != '}') {
                throw new Error("expected }, got " + currentToken + " syntax:" + syntax);
            }
        } else if ("@commaDots@".equals(currentToken)) {
            r = new RuleList(new RuleElement(",", currentTopic), lastRepeat, false);
            r = new RuleRepeat(r);
        } else if ("@dots@".equals(currentToken)) {
            r = new RuleRepeat(lastRepeat);
        } else {
            r = new RuleElement(currentToken, currentTopic);
        }
        lastRepeat = r;
        read();
        return r;
    }

    private void read() {
        if (index < tokens.length) {
            currentToken = tokens[index++];
            firstChar = currentToken.charAt(0);
        } else {
            currentToken = "";
            firstChar = 0;
        }
    }

    private String[] tokenize() {
        ArrayList list = new ArrayList();
        syntax = StringUtils.replaceAll(syntax, "yyyy-MM-dd", "@ymd@");
        syntax = StringUtils.replaceAll(syntax, "hh:mm:ss", "@hms@");
        syntax = StringUtils.replaceAll(syntax, "nnnnnnnnn", "@nanos@");
        syntax = StringUtils.replaceAll(syntax, "function", "@func@");
        syntax = StringUtils.replaceAll(syntax, "0x", "@hexStart@");
        syntax = StringUtils.replaceAll(syntax, ",...", "@commaDots@");
        syntax = StringUtils.replaceAll(syntax, "...", "@dots@");
        syntax = StringUtils.replaceAll(syntax, "||", "@concat@");
        syntax = StringUtils.replaceAll(syntax, "a-z|_", "@az_@");
        syntax = StringUtils.replaceAll(syntax, "A-Z|_", "@az_@");
        syntax = StringUtils.replaceAll(syntax, "a-f", "@af@");
        syntax = StringUtils.replaceAll(syntax, "A-F", "@af@");
        syntax = StringUtils.replaceAll(syntax, "0-9", "@digit@");
        StringTokenizer tokenizer = new StringTokenizer(syntax, SEPARATORS, true);
        while (tokenizer.hasMoreTokens()) {
            String s = tokenizer.nextToken();
            // avoid duplicate strings
            s = StringCache.get(s);
            if (s.length() == 1) {
                if (" \r\n".indexOf(s.charAt(0)) >= 0) {
                    continue;
                }
            }
            list.add(s);
        }
        return (String[]) list.toArray(new String[0]);
    }

    public HashMap getNextTokenList(String query) {
        HashMap next = new HashMap();
        Sentence sentence = new Sentence();
        sentence.next = next;
        sentence.text = query;
        for (int i = 0; i < statements.size(); i++) {
            RuleHead head = (RuleHead) statements.get(i);
            if (!head.section.startsWith("Commands")) {
                continue;
            }
            sentence.max = System.currentTimeMillis() + MAX_PARSE_TIME;
            head.getRule().addNextTokenList(query, sentence);
        }
        return next;
    }

    public void linkStatements() {
        for (Iterator it = ruleMap.values().iterator(); it.hasNext();) {
            RuleHead r = (RuleHead) it.next();
            r.getRule().setLinks(ruleMap);
        }
    }

    public void updateTopic(String topic, DbContextRule rule) {
        topic = StringUtils.toLowerEnglish(topic);
        RuleHead head = (RuleHead) ruleMap.get(topic);
        if (head == null) {
            head = new RuleHead(0, "db", topic, rule);
            ruleMap.put(topic, head);
            statements.add(head);
        } else {
            head.rule = rule;
        }
    }

    public ArrayList getStatements() {
        return statements;
    }

}
