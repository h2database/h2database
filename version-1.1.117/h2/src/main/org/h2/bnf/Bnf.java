/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.StringTokenizer;

import org.h2.server.web.DbContextRule;
import org.h2.tools.Csv;
import org.h2.util.New;
import org.h2.util.Resources;
import org.h2.util.StringCache;
import org.h2.util.StringUtils;

/**
 * This class can read a file that is similar to BNF (Backus-Naur form).
 * It is made specially to support SQL grammar.
 */
public class Bnf {

    private final Random random = new Random();
    private final HashMap<String, RuleHead> ruleMap = New.hashMap();
    private String syntax;
    private String currentToken;
    private String[] tokens;
    private char firstChar;
    private int index;
    private Rule lastRepeat;
    private ArrayList<RuleHead> statements;
    private String currentTopic;

    Bnf() {
        random.setSeed(1);
    }

    /**
     * Create an instance using the grammar specified in the CSV file.
     *
     * @param csv if not specified, the help.csv is used
     * @return a new instance
     */
    public static Bnf getInstance(Reader csv) throws SQLException, IOException {
        Bnf bnf = new Bnf();
        if (csv == null) {
            byte[] data = Resources.get("/org/h2/res/help.csv");
            csv = new InputStreamReader(new ByteArrayInputStream(data));
        }
        bnf.parse(csv);
        return bnf;
    }

    private void addFixedRule(String name, int fixedType) {
        Rule rule = new RuleFixed(fixedType);
        addRule(name, "Fixed", rule);
    }

    private RuleHead addRule(String topic, String section, Rule rule) {
        RuleHead head = new RuleHead(section, topic, rule);
        if (ruleMap.get(StringUtils.toLowerEnglish(topic)) != null) {
            throw new Error("already exists: " + topic);
        }
        ruleMap.put(StringUtils.toLowerEnglish(topic), head);
        return head;
    }

    public Random getRandom() {
        return random;
    }

    private void parse(Reader csv) throws SQLException, IOException {
        Rule functions = null;
        statements = New.arrayList();
        ResultSet rs = Csv.getInstance().read(csv, null);
        for (int id = 0; rs.next(); id++) {
            String section = rs.getString("SECTION").trim();
            if (section.startsWith("System")) {
                continue;
            }
            String topic = StringUtils.toLowerEnglish(rs.getString("TOPIC").trim());
            topic = StringUtils.replaceAll(topic, " ", "_");
            // topic = StringUtils.replaceAll(topic, "_", "");
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
            RuleHead head = addRule(topic, section, rule);
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
        addRule("@func@", "Function", functions);
        addFixedRule("@ymd@", RuleFixed.YMD);
        addFixedRule("@hms@", RuleFixed.HMS);
        addFixedRule("@nanos@", RuleFixed.NANOS);
        addFixedRule("anything_except_single_quote", RuleFixed.ANY_EXCEPT_SINGLE_QUOTE);
        addFixedRule("anything_except_double_quote", RuleFixed.ANY_EXCEPT_DOUBLE_QUOTE);
        addFixedRule("anything_until_end_of_line", RuleFixed.ANY_UNTIL_EOL);
        addFixedRule("anything_until_end_comment", RuleFixed.ANY_UNTIL_END);
        addFixedRule("anything_except_two_dollar_signs", RuleFixed.ANY_EXCEPT_2_DOLLAR);
        addFixedRule("anything", RuleFixed.ANY_WORD);
        addFixedRule("@hex_start@", RuleFixed.HEX_START);
        addFixedRule("@concat@", RuleFixed.CONCAT);
        addFixedRule("@az_@", RuleFixed.AZ_UNDERSCORE);
        addFixedRule("@af@", RuleFixed.AF);
        addFixedRule("@digit@", RuleFixed.DIGIT);
    }

    /**
     * Get the HTML documentation for a given syntax.
     *
     * @param bnf the BNF syntax
     * @return the HTML formatted text
     */
    public String getSyntaxHtml(String bnf) {
        StringTokenizer tokenizer = getTokenizer(bnf);
        StringBuilder buff = new StringBuilder();
        while (tokenizer.hasMoreTokens()) {
            String s = tokenizer.nextToken();
            if (s.length() == 1 || StringUtils.toUpperEnglish(s).equals(s)) {
                buff.append(s);
                continue;
            }
            RuleHead found = null;
            for (int i = 0; i < s.length(); i++) {
                String test = StringUtils.toLowerEnglish(s.substring(i));
                RuleHead r = ruleMap.get(test);
                if (r != null) {
                    found = r;
                    break;
                }
            }
            if (found == null || found.getRule() instanceof RuleFixed) {
                buff.append(s);
                continue;
            }
            String page = "grammar.html";
            if (found.getSection().startsWith("Data Types")) {
                page = "datatypes.html";
            } else if (found.getSection().startsWith("Functions")) {
                page = "functions.html";
            }
            String link = StringUtils.urlEncode(found.getTopic().toLowerCase());
            buff.append("<a href=\"").append(page).append("#").
                append(link).append("\">").append(s).append("</a>");
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
            r = new RuleOptional(r2);
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
        ArrayList<String> list = New.arrayList();
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
        StringTokenizer tokenizer = getTokenizer(syntax);
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
        return list.toArray(new String[0]);
    }

    /**
     * Get the list of tokens that can follow.
     * This is the main autocomplete method.
     * The returned map for the query 'S' may look like this:
     * <pre>
     * key: 1#SELECT, value: ELECT
     * key: 1#SET, value: ET
     * </pre>
     *
     * @param query the start of the statement
     * @return the map of possible token types / tokens
     */
    public HashMap<String, String> getNextTokenList(String query) {
        Sentence sentence = new Sentence();
        sentence.setQuery(query);
        for (RuleHead head : statements) {
            if (!head.getSection().startsWith("Commands")) {
                continue;
            }
            sentence.start();
            head.getRule().addNextTokenList(sentence);
        }
        return sentence.getNext();
    }

    /**
     * Cross-link all statements with each other.
     * This method is called after updating the topics.
     */
    public void linkStatements() {
        for (RuleHead r : ruleMap.values()) {
            r.getRule().setLinks(ruleMap);
        }
    }

    /**
     * Update a topic with a context specific rule.
     * This is used for autocomplete support.
     *
     * @param topic the topic
     * @param rule the database context rule
     */
    public void updateTopic(String topic, DbContextRule rule) {
        topic = StringUtils.toLowerEnglish(topic);
        RuleHead head = ruleMap.get(topic);
        if (head == null) {
            head = new RuleHead("db", topic, rule);
            ruleMap.put(topic, head);
            statements.add(head);
        } else {
            head.setRule(rule);
        }
    }

    /**
     * Get the list of possible statements.
     *
     * @return the list of statements
     */
    public ArrayList<RuleHead> getStatements() {
        return statements;
    }

    private StringTokenizer getTokenizer(String s) {
        return new StringTokenizer(s, " [](){}|.,\r\n<>:-+*/=<\">!'$", true);
    }

}
