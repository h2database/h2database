/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.doc;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.h2.bnf.Bnf;
import org.h2.bnf.BnfVisitor;
import org.h2.bnf.Rule;
import org.h2.bnf.RuleFixed;
import org.h2.bnf.RuleHead;
import org.h2.util.StringUtils;

/**
 * A BNF visitor that generates BNF in HTML form.
 */
public class BnfSyntax implements BnfVisitor {

    private String html;

    /**
     * Get the HTML syntax for the given syntax.
     *
     * @param bnf the BNF
     * @param syntaxLines the syntax
     * @return the HTML
     */
    public String getHtml(Bnf bnf, String syntaxLines) {
        syntaxLines = StringUtils.replaceAll(syntaxLines, "\n    ", "\n");
        StringTokenizer tokenizer = Bnf.getTokenizer(syntaxLines);
        StringBuilder buff = new StringBuilder();
        ArrayDeque<Character> deque = new ArrayDeque<>();
        boolean extension = false;
        while (tokenizer.hasMoreTokens()) {
            String s = tokenizer.nextToken();
            if (s.equals("@c@")) {
                if (!extension) {
                    extension = true;
                    buff.append("<span class=\"ruleCompat\">");
                }
                s = skipAfterExtensionStart(tokenizer);
            } else if (s.equals("@h2@")) {
                if (!extension) {
                    extension = true;
                    buff.append("<span class=\"ruleH2\">");
                }
                s = skipAfterExtensionStart(tokenizer);
            }
            if (extension) {
                if (s.length() == 1) {
                    char c = s.charAt(0);
                    switch (c) {
                    case '[':
                        deque.addLast(']');
                        break;
                    case '{':
                        deque.addLast('}');
                        break;
                    case ']':
                    case '}':
                        char c2 = deque.removeLast();
                        if (c != c2) {
                            throw new AssertionError("Expected " + c2 + " got " + c);
                        }
                        break;
                    default:
                        if (deque.isEmpty()) {
                            deque.add('*');
                        }
                    }
                } else if (deque.isEmpty()) {
                    deque.add('*');
                }
            }
            if (s.length() == 1 || StringUtils.toUpperEnglish(s).equals(s)) {
                buff.append(StringUtils.xmlText(s));
                if (extension && deque.isEmpty()) {
                    extension = false;
                    buff.append("</span>");
                }
                continue;
            }
            buff.append(getLink(bnf, s));
        }
        if (extension) {
            if (deque.size() != 1 || deque.getLast() != '*') {
                throw new AssertionError("Expected " + deque.getLast() + " got end of data");
            }
            buff.append("</span>");
        }
        String s = buff.toString();
        // ensure it works within XHTML comments
        s = StringUtils.replaceAll(s, "--", "&#45;-");
        return s;
    }

    private static String skipAfterExtensionStart(StringTokenizer tokenizer) {
        String s;
        do {
            s = tokenizer.nextToken();
        } while (s.equals(" "));
        return s;
    }

    /**
     * Get the HTML link to the given token.
     *
     * @param bnf the BNF
     * @param token the token
     * @return the HTML link
     */
    String getLink(Bnf bnf, String token) {
        RuleHead found = null;
        String key = Bnf.getRuleMapKey(token);
        for (int i = 0; i < token.length(); i++) {
            String test = StringUtils.toLowerEnglish(key.substring(i));
            RuleHead r = bnf.getRuleHead(test);
            if (r != null) {
                found = r;
                break;
            }
        }
        if (found == null) {
            return token;
        }
        String page = "grammar.html";
        String section = found.getSection();
        if (section.startsWith("Commands")) {
            page = "commands.html";
        } if (section.startsWith("Data Types") || section.startsWith("Interval Data Types")) {
            page = "datatypes.html";
        } else if (section.startsWith("Functions")) {
            page = "functions.html";
        } else if (token.equals("@func@")) {
            return "<a href=\"functions.html\">Function</a>";
        } else if (found.getRule() instanceof RuleFixed) {
            found.getRule().accept(this);
            return html;
        }
        String link = found.getTopic().toLowerCase().replace(' ', '_');
        link = page + "#" + StringUtils.urlEncode(link);
        return "<a href=\"" + link + "\">" + token + "</a>";
    }

    @Override
    public void visitRuleElement(boolean keyword, String name, Rule link) {
        // not used
    }

    @Override
    public void visitRuleFixed(int type) {
        html = BnfRailroad.getHtmlText(type);
    }

    @Override
    public void visitRuleList(boolean or, ArrayList<Rule> list) {
        // not used
    }

    @Override
    public void visitRuleOptional(Rule rule) {
        // not used
    }

    @Override
    public void visitRuleOptional(ArrayList<Rule> list) {
        // not used
    }

    @Override
    public void visitRuleRepeat(boolean comma, Rule rule) {
        // not used
    }

    @Override
    public void visitRuleExtension(Rule rule, boolean compatibility) {
        // not used
    }

}
