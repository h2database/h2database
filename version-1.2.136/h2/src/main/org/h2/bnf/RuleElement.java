/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf;

import java.util.HashMap;

import org.h2.util.StringUtils;

/**
 * A single terminal rule in a BNF object.
 */
public class RuleElement implements Rule {

    private boolean keyword;
    private String name;
    private Rule link;
    private int type;

    RuleElement(String name, String topic) {
        this.name = name;
        if (name.length() == 1 || name.equals(StringUtils.toUpperEnglish(name))) {
            keyword = true;
        }
        topic = StringUtils.toLowerEnglish(topic);
        this.type = topic.startsWith("function") ? Sentence.FUNCTION : Sentence.KEYWORD;
    }

    public String toString() {
        return name;
    }

    public void accept(BnfVisitor visitor) {
        visitor.visitRuleElement(keyword, name, link);
    }

    public String name() {
        return name;
    }

    public void setLinks(HashMap<String, RuleHead> ruleMap) {
        if (link != null) {
            link.setLinks(ruleMap);
        }
        if (keyword) {
            return;
        }
        String test = Bnf.getRuleMapKey(name);
        for (int i = 0; i < test.length(); i++) {
            String t = test.substring(i);
            RuleHead r = ruleMap.get(t);
            if (r != null) {
                link = r.getRule();
                return;
            }
        }
        throw new AssertionError("Unknown " + name + "/" + test);
    }

    public boolean matchRemove(Sentence sentence) {
        if (sentence.shouldStop()) {
            return false;
        }
        String query = sentence.getQuery();
        if (query.length() == 0) {
            return false;
        }
        if (keyword) {
            String up = sentence.getQueryUpper();
            if (up.startsWith(name)) {
                query = query.substring(name.length());
                while (!"_".equals(name) && query.length() > 0 && Character.isWhitespace(query.charAt(0))) {
                    query = query.substring(1);
                }
                sentence.setQuery(query);
                return true;
            }
            return false;
        }
        if (!link.matchRemove(sentence)) {
            return false;
        }
        if (name != null && !name.startsWith("@") && (link.name() == null || !link.name().startsWith("@"))) {
            query = sentence.getQuery();
            while (query.length() > 0 && Character.isWhitespace(query.charAt(0))) {
                query = query.substring(1);
            }
            sentence.setQuery(query);
        }
        return true;
    }

    public void addNextTokenList(Sentence sentence) {
        if (sentence.shouldStop()) {
            return;
        }
        if (keyword) {
            String query = sentence.getQuery();
            String q = query.trim();
            String up = sentence.getQueryUpper().trim();
            if (q.length() == 0 || name.startsWith(up)) {
                if (q.length() < name.length()) {
                    sentence.add(name, name.substring(q.length()), type);
                }
            }
            return;
        }
        link.addNextTokenList(sentence);
    }

}
