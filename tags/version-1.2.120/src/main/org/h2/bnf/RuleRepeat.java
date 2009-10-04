/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf;

import java.util.HashMap;

/**
 * Represents a loop in a BNF object.
 */
public class RuleRepeat implements Rule {

    private Rule rule;

    RuleRepeat(Rule rule) {
        this.rule = rule;
    }

    public String toString() {
        return "...";
    }

    public String name() {
        return rule.name();
    }

    public Rule last() {
        return this;
    }

    public void setLinks(HashMap<String, RuleHead> ruleMap) {
        // rule.setLinks(ruleMap);
    }

    public String random(Bnf config, int level) {
        return rule.random(config, level);
    }

    public boolean matchRemove(Sentence sentence) {
        if (sentence.shouldStop()) {
            return false;
        }
        String query = sentence.getQuery();
        if (query.length() == 0) {
            return false;
        }
        while (true) {
            if (!rule.matchRemove(sentence)) {
                return true;
            }
            if (sentence.getQuery().length() == 0) {
                return true;
            }
        }
    }

    public void addNextTokenList(Sentence sentence) {
        if (sentence.shouldStop()) {
            return;
        }
        String old = sentence.getQuery();
        while (true) {
            rule.addNextTokenList(sentence);
            if (!rule.matchRemove(sentence) || old.equals(sentence.getQuery())) {
                break;
            }
        }
        sentence.setQuery(old);
    }

}
