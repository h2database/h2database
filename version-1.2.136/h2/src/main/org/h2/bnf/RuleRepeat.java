/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
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

    private final Rule rule;
    private final boolean comma;

    RuleRepeat(Rule rule, boolean comma) {
        this.rule = rule;
        this.comma = comma;
    }

    public String toString() {
        return "...";
    }

    public void accept(BnfVisitor visitor) {
        visitor.visitRuleRepeat(comma, rule);
    }

    public String name() {
        return rule.name();
    }

    public void setLinks(HashMap<String, RuleHead> ruleMap) {
        // rule.setLinks(ruleMap);
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
