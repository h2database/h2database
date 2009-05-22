/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf;

import java.util.HashMap;

/**
 * Represents an optional BNF rule.
 */
public class RuleOptional implements Rule {
    private Rule rule;
    private boolean mapSet;

    RuleOptional(Rule rule) {
        this.rule = rule;
    }

    public String toString() {
        return "[" + rule.toString() + "]";
    }

    public String name() {
        return null;
    }

    public String random(Bnf config, int level) {
        if (level > 10 ? config.getRandom().nextInt(level) == 1 : config.getRandom().nextInt(4) == 1) {
            return rule.random(config, level + 1);
        }
        return "";
    }

    public Rule last() {
        return this;
    }

    public void setLinks(HashMap<String, RuleHead> ruleMap) {
        if (!mapSet) {
            rule.setLinks(ruleMap);
            mapSet = true;
        }
    }

    public boolean matchRemove(Sentence sentence) {
        if (sentence.shouldStop()) {
            return false;
        }
        String query = sentence.getQuery();
        if (query.length() == 0) {
            return true;
        }
        if (!rule.matchRemove(sentence)) {
            return true;
        }
        return true;
    }

    public void addNextTokenList(Sentence sentence) {
        if (sentence.shouldStop()) {
            return;
        }
        rule.addNextTokenList(sentence);
    }

}
