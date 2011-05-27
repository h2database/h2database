/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
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

    RuleOptional(Rule rule, boolean repeat) {
        this.rule = rule;
    }

    public String name() {
        return null;
    }

    public String random(Bnf config, int level) {
        if (level > 10 ? config.getRandom().nextInt(level) == 1 : config.getRandom().nextInt(4) == 1) {
            return rule.random(config, level + 1);
        } else {
            return "";
        }
    }

    public Rule last() {
        return this;
    }

    public void setLinks(HashMap ruleMap) {
        if (!mapSet) {
            rule.setLinks(ruleMap);
            mapSet = true;
        }
    }

    public String matchRemove(String query, Sentence sentence) {
        if (sentence.stop()) {
            return null;
        }
        if (query.length() == 0) {
            return query;
        }
        String s = rule.matchRemove(query, sentence);
        if (s == null) {
            return query;
        }
        return s;
    }

    public void addNextTokenList(String query, Sentence sentence) {
        if (sentence.stop()) {
            return;
        }
        rule.addNextTokenList(query, sentence);
    }

}
