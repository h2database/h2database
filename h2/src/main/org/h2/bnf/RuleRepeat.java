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

    public void accept(BnfVisitor visitor) {
        visitor.visitRuleRepeat(comma, rule);
    }

    public void setLinks(HashMap<String, RuleHead> ruleMap) {
        // rule.setLinks(ruleMap);
    }

    public boolean autoComplete(Sentence sentence) {
        if (sentence.shouldStop()) {
            return false;
        }
        while (rule.autoComplete(sentence)) {
            // nothing to do
        }
        return true;
    }

}
