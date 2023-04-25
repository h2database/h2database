/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf;

import java.util.HashMap;

/**
 * Represents an optional BNF rule.
 */
public class RuleOptional implements Rule {
    private final Rule rule;
    private boolean mapSet;

    public RuleOptional(Rule rule) {
        this.rule = rule;
    }

    @Override
    public void accept(BnfVisitor visitor) {
        if (rule instanceof RuleList) {
            RuleList ruleList = (RuleList) rule;
            if (ruleList.or) {
                visitor.visitRuleOptional(ruleList.list);
                return;
            }
        }
        visitor.visitRuleOptional(rule);
    }

    @Override
    public void setLinks(HashMap<String, RuleHead> ruleMap) {
        if (!mapSet) {
            rule.setLinks(ruleMap);
            mapSet = true;
        }
    }
    @Override
    public boolean autoComplete(Sentence sentence) {
        sentence.stopIfRequired();
        rule.autoComplete(sentence);
        return true;
    }

    @Override
    public String toString() {
        return '[' + rule.toString() + ']';
    }

}
