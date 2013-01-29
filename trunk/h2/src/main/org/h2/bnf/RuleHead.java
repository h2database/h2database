/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf;

/**
 * Represents the head of a BNF rule.
 */
public class RuleHead {
    private final String section;
    private final String topic;
    private Rule rule;

    RuleHead(String section, String topic, Rule rule) {
        this.section = section;
        this.topic = topic;
        this.rule = rule;
    }

    public String getTopic() {
        return topic;
    }

    public Rule getRule() {
        return rule;
    }

    void setRule(Rule rule) {
        this.rule = rule;
    }

    public String getSection() {
        return section;
    }

}
