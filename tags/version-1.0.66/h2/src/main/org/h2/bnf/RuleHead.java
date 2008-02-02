/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf;

/**
 * Represents the head of a BNF rule.
 */
public class RuleHead {
    String section;
    Rule rule;
    private String topic;

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

}
