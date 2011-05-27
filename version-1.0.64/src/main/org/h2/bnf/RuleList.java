/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Represents a sequence of BNF rules, or a list of alternative rules.
 */
public class RuleList implements Rule {

    private boolean or;
    private ArrayList list;
    private boolean mapSet;

    RuleList(Rule first, Rule next, boolean or) {
        list = new ArrayList();
        if (first instanceof RuleList && ((RuleList) first).or == or) {
            list.addAll(((RuleList) first).list);
        } else {
            list.add(first);
        }
        if (next instanceof RuleList && ((RuleList) next).or == or) {
            list.addAll(((RuleList) next).list);
        } else {
            list.add(next);
        }
        if (!or && Bnf.COMBINE_KEYWORDS) {
            for (int i = 0; i < list.size() - 1; i++) {
                Rule r1 = (Rule) list.get(i);
                Rule r2 = (Rule) list.get(i + 1);
                if (!(r1 instanceof RuleElement) || !(r2 instanceof RuleElement)) {
                    continue;
                }
                RuleElement re1 = (RuleElement) r1;
                RuleElement re2 = (RuleElement) r2;
                if (!re1.isKeyword() || !re2.isKeyword()) {
                    continue;
                }
                re1 = re1.merge(re2);
                list.set(i, re1);
                list.remove(i + 1);
                i--;
            }
        }
        this.or = or;
    }

    public String random(Bnf config, int level) {
        if (or) {
            if (level > 10) {
                if (level > 1000) {
                    // better than stack overflow
                    throw new Error();
                }
                return get(0).random(config, level);
            }
            int idx = config.getRandom().nextInt(list.size());
            return get(idx).random(config, level + 1);
        } else {
            StringBuffer buff = new StringBuffer();
            for (int i = 0; i < list.size(); i++) {
                buff.append(get(i).random(config, level+1));
            }
            return buff.toString();
        }
    }

    private Rule get(int idx) {
        return ((Rule) list.get(idx));
    }

    public String name() {
        return null;
    }

    public Rule last() {
        return get(list.size() - 1);
    }

    public void setLinks(HashMap ruleMap) {
        if (!mapSet) {
            for (int i = 0; i < list.size(); i++) {
                get(i).setLinks(ruleMap);
            }
            mapSet = true;
        }
    }

    public String matchRemove(String query, Sentence sentence) {
        if (query.length() == 0) {
            return null;
        }
        if (or) {
            for (int i = 0; i < list.size(); i++) {
                String s = get(i).matchRemove(query, sentence);
                if (s != null) {
                    return s;
                }
            }
            return null;
        } else {
            for (int i = 0; i < list.size(); i++) {
                Rule r = get(i);
                query = r.matchRemove(query, sentence);
                if (query == null) {
                    return null;
                }
            }
            return query;
        }
    }

    public void addNextTokenList(String query, Sentence sentence) {
        if (sentence.stop()) {
            //
        }
        if (or) {
            for (int i = 0; i < list.size(); i++) {
                get(i).addNextTokenList(query, sentence);
            }
        } else {
            for (int i = 0; i < list.size(); i++) {
                Rule r = get(i);
                r.addNextTokenList(query, sentence);
                query = r.matchRemove(query, sentence);
                if (query == null) {
                    break;
                }
            }
        }
    }

}
