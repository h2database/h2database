/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Alessandro Ventura
 */
package org.h2.security.auth;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

/**
 * Configuration property
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class PropertyConfig {

    @XmlAttribute(required = true)
    String name;

    @XmlAttribute
    String value;

    public PropertyConfig() {
    }
    
    public PropertyConfig(String name, String value) {
       this.name=name;
       this.value=value;
    }
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
