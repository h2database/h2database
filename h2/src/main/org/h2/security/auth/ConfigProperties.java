/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Alessandro Ventura
 */
package org.h2.security.auth;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * wrapper for configuration properties
 */
public class ConfigProperties {

    Map<String, String> properties;

    public ConfigProperties() {
       properties = new HashMap<>();
    }
    
    public ConfigProperties(PropertyConfig...configProperties) {
        this(configProperties==null?null:Arrays.asList(configProperties));
    }
    
    public ConfigProperties(Collection<PropertyConfig> configProperties) {
        properties = new HashMap<>();
        if (properties != null) {
            for (PropertyConfig currentProperty : configProperties) {
                if (properties.put(currentProperty.getName(), currentProperty.getValue())!=null) {
                    throw new AuthConfigException("duplicate property "+currentProperty.getName());
                }
            }
        }
    }

    public String getStringValue(String name, String defaultValue) {
        String result = properties.get(name);
        if (result == null) {
            return defaultValue;
        }
        return result;
    }

    public String getStringValue(String name) {
        String result = properties.get(name);
        if (result == null) {
            throw new AuthConfigException("missing config property "+name);
        }
        return result;
    }

    public int getIntValue(String name, int defaultValue) {
        String result = properties.get(name);
        if (result == null) {
            return defaultValue;
        }
        return Integer.parseInt(result);
    }

    public int getIntValue(String name) {
        String result = properties.get(name);
        if (result == null) {
            throw new AuthConfigException("missing config property "+name);
        }
        return Integer.parseInt(result);
    }

    public boolean getBooleanValue(String name, boolean defaultValue) {
        String result = properties.get(name);
        if (result == null) {
            return defaultValue;
        }
        switch (result) {
        case "true":
        case "yes":
        case "1":
            return true;
        default:
            return false;
        }
    }

}
