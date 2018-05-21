/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Alessandro Ventura
 */
package org.h2.security.auth.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import org.h2.api.UserToRolesMapper;
import org.h2.security.auth.AuthenticationException;
import org.h2.security.auth.AuthenticationInfo;
import org.h2.security.auth.ConfigProperties;

/**
 * Assign static roles to authenticated users
 * 
 * parameters:
 *   roles = role list separated by comma
 */
public class StaticRolesMapper implements UserToRolesMapper {

    Collection<String> roles;
    
    public StaticRolesMapper() {
    }
    
    public StaticRolesMapper(String... roles) {
        this.roles=Arrays.asList(roles);
    }
    
    @Override
    public void configure(ConfigProperties configProperties) {
        String rolesString=configProperties.getStringValue("roles", "");
        if (rolesString!=null) {
            roles = new HashSet<>(Arrays.asList(rolesString.split(",")));
        }
    }

    @Override
    public Collection<String> mapUserToRoles(AuthenticationInfo authenticationInfo) throws AuthenticationException {
        return roles;
    }

}
