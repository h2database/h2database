package org.h2.security.auth.impl;

import java.util.Arrays;
import java.util.Collection;

import org.h2.api.UserToRolesMapper;
import org.h2.security.auth.AuthenticationException;
import org.h2.security.auth.AuthenticationInfo;
import org.h2.security.auth.ConfigProperties;

/**
 * Assign to user a role based on realm name
 * 
 * by default role name is @{realm}
 */
public class AssignRealmNameRole implements UserToRolesMapper{

    String roleNameFormat;

    public AssignRealmNameRole() {
        this("@%s");
    }
    
    public AssignRealmNameRole(String roleNameFormat) {
        this.roleNameFormat = roleNameFormat;
    }
    
    @Override
    public void configure(ConfigProperties configProperties) {
    	roleNameFormat=configProperties.getStringValue("roleNameFormat",roleNameFormat);
    }

    @Override
    public Collection<String> mapUserToRoles(AuthenticationInfo authenticationInfo) throws AuthenticationException {
        return Arrays.asList(String.format(roleNameFormat, authenticationInfo.getRealm()));
    }

}
